import requests
import pandas as pd
import io
import os
import time
import logging
from urllib.parse import quote
from datetime import datetime, timedelta
from dataclasses import dataclass, field, fields
import sys
import argparse
import json
import posixpath

from db import database_manager
from core import secret_manager, gsutil_manager, bq_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ------------------------ Data Classes ------------------------

@dataclass
class InputParams:
    meta_db_secret_name: str
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    additional_param: str = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class ApiParams:
    work_flow_step_log_id: str
    app_id: str
    report_type: str
    event_names: str
    token: str
    local_data_directory: str
    window_hours: int
    additional_fields: str
    max_retries: int
    maximum_rows: int

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class BQParams:
    service_account_path: str
    query: str
    bigquery_project: str
    get_key_file_sec_name: str
    gs_directory: str
    gs_bucket_name: str
    get_key_file_from_sec: str = field(default='Y', metadata={"optional": True})
    def_bq_cred: str = field(default='Y', metadata={"optional": True})
    def_bq_project: str = field(default='Y', metadata={"optional": True})

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


# ------------------------ Utility Function ------------------------

def download_appsflyer_csv_with_windows(
    token: str,
    app_id: str,
    report_type: str,
    event_names: str,
    start_date: str,
    end_date: str,
    local_data_directory: str,
    window_hours: int,
    additional_fields: str,
    max_retries: int,
    maximum_rows: int,
    gs_directory: str,
    gs_bucket_name: str,
    db_manager: database_manager.DatabaseManager,
    work_flow_step_log_id: str,
    input_param_obj: InputParams
):
    try:
        gsutil = gsutil_manager.GSUtilClient()
        os.makedirs(local_data_directory, exist_ok=True)

        base_url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{report_type}/v5"

        def to_epoch(dt_str):
            return int(time.mktime(time.strptime(dt_str, "%Y-%m-%d %H:%M")))

        start_ts = to_epoch(start_date)
        end_ts = to_epoch(end_date)
        window_secs = window_hours * 3600
        current_start = start_ts

        while current_start < end_ts:
            current_end = min(current_start + window_secs, end_ts)
            from_str = time.strftime("%Y-%m-%d %H:%M", time.gmtime(current_start))
            to_str = time.strftime("%Y-%m-%d %H:%M", time.gmtime(current_end))

            params = {
                "from": quote(from_str, safe=""),
                "to": quote(to_str, safe=""),
                "maximum_rows": str(maximum_rows),
                "event_name": quote(event_names, safe=""),
            }
            if additional_fields:
                params["additional_fields"] = quote(additional_fields, safe="")

            full_url = f"{base_url}?" + "&".join(f"{k}={v}" for k, v in params.items())
            output_file = os.path.join(local_data_directory, f"{app_id}_{report_type}_{from_str.replace(' ', '_').replace(':', '-')}_to_{to_str.replace(' ', '_').replace(':', '-')}.csv")

            row_count = 0
            for attempt in range(1, max_retries + 1):
                try:
                    response = requests.get(full_url, headers={"authorization": token}, timeout=120)
                    if response.status_code == 200:
                        content = response.text
                        if content.strip():
                            df = pd.read_csv(io.StringIO(content))
                            df.to_csv(output_file, index=False)
                            row_count = len(df)
                        break
                    elif response.status_code in (401, 403):
                        time.sleep(2 ** attempt)
                    else:
                        break
                except requests.RequestException:
                    time.sleep(2 ** attempt)

            if row_count > 0:
                if not gs_directory.endswith('/'):
                    gs_directory += '/'
                gsutil.push_file_to_gs_util(gs_bucket_name, gs_directory, output_file)

                filename_base = os.path.basename(output_file)
                db_manager.create_dataset_instance(input_param_obj.work_flow_log_id, work_flow_step_log_id, input_param_obj.step_name, "Source", {
                    'full_url': full_url,
                    'file_row_count': str(row_count),
                    'gs_bucket_name': gs_bucket_name
                })
                db_manager.create_dataset_instance(input_param_obj.work_flow_log_id, work_flow_step_log_id, input_param_obj.step_name, "Successor", {
                    'filename_full': posixpath.join(gs_directory, filename_base),
                    'filename_base': filename_base,
                    'gs_bucket_name': gs_bucket_name,
                    'file_row_count': str(row_count)
                })

            current_start = current_end

    except Exception as top_level_error:
        logging.error(f"Top-level error: {top_level_error}")
        db_manager.close_step_log(input_param_obj.workflow_name, input_param_obj.step_name, input_param_obj.work_flow_log_id, work_flow_step_log_id, "failed", str(top_level_error))
        raise


# ------------------------ Main Function ------------------------

def main(param: InputParams):
    secrets = secret_manager.SecretManager()
    meta_connection = secrets.get_meta_connection_from_secret(param.meta_db_secret_name)
    db_manager = database_manager.DatabaseManager(meta_connection.mysql_host, meta_connection.mysql_user, meta_connection.mysql_password, meta_connection.mysql_database)

    variables = db_manager.start_workflow_step_log(param.workflow_name, param.step_name, param.work_flow_log_id, param.additional_param)
    from_secret_list = json.loads(variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0] if 'from_secret_list' in variables['key'].values else '[]')

    api_param = ApiParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        app_id=secrets.get_variable_value('app_id', variables, from_secret_list),
        report_type=secrets.get_variable_value('report_type', variables, from_secret_list),
        event_names=secrets.get_variable_value('event_names', variables, from_secret_list),
        token=secrets.get_variable_value('token', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        window_hours=int(secrets.get_variable_value('window_hours', variables, from_secret_list)),
        additional_fields=secrets.get_variable_value('additional_fields', variables, from_secret_list),
        max_retries=int(secrets.get_variable_value('max_retries', variables, from_secret_list)),
        maximum_rows=int(secrets.get_variable_value('maximum_rows', variables, from_secret_list))
    )

    bq_params = BQParams(
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        query=secrets.get_variable_value('query', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        def_bq_cred=secrets.get_variable_value('def_bq_cred', variables, from_secret_list),
        def_bq_project=secrets.get_variable_value('def_bq_project', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list)
    )

    if bq_params.get_key_file_from_sec == "Y":
        secret_content = secrets.fetch_secret(bq_params.get_key_file_sec_name)
        secrets.write_secret_to_file(secret_content, bq_params.service_account_path)

    bq = bq_manager.BQManager(bq_params.service_account_path, bq_params.bigquery_project)
    value = bq.run_query_and_return_single_value(bq_params.query)

    dt_format = "%Y-%m-%d %H:%M"
    start_date = (datetime.strptime(value, dt_format) + timedelta(minutes=1)).strftime(dt_format)
    end_date = (datetime.now() - timedelta(minutes=3)).strftime(dt_format)

    download_appsflyer_csv_with_windows(
        token=api_param.token,
        app_id=api_param.app_id,
        report_type=api_param.report_type,
        event_names=api_param.event_names,
        start_date=start_date,
        end_date=end_date,
        local_data_directory=api_param.local_data_directory,
        window_hours=api_param.window_hours,
        additional_fields=api_param.additional_fields,
        max_retries=api_param.max_retries,
        maximum_rows=api_param.maximum_rows,
        gs_directory=bq_params.gs_directory,
        gs_bucket_name=bq_params.gs_bucket_name,
        db_manager=db_manager,
        work_flow_step_log_id=api_param.work_flow_step_log_id,
        input_param_obj=param
    )

    db_manager.close_step_log(param.workflow_name, param.step_name, param.work_flow_log_id, api_param.work_flow_step_log_id, "success", "files_from_api_created_ok")


# ------------------------ Entry Point ------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", required=True)
    parser.add_argument("--workflow_name", required=True)
    parser.add_argument("--step_name", required=True)
    args = parser.parse_args()

    try:
        input_dict = json.loads(args.result)
        parsed_input = InputParams(
            meta_db_secret_name=input_dict['meta_db_secret_name'],
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param')
        )
        main(parsed_input)
    except Exception as e:
        logging.error(f"Script failure: {e}")
        sys.exit(1)
