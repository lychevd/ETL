import sys
import argparse
import logging
import json
import pandas as pd
from dataclasses import dataclass, field, fields
from typing import Union, List, Dict


from pyjsparser.parser import false

from core import gcs_manager, secret_manager,bq_manager,email_manager
from db import database_manager
from google.cloud import bigquery
from google.cloud import storage
import uuid
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sftp_file_mover.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class InputParams:
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    meta_db_secret_name: str
    additional_param: str = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class GcBQParams:
    work_flow_step_log_id: str
    service_account_path: str
    query: str
    end_boundary_query: str
    bigquery_project: str
    Start_Boundary: str
    get_key_file_sec_name: str
    LABEL:str
    get_key_file_from_sec: str = field(default='Y', metadata={"optional": True})
    requires_boundary: str = field(default='Y', metadata={"optional": True})
    def_bq_cred: str = field(default='Y', metadata={"optional": True})
    def_bq_project: str = field(default='Y', metadata={"optional": True})



    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

@dataclass
class EmailParams:
    smtp_conn_secret: str
    smtp_server: str
    smtp_port: str
    sender_email: str
    sender_password: str
    use_tls: str
    REPORT_EMAIL_TO: str
    is_html:str
@dataclass
class ReportParams:
    REPORT_HEADER: str
    REPORT_EMAIL_SUBJECT: str
    desired_fields_str: str
def str_to_bool(value: str) -> bool:
    val = value.strip().lower()
    if val == "true":
        return True
    elif val == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean string: '{value}'")




from typing import Union, List, Dict
import pandas as pd

def exceptions_json_to_html_table(
    data: Union[Dict[str, List[Dict]], List[Dict]],
    desired_fields_str: str
) -> str:
    """
    Converts a list or dict containing 'Exceptions' into an HTML table
    using only the desired fields.

    Args:
        data: Either a dict with key 'Exceptions' or the list itself.
        desired_fields_str: Comma-separated field names to include in table.

    Returns:
        str: HTML table as a string.
    """
    # Convert comma-separated string to list of field names
    desired_fields = [field.strip() for field in desired_fields_str.split(",")]

    if isinstance(data, dict):
        exceptions = data.get("Exceptions", [])
    elif isinstance(data, list):
        exceptions = data
    else:
        return "<p><strong>Invalid input format.</strong></p>"

    if not isinstance(exceptions, list) or not exceptions:
        return "<p><strong>No exceptions found.</strong></p>"

    df = pd.DataFrame(exceptions)

    # Keep only desired fields that exist in the DataFrame
    valid_fields = [field for field in desired_fields if field in df.columns]
    df = df[valid_fields]
    logging.info(df)

    return df.to_html(index=False, border=1, justify="center")


def run_bq_table_alert(
    gc_bq_params: GcBQParams,
    email_params: EmailParams,
    db_manager: database_manager.DatabaseManager,
    report_params: ReportParams
):
    try:
        service_account_path = None if gc_bq_params.def_bq_cred == "Y" else gc_bq_params.service_account_path
        bq_project_name = None if gc_bq_params.def_bq_project == "Y" else gc_bq_params.bigquery_project

        try:
            bq = bq_manager.BQManager(service_account_path, bq_project_name)
        except Exception as err:
            logging.error(f"Error opening bq_manager: {err}")
            raise

        logging.info(f"Client BQ set successful! Using {gc_bq_params.service_account_path}")

        sender = email_manager.EmailSender(
            smtp_server=email_params.smtp_server,
            smtp_port=int(email_params.smtp_port),
            login_email=email_params.sender_email,
            login_password=email_params.sender_password,
            use_tls=str_to_bool(email_params.use_tls)
        )

        # Prepare query and boundaries
        start_boundary = gc_bq_params.Start_Boundary if gc_bq_params.requires_boundary == "Y" else None
        end_boundary_query = gc_bq_params.end_boundary_query if gc_bq_params.requires_boundary == "Y" else None

        query = gc_bq_params.query.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query = query.replace("|||WorkFlow_Step_Log_id|||", gc_bq_params.work_flow_step_log_id)

        # Run BQ stored procedure / query
        result_json = bq.run_query_and_return_json(gc_bq_params.requires_boundary, query, end_boundary_query, start_boundary)
        rows = result_json.get("rows", [])

        metadata_keys = ["query", "Start_Boundary", "End_Boundary"]
        metadata = {k: result_json[k] for k in metadata_keys if k in result_json}

        if not rows:
            logging.info("No rows returned from BigQuery. Skipping email.")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                metadata
            )
            return

        # Parse json_data field from first row
        parsed_row = rows[0]
        json_data_str = parsed_row.get("json_data")

        if not json_data_str or json_data_str.strip().lower() == "null":
            logging.info("json_data is null, 'null' string, or empty. Skipping email.")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                metadata
            )
            return
        if isinstance(parsed_row, dict) and "json_data" in parsed_row:
            try:
                exception_json = json.loads(parsed_row["json_data"])
                html_table = exceptions_json_to_html_table(exception_json, report_params.desired_fields_str)
            except Exception as e:
                logging.error(f"Failed to parse 'json_data' as JSON: {e}")
                html_table = "<p>Error parsing exception data</p>"
        else:
            logging.warning("Expected 'json_data' key not found in result row.")
            html_table = "<p>No valid json_data found in result</p>"

        html_header = f"<h3>{report_params.REPORT_HEADER}</h3>"
        report_email_body = html_header + html_table

        logging.info(f"Sending HTML email with body:\n{report_email_body}")
        sender.send_email(
            recipient_email=email_params.REPORT_EMAIL_TO,
            subject=report_params.REPORT_EMAIL_SUBJECT,
            body=report_email_body,
            is_html="Y"
        )

        # Log metadata
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            metadata
        )

    except Exception as err:
        logging.error(f"Error in run_bq_table_alert: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            "FAILED",
            str(err)
        )
        raise


def main():
    secrets = secret_manager.SecretManager()
    meta_connection = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)

    #gcs = gcs_manager.GCSManager()
    db_manager = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database
    )
    variables = db_manager.start_workflow_step_log(input_data.workflow_name, input_data.step_name,
                                                   input_data.work_flow_log_id, input_data.additional_param)
    #logging.info(variables)
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = (
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

    )
    from_secret_list = json.loads(from_secret_list)

    gc_bq_params = GcBQParams(
        work_flow_step_log_id=secrets.get_variable_value(  'WorkFlow_Step_Log_id', variables, from_secret_list),
        service_account_path=secrets.get_variable_value(  'service_account_path', variables, from_secret_list),
        query=secrets.get_variable_value( 'query', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value(  'end_boundary_query', variables, from_secret_list),
        requires_boundary=secrets.get_variable_value(  'requires_boundary', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value(  'get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value(  'get_key_file_sec_name', variables, from_secret_list),
        def_bq_cred =secrets.get_variable_value(  'def_bq_cred',variables, from_secret_list),
        def_bq_project=secrets.get_variable_value(  'def_bq_project', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value(   'bigquery_project', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value(   'Start_Boundary', variables, from_secret_list),
        LABEL=secrets.get_variable_value('LABEL', variables, from_secret_list)
    )
    #logging.info(f"gc_bq_params: {gc_bq_params}")
    query = gc_bq_params.query
    query=query.replace('|||LABEL|||',gc_bq_params.LABEL)
    query = query.replace('|||work_flow_step_log_id|||', gc_bq_params.work_flow_step_log_id)
    query = query.replace('|||work_flow_log_id|||', input_data.work_flow_log_id)
    query = query.replace('|||TalendJobBundleRunId|||',input_data.work_flow_log_id)
    query = query.replace('|||TalendLogID|||', gc_bq_params.work_flow_step_log_id)
    gc_bq_params.query= query

    end_boundary_query = gc_bq_params.end_boundary_query
    end_boundary_query = end_boundary_query.replace('|||LABEL|||', gc_bq_params.LABEL)
    end_boundary_query = end_boundary_query.replace('|||work_flow_step_log_id|||', gc_bq_params.work_flow_step_log_id)
    end_boundary_query = end_boundary_query.replace('|||work_flow_log_id|||', input_data.work_flow_log_id)
    end_boundary_query= end_boundary_query.replace('|||TalendJobBundleRunId|||', input_data.work_flow_log_id)
    end_boundary_query = end_boundary_query.replace('|||TalendLogID|||', gc_bq_params.work_flow_step_log_id)
    gc_bq_params.end_boundary_query = end_boundary_query


    email_params = EmailParams(
        smtp_conn_secret=secrets.get_variable_value('smtp_conn_secret', variables, from_secret_list),
        smtp_server=secrets.get_variable_value('smtp_server', variables, from_secret_list),
        smtp_port=secrets.get_variable_value('smtp_port', variables, from_secret_list),
        sender_email=secrets.get_variable_value('sender_email', variables, from_secret_list),
        sender_password=secrets.get_variable_value('sender_password', variables, from_secret_list),
        use_tls=secrets.get_variable_value('use_tls', variables, from_secret_list),
        REPORT_EMAIL_TO=secrets.get_variable_value('REPORT_EMAIL_TO', variables, from_secret_list),
        is_html=secrets.get_variable_value('is_html', variables, from_secret_list)

    )
    #logging.info(f"email_params: {email_params}")

    report_params = ReportParams(
        REPORT_HEADER=secrets.get_variable_value('REPORT_HEADER', variables, from_secret_list),
        REPORT_EMAIL_SUBJECT=secrets.get_variable_value('REPORT_EMAIL_SUBJECT', variables, from_secret_list),
        desired_fields_str=secrets.get_variable_value('desired_fields_str', variables, from_secret_list)

           )

    #logging.info(f"report_params: {report_params}")





    logging.info(f"query: {gc_bq_params.query}")

    if gc_bq_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                gc_bq_params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gc_bq_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    run_bq_table_alert(gc_bq_params,email_params, db_manager,report_params)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                              gc_bq_params.work_flow_step_log_id, "success", "statement_run_ok")



# --- Main execution ---
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process input JSON and fetch workflow data.")
    parser.add_argument("--result", required=True, help="Input JSON in dictionary format")
    parser.add_argument("--workflow_name", required=True, help="Name of workflow executing")
    parser.add_argument("--step_name", required=True, help="Name of step to execute")

    # Parse command-line arguments
    args = parser.parse_args()

    # Convert the JSON string argument to a Python dictionary
    try:
        input_dict = json.loads(
            args.result)  # this is going to parsed JSON pushed from parent from opening workflow log
        input_data = InputParams(
            meta_db_secret_name=input_dict['meta_db_secret_name'],
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param')
        )
        ##logging.info(f"Parsed input JSON: {input_data}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
