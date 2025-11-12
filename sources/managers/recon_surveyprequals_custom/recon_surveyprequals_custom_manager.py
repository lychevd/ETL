import sys
import argparse
from dataclasses import dataclass, field, fields
import json
import logging
import pandas as pd
import requests

from pyjsparser.parser import false

from core import  secret_manager, ms_sql_client_manager
from db import database_manager


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
class MsSqlParamsOut:
    work_flow_step_log_id: str
    mssql_conn_secret:str
    ms_sql_server: str
    ms_sql_database: str
    ms_sql_password: str
    ms_sql_user: str
    ms_sql_port: str
    ms_sql_schema: str
    query: str
    insert_query:str
    dry_run:str
    data_url:str
    ResponseSectionCode:str




    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")



def _minify_response_or_empty(text: str) -> str:
    """
    Return a minified JSON string if the payload has a non-empty Data field,
    otherwise return an empty string. Also strips line breaks by minifying.
    """
    if not text or not text.strip():
        return ""
    try:
        obj = json.loads(text)

        def data_field(o):
            if isinstance(o, dict):
                return o.get("Data")
            if isinstance(o, list) and o and isinstance(o[0], dict):
                return o[0].get("Data")
            return None

        if not data_field(obj):
            return ""
        return json.dumps(obj, separators=(",", ":"))
    except Exception:
        # Non-JSON or parsing error -> treat as empty
        return ""

def process_df_and_dispatch(
    df: pd.DataFrame,
    sql_client,                      # your SQLClient instance (with insert_single_row)
    insert_sql: str,                 # INSERT INTO [dbo].[Stg_RequestResponsesIn] ([FeedBatchID],[RequestToken],[ResponseSectionCode],[ResponseBody]) VALUES (?, ?, ?, ?)
    feed_batch_id: str,
    response_section_code: str,
    api_url: str,
    request_timeout: int = 15,
    dry_run: bool = False,
    session: requests.Session | None = None
):
    """
    For each RequestToken, if GMTs are present -> POST to API and INSERT the API response.
    If no GMT for that token -> INSERT a row with empty ResponseBody and NO API call.

    Corrections:
    - Non-2xx API responses increment errors and SKIP DB insert.
    - posted_payloads counts only successful POSTs.
    - Dry-run does no network/DB, only logs "would do".
    """
    if df is None or df.empty:
        return {"inserted_empty": 0, "posted_payloads": 0, "skipped_missing_token": 0, "errors": 0}

    # Resolve columns (case-insensitive)
    lower_map = {c.lower(): c for c in df.columns}
    gmt_col   = lower_map.get("identityvalue") or lower_map.get("gmt")
    token_col = lower_map.get("token") or lower_map.get("requesttoken") or lower_map.get("request_token")
    if not gmt_col or not token_col:
        raise ValueError("Expected columns not found. Need IdentityValue/gmt and token/RequestToken.")

    x = (
        df[[gmt_col, token_col]]
        .rename(columns={gmt_col: "gmt", token_col: "token"})
        .dropna(subset=["token"])
        .copy()
    )
    x["token"] = x["token"].astype(str).str.strip()
    x = x[x["token"].str.len() > 0]

    sess = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://offers.pch.com",
        "Referer": "https://offers.pch.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
    }

    summary = {"inserted_empty": 0, "posted_payloads": 0, "skipped_missing_token": 0, "errors": 0}

    for token, sub in x.groupby("token", sort=False):
        if not token:
            summary["skipped_missing_token"] += 1
            continue

        # Clean GMTs: drop NaNs, '', 'none', 'null', 'nan', lowercase & dedupe
        gmts_series = sub["gmt"].dropna()
        gmts = (
            gmts_series.astype(str)
            .map(lambda s: s.strip().lower())
            .replace({"none": "", "null": "", "nan": ""})
            .tolist()
        )
        gmts = [g for g in gmts if g]
        gmts = list(dict.fromkeys(gmts))

        try:
            if not gmts:
                # No IdentityValue -> only DB insert with empty body; no API call
                if dry_run:
                    logging.info(f"[DRY RUN] No GMT for token={token} -> Would INSERT ResponseBody='' (no API call).")
                    summary["inserted_empty"] += 1
                    continue

                try:
                    params = (feed_batch_id, token, response_section_code, "")
                    sql_client.insert_single_row(insert_sql, params, return_identity=False)
                except Exception as db_err:
                    logging.error(f"DB insert failed for token={token}: {db_err}")
                    summary["errors"] += 1
                    continue

                summary["inserted_empty"] += 1
                continue

            # Has GMTs -> POST, then insert API response (minified or "")
            payload = {"gmt": gmts, "token": token}
            logging.info(f"payload={payload}")
            logging.info(f"api_url={api_url}")
            logging.info(f"headers={headers}")

            if dry_run:
                logging.info(f"[DRY RUN] Has GMT for token={token} -> Would POST payload={payload}")
                logging.info(f"[DRY RUN] Then would INSERT API response (minified JSON or '' if no Data).")
                summary["posted_payloads"] += 1
                continue

            # Real call; treat any non-2xx as error and skip DB insert
            try:
                r = sess.post(api_url, headers=headers, json=payload, timeout=request_timeout)
            except Exception as http_err:
                logging.error(f"Request failed for token={token}: {http_err}")
                summary["errors"] += 1
                continue

            if not r.ok:
                logging.error(f"HTTP {r.status_code} for token={token}: {r.text[:400]}")
                summary["errors"] += 1
                continue

            resp_body_min = _minify_response_or_empty(r.text)
            logging.info(f"resp_body={resp_body_min }")

            try:
                params = (feed_batch_id, token, response_section_code, resp_body_min)
                sql_client.insert_single_row(insert_sql, params, return_identity=False)
            except Exception as db_err:
                logging.error(f"DB insert failed for token={token}: {db_err}")
                summary["errors"] += 1
                continue

            summary["posted_payloads"] += 1

        except Exception as e:
            logging.error(f"Failed processing token={token}: {e}")
            summary["errors"] += 1

    return summary

def str_to_bool_simple(s: str) -> bool:
    """
    Convert 'Y'/'N'/'true'/'false' (any case) to bool.
    Raises ValueError for anything else.
    """
    if not isinstance(s, str):
        raise TypeError("Expected a string")

    v = s.strip().lower()
    if v in ("y", "true"):
        return True
    if v in ("n", "false"):
        return False

    raise ValueError("Expected 'Y', 'N', 'true', or 'false'")

def run_surveyprequals_to_api(ms_sql_params_out:MsSqlParamsOut,
                           db_manager: database_manager.DatabaseManager):
    try:     # Associate program with pgp manager

        try:
            ms_sql_client = ms_sql_client_manager.SQLClient(
                ms_sql_params_out.ms_sql_server, ms_sql_params_out.ms_sql_database, ms_sql_params_out.ms_sql_user, ms_sql_params_out.ms_sql_password, ms_sql_params_out.ms_sql_port
            )
            logging.info("ms sql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise

        query_adjusted = ms_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", ms_sql_params_out.work_flow_step_log_id)
        data=ms_sql_client.execute_query_data_pandas(query_adjusted)
        logging.info(data)
        session = requests.Session()
        summary = process_df_and_dispatch(
            df=data,
            sql_client=ms_sql_client,
            insert_sql=ms_sql_params_out.insert_query,
            feed_batch_id=input_data.work_flow_log_id,  # <- your batch id
            response_section_code=ms_sql_params_out.ResponseSectionCode,  # <- your section code
            api_url=ms_sql_params_out.data_url,
            request_timeout=20,
            dry_run=str_to_bool_simple(ms_sql_params_out.dry_run),
            session=session
        )
        logging.info(summary)
        inserted_empty_s = str(summary.get("inserted_empty", 0))
        posted_payloads_s = str(summary.get("posted_payloads", 0))
        skipped_missing_token_s = str(summary.get("skipped_missing_token", 0))
        errors_s = str(summary.get("errors", 0))

        json_source = {
            'query': query_adjusted,
            'inserted_empty': inserted_empty_s,
            'posted_payloads': posted_payloads_s,
            'skipped_missing_token': skipped_missing_token_s,
            'errors': errors_s

        }

        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )


    except Exception as err:
        logging.error(f"Error in run_ms_sql_no_data_out: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
            "FAILED",
            str(err)
        )
        raise


def main():
    secrets = secret_manager.SecretManager()

    meta_connection = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)
    db_manager = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database
    )


    variables = db_manager.start_workflow_step_log(input_data.workflow_name, input_data.step_name,
                                                   input_data.work_flow_log_id, input_data.additional_param)
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = (
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

    )
    from_secret_list = json.loads(from_secret_list)




    ms_sql_params_out = MsSqlParamsOut(
        work_flow_step_log_id=secrets.get_variable_value( 'WorkFlow_Step_Log_id', variables, from_secret_list),
        mssql_conn_secret=secrets.get_variable_value('mssql_conn_secret', variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value( 'ms_sql_server', variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value( 'ms_sql_database', variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value( 'ms_sql_password', variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value( 'ms_sql_user', variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value('ms_sql_port', variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value( 'ms_sql_schema', variables, from_secret_list),
        query=secrets.get_variable_value( 'query', variables, from_secret_list),
        insert_query=secrets.get_variable_value('insert_query', variables, from_secret_list),
        dry_run =secrets.get_variable_value('dry_run', variables, from_secret_list),
        data_url=secrets.get_variable_value('data_url', variables, from_secret_list),
        ResponseSectionCode=secrets.get_variable_value('ResponseSectionCode', variables, from_secret_list)
    )

    logging.info(ms_sql_params_out )

    run_surveyprequals_to_api(ms_sql_params_out, db_manager)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                              ms_sql_params_out.work_flow_step_log_id, "success", "file_created_ok")

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
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param'),
            meta_db_secret_name=input_dict['meta_db_secret_name']
        )
        ##logging.info(f"Parsed input JSON: {input_data}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
