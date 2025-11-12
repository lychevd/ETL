import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields
import os

import pandas as pd  # NEW

from core import gcs_manager, secret_manager, gsutil_manager, local_util, ms_sql_bcp_manager  # bcp no longer used below
from core.ms_sql_client_manager_pymssql import SQLClient_PYmsql  # NEW
from db import database_manager
from google.cloud import bigquery
from google.cloud import storage

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
    ms_sql_server: str
    ms_sql_database: str
    ms_sql_password: str
    ms_sql_user: str
    ms_sql_port: str
    ms_sql_schema: str
    field_delimiter: str
    row_delimiter: str
    requires_boundary: str
    query: str
    end_boundary_query: str
    Start_Boundary: str
    filename_base: str
    local_data_directory: str
    mssql_conn_secret: str
    header: str
    header_line: str
    is_header_dynamic: str
    sql_dynamic_header: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class GcParamsIn:
    work_flow_step_log_id: str
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    get_key_file_sec_name: str
    get_key_file_from_sec: str
    google_project_name: str
    def_gs_cred: str
    def_gs_project: str
    push_file_to_gs: str
    clean_local_dir: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def _map_field_delimiter(token: str) -> str:
    if token is None:
        return ","
    t = token.upper()
    if t == "TAB":
        return "\t"
    if t == "PIPE":
        return "|"
    return token  # allow custom string or ","


def _map_row_delimiter(token: str) -> str:
    if token is None:
        return "\n"
    t = token.upper()
    if t == "NEW_LINE":
        return "\n"
    if t == "CR_NEW_LINE":
        return "\r\n"
    return token  # allow custom string


def _write_df_to_file(df: pd.DataFrame, output_file: str, field_delim: str, row_delim: str,
                      header_mode: str, static_header: str = None, dynamic_header: str = None) -> int:
    """
    header_mode: 'no_header' | 'static_header' | 'dynamic_header'
    static_header: header line as string (already delimited)
    dynamic_header: header line as string (already delimited)
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Open in text mode and write header (if any), then append DataFrame
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        if header_mode == "static_header" and static_header:
            f.write(static_header + row_delim)
        elif header_mode == "dynamic_header" and dynamic_header:
            f.write(dynamic_header + row_delim)

    # Then append the rows (no header, pandas handles quoting if needed)
    df.to_csv(
        output_file,
        sep=field_delim,
        index=False,
        header=False,
        mode="a",
        lineterminator=row_delim,
        encoding="utf-8"
    )

    return len(df)


def create_gs_file_from_ms_sql(gc_params_in: GcParamsIn, ms_sql_params_out: MsSqlParamsOut,
                               db_manager: database_manager.DatabaseManager):
    try:
        # gsutil + gcs managers
        service_account_path = None if gc_params_in.def_gs_cred == "Y" else gc_params_in.service_account_path
        google_project_name = None if gc_params_in.def_gs_project == "Y" else gc_params_in.google_project_name

        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise

        try:
            gcs = gcs_manager.GCSManager(service_account_path, google_project_name)
            logging.info("gcs OK!")
        except Exception as err:
            logging.error(f"Error initializing gcs manager: {err}")
            raise

        # local utils
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise

        # MS SQL client (pymssql)
        try:
            ms_sql_client = SQLClient_PYmsql(
                ms_sql_params_out.ms_sql_server,
                ms_sql_params_out.ms_sql_database,
                ms_sql_params_out.ms_sql_user,
                ms_sql_params_out.ms_sql_password,
                ms_sql_params_out.ms_sql_port
            )
            logging.info("ms sql (pymssql) connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise

        # Boundaries
        End_boundary = None
        if ms_sql_params_out.requires_boundary == "Y":
            try:
                end_rows = ms_sql_client.execute_query_autocommit(ms_sql_params_out.end_boundary_query, "Y")
                # Expect single row, single col
                if end_rows and len(end_rows) > 0:
                    End_boundary = end_rows[0][0]
                else:
                    logging.error("End boundary query returned no results.")
                    End_boundary = None
                logging.info(f"End boundary: {End_boundary}")
            except Exception as e:
                logging.error(f"Failed to fetch end boundary: {e}")
                End_boundary = None

        # Adjust main query
        query_adjusted = ms_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", gc_params_in.work_flow_step_log_id)

        if ms_sql_params_out.requires_boundary == "Y":
            # Use your placeholders; adjust if your query uses a single-bar variant.
            query_adjusted = query_adjusted.replace("|||Start_Boundary|||", ms_sql_params_out.Start_Boundary)
            query_adjusted = query_adjusted.replace("|||End_boundary|||", str(End_boundary) if End_boundary is not None else "")

        logging.info(f"Query after replacement={query_adjusted}")

        # Header handling
        header_mode = "no_header"
        header_line_to_write = None

        if ms_sql_params_out.header == "Y" and ms_sql_params_out.is_header_dynamic == "N":
            header_mode = "static_header"
            header_line_to_write = ms_sql_params_out.header_line

        elif ms_sql_params_out.header == "Y" and ms_sql_params_out.is_header_dynamic == "Y":
            header_mode = "dynamic_header"
            try:
                dyn_rows = ms_sql_client.execute_query_autocommit(ms_sql_params_out.sql_dynamic_header, "Y")
                # Prefer single value if provided (SELECT 'a,b,c')
                if dyn_rows and len(dyn_rows) > 0:
                    # If 1 col, take the value; else join first row values by field delimiter
                    vals = list(dyn_rows[0])
                    if len(vals) == 1:
                        header_line_to_write = str(vals[0]) if vals[0] is not None else ""
                    else:
                        field_delim = _map_field_delimiter(ms_sql_params_out.field_delimiter)
                        header_line_to_write = field_delim.join("" if v is None else str(v) for v in vals)
                else:
                    header_line_to_write = None
            except Exception as e:
                logging.error(f"Failed to get dynamic header: {e}")
                header_line_to_write = None

        # Fetch data into DataFrame (pymssql)
        df = ms_sql_client.execute_query_data_pandas(query_adjusted)
        row_count = len(df)
        logging.info(f"DataFrame rows: {row_count}, cols: {df.shape[1] if not df.empty else 0}")

        # Build output path
        output_file = os.path.join(ms_sql_params_out.local_data_directory, ms_sql_params_out.filename_base)
        field_delim = _map_field_delimiter(ms_sql_params_out.field_delimiter)
        row_delim = _map_row_delimiter(ms_sql_params_out.row_delimiter)

        # If no rows, finalize with Source and success (no file content or optional header only)
        json_source = {
            "query": query_adjusted,
            "file_row_count": str(row_count)
        }
        if ms_sql_params_out.requires_boundary == "Y":
            json_source.update({
                "Start_Boundary": ms_sql_params_out.Start_Boundary,
                "End_Boundary": End_boundary
            })

        logging.info(f"json_source JSON= {json.dumps(json_source)}")

        # Write file (header, then rows)
        if row_count == 0:
            # Optionally still write only header if required (usually not needed)
            if header_mode in ("static_header", "dynamic_header") and header_line_to_write:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, "w", encoding="utf-8", newline="") as f:
                    f.write(header_line_to_write + row_delim)

            # Log and close step (no data)
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_params_in.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                json_source
            )
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                gc_params_in.work_flow_step_log_id,
                "success",
                "no_records_to_output"
            )
            sys.exit(0)
        else:
            mode = {
                "no_header": "no_header",
                "static_header": "static_header",
                "dynamic_header": "dynamic_header"
            }[header_mode]

            _ = _write_df_to_file(
                df=df,
                output_file=output_file,
                field_delim=field_delim,
                row_delim=row_delim,
                header_mode=mode,
                static_header=header_line_to_write if mode == "static_header" else None,
                dynamic_header=header_line_to_write if mode == "dynamic_header" else None
            )

        # Push to GCS and collect metadata
        if gc_params_in.push_file_to_gs == "Y":
            gsutil.push_file_to_gs_util(gc_params_in.gs_bucket_name, gc_params_in.gs_directory, output_file)
            json_data_successor = gcs.get_gs_file_pro(
                gc_params_in.gs_bucket_name,
                gc_params_in.gs_directory,
                ms_sql_params_out.filename_base,
                row_count
            )
        else:
            json_data_successor = local.collect_local_file_metadata(output_file, row_count)

        # Clean temp as requested
        if gc_params_in.clean_local_dir == "Y" and gc_params_in.push_file_to_gs == "Y":
            local.clean_local_directory(ms_sql_params_out.local_data_directory)
        if gc_params_in.clean_local_dir == "N" and gc_params_in.push_file_to_gs == "Y":
            local.clean_local_file(output_file)
            logging.info(f"local_temp_file={output_file}. Has been deleted")

        # Log dataset instances
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_params_in.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )

        logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_params_in.work_flow_step_log_id,
            input_data.step_name,
            "Successor",
            json_data_successor
        )

        if gc_params_in.clean_local_dir == "Y" and gc_params_in.push_file_to_gs != "Y":
            local.clean_local_directory(ms_sql_params_out.local_data_directory)

    except Exception as err:
        logging.error(f"Error in create_gs_file_from_ms_sql: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            gc_params_in.work_flow_step_log_id,
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

    variables = db_manager.start_workflow_step_log(
        input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id, input_data.additional_param
    )

    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = json.loads(variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0])

    gc_params_in = GcParamsIn(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
        push_file_to_gs=secrets.get_variable_value('push_file_to_gs', variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list)
    )

    # NOTE: fixed a couple of likely key mix-ups for delimiters/boundary flags
    ms_sql_params_out = MsSqlParamsOut(
        mssql_conn_secret=secrets.get_variable_value('mssql_conn_secret', variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value('ms_sql_server', variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value('ms_sql_database', variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value('ms_sql_password', variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value('ms_sql_user', variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value('ms_sql_port', variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value('ms_sql_schema', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        row_delimiter=secrets.get_variable_value('row_delimiter', variables, from_secret_list),        # FIX
        requires_boundary=secrets.get_variable_value('requires_boundary', variables, from_secret_list),# FIX
        query=secrets.get_variable_value('query', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value('end_boundary_query', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value('Start_Boundary', variables, from_secret_list),
        filename_base=secrets.get_variable_value('filename_base', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        header=secrets.get_variable_value('header', variables, from_secret_list),
        header_line=secrets.get_variable_value('header_line', variables, from_secret_list),
        is_header_dynamic=secrets.get_variable_value('is_header_dynamic', variables, from_secret_list),
        sql_dynamic_header=secrets.get_variable_value('sql_dynamic_header', variables, from_secret_list)
    )

    if gc_params_in.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting key from secret")
            secret_content = secrets.fetch_secret(gc_params_in.get_key_file_sec_name)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gc_params_in.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(
                input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                gc_params_in.work_flow_step_log_id, "failed", f"{e}"
            )
            sys.exit(1)

    create_gs_file_from_ms_sql(gc_params_in, ms_sql_params_out, db_manager)
    db_manager.close_step_log(
        input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
        gc_params_in.work_flow_step_log_id, "success", "file_created_ok"
    )


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
        input_dict = json.loads(args.result)
        input_data = InputParams(
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param'),
            meta_db_secret_name=input_dict['meta_db_secret_name']
        )
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
