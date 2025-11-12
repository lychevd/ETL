import sys
import argparse
import posixpath
import logging
import json
import csv
from typing import Tuple
from dataclasses import dataclass, field, fields
from core import gcs_manager, secret_manager, gsutil_manager, local_util, ms_sql_bcp_manager, \
    ms_sql_client_manager
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
class GcsFileParamsOut:
    work_flow_step_log_id: str
    local_data_directory: str
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    source_list_type: str
    post_process_type: str
    has_post_process: str
    delete_source_record: str
    google_project_name: str
    gs_directory_done: str
    gs_bucket_name_done: str
    get_key_file_from_sec: str
    get_key_file_sec_name: str
    def_gs_cred: str
    def_gs_project: str
    service_account_path: str
    clean_local_dir: str


    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class MsSqlParamsIN:
    mssql_conn_secret: str
    ms_sql_server: str
    ms_sql_database: str
    action_before_insert: str
    sql_action_insert:str
    ms_sql_password: str
    ms_sql_user: str
    data_file_delimiter: str
    ms_sql_port: str
    ms_sql_schema: str
    row_delimiter: str
    skip_header_rows: str
    insert_query: str
    static_fields: str
    expected_file_cols:str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
def is_convertible_to_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def parse_static_fields(csv_string: str, *, max_items: int = 4) -> Tuple[str, ...]:
    """
    Convert a comma-separated string into a tuple of strings (max 4 items).
    - Trims surrounding whitespace on each field.
    - Respects quotes, so 'a,"b,c",d' -> ('a', 'b,c', 'd').
    - Raises ValueError if empty input or more than max_items are provided.

    Examples:
        parse_static_fields("123456, 98765") -> ("123456", "98765")
        parse_static_fields('"foo", "bar,baz"') -> ("foo", "bar,baz")
    """
    if csv_string is None:
        raise ValueError("csv_string must be a non-empty string.")
    csv_string = str(csv_string).strip()
    if not csv_string:
        raise ValueError("csv_string must not be empty.")

    # Use csv module to handle quotes/embedded commas robustly
    row = next(csv.reader([csv_string], skipinitialspace=True), [])
    fields = [f.strip() for f in row]

    if len(fields) == 0:
        raise ValueError("No fields found in csv_string.")
    if len(fields) > max_items:
        raise ValueError(f"Too many fields: {len(fields)} > max_items={max_items}.")

    return tuple(fields)
def load_gs_file_to_ms_sql(file_list, Gcs_File_Params: GcsFileParamsOut, Ms_Sql_Params_IN: MsSqlParamsIN,
                           db_manager: database_manager.DatabaseManager, input_data : InputParams):
    try:
        if  Ms_Sql_Params_IN.static_fields=="None":
            static_fields=None
        else:
            static_fields=Ms_Sql_Params_IN.static_fields


        if Ms_Sql_Params_IN.expected_file_cols == "None" or is_convertible_to_int(Ms_Sql_Params_IN.expected_file_cols)==False:
            expected_file_cols = None
        else:
            expected_file_cols = int(Ms_Sql_Params_IN.expected_file_cols)
        filename_full = None
        logging.info(f"Files NOT PROCESSED FILES from inside of function= {json.dumps(file_list)}")
        if Gcs_File_Params.def_gs_cred == "Y":
            service_account_path = None
        else:
            service_account_path = Gcs_File_Params.service_account_path
        if Gcs_File_Params.def_gs_project == "Y":
            google_project_name = None
        else:
            google_project_name = Gcs_File_Params.google_project_name
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise


        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise
        try:
            ms_sql_client = ms_sql_client_manager.SQLClient(
                Ms_Sql_Params_IN.ms_sql_server, Ms_Sql_Params_IN.ms_sql_database, Ms_Sql_Params_IN.ms_sql_user, Ms_Sql_Params_IN.ms_sql_password, Ms_Sql_Params_IN.ms_sql_port
            )
            logging.info("ms sql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise

        for item in file_list:
            logging.info(f"Start Processing Item= {json.dumps(item)}")
            try:
                # Process each file

                filename_full = item['filename_full']
                filename_base = item['filename_base']
                gs_bucket_name = item['gs_bucket_name']
                local_temp_file = posixpath.join(Gcs_File_Params.local_data_directory, filename_base)
                gsutil.get_file_from_gs_util(gs_bucket_name, filename_full, local_temp_file, filename_base)

                if static_fields is not None and str(static_fields).strip().lower() not in ("none", "null", ""):
                    sf = str(static_fields)  # work on a copy
                    sf = sf.replace("|||workflow_log_id|||", str(input_data.work_flow_log_id))
                    sf = sf.replace("|||workflow_step_log_id|||",
                                    str(Gcs_File_Params.work_flow_step_log_id))  # ok if absent
                    sf = sf.replace("|||file_name|||", filename_base)
                    logging.info(f"sf={sf}")
                    static_fields_tuple = parse_static_fields(sf)  # <-- tuple('file','wf_id')
                else:
                    static_fields_tuple = None
                logging.info(f"static_fields_tuple={static_fields_tuple}")

                if Ms_Sql_Params_IN.action_before_insert=="Y":

                    ms_sql_client.execute_query_autocommit( Ms_Sql_Params_IN.sql_action_insert,"N")
                skip_header_rows=int(Ms_Sql_Params_IN.skip_header_rows)
                logging.info(f"Ms_Sql_Params_IN.ms_sql_user={Ms_Sql_Params_IN.ms_sql_user}")
                number_of_records= ms_sql_client.insert_from_file_with_fields(
                    data_file_path= local_temp_file,
                    insert_query=Ms_Sql_Params_IN.insert_query,
                    delimiter=Ms_Sql_Params_IN.data_file_delimiter,
                    row_delimiter=Ms_Sql_Params_IN.row_delimiter,
                    skip_header_rows=skip_header_rows,
                    static_fields=static_fields_tuple,
                    expected_file_cols=expected_file_cols
                )

                # Execute stored procedures
                db_manager.create_dataset_instance(input_data.work_flow_log_id, Gcs_File_Params.work_flow_step_log_id,

                                                   input_data.step_name, "Destination", item)

                # Extract the directory path

                json_data_successor = {
                    'number_of_records': str(number_of_records),
                    'filename_full': filename_full

                }
                logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
                db_manager.create_dataset_instance(input_data.work_flow_log_id, Gcs_File_Params.work_flow_step_log_id,
                                                   input_data.step_name, "Successor", json_data_successor)

                # Handle post-processing
                if Gcs_File_Params.has_post_process == "Y":
                    gsutil.gs_post_process(
                        post_process_type=Gcs_File_Params.post_process_type,
                        gs_bucket_name=Gcs_File_Params.gs_bucket_name,
                        filename_full=filename_full,
                        gs_bucket_name_done=Gcs_File_Params.gs_bucket_name_done,
                        gs_done_directory=Gcs_File_Params.gs_directory_done
                    )
                    logging.info(f"Post-processing completed for file {filename_full}.")
                if Gcs_File_Params.clean_local_dir == "Y":
                    local.clean_local_directory(Gcs_File_Params.local_data_directory)


            except Exception as e:
                logging.error(f"Error processing file {filename_full}: {e}")
                raise  # Stop processing on any error

    except Exception as top_level_error:
        logging.error(f"Top-level error in process_gs_to_sftp_files: {top_level_error}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "failed",
                                  f"{top_level_error}")
        raise


def main():
    files = []
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
    ##logging.info(f"variables ={variables}")
    Gcs_File_Params = GcsFileParamsOut(
        gs_bucket_name=secrets.get_variable_value( 'gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value( 'service_account_path', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        post_process_type=secrets.get_variable_value( 'post_process_type', variables, from_secret_list),
        has_post_process=secrets.get_variable_value( 'has_post_process', variables, from_secret_list),
        delete_source_record=secrets.get_variable_value( 'delete_source_record', variables, from_secret_list),
        source_list_type=secrets.get_variable_value('source_list_type', variables, from_secret_list),
        gs_directory_done=secrets.get_variable_value( 'gs_directory_done', variables, from_secret_list),
        gs_bucket_name_done=secrets.get_variable_value( 'gs_bucket_name_done', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value( 'get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value( 'get_key_file_sec_name', variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value( 'clean_local_dir', variables, from_secret_list)


    )
    action_before_insert: str
    sql_action_insert: str
    ##logging.info(f"Gcs_File_Params={Gcs_File_Params}")
    Ms_Sql_Params_IN = MsSqlParamsIN(
        mssql_conn_secret=secrets.get_variable_value('mssql_conn_secret',variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value('ms_sql_server', variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value('ms_sql_database', variables, from_secret_list),
        action_before_insert=secrets.get_variable_value('action_before_insert', variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value( 'ms_sql_password',variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value( 'ms_sql_port', variables, from_secret_list),
        data_file_delimiter=secrets.get_variable_value('data_file_delimiter', variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value('ms_sql_user', variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value( 'ms_sql_schema', variables, from_secret_list),
        sql_action_insert=secrets.get_variable_value( 'sql_action_insert', variables, from_secret_list),
        row_delimiter=secrets.get_variable_value('row_delimiter', variables, from_secret_list),
        skip_header_rows=secrets.get_variable_value('skip_header_rows', variables, from_secret_list),
        insert_query = secrets.get_variable_value('insert_query', variables, from_secret_list),
        static_fields=secrets.get_variable_value('static_fields', variables, from_secret_list),
        expected_file_cols = secrets.get_variable_value('expected_file_cols', variables, from_secret_list)
    )

    logging.info(f"Ms_Sql_Params_IN ={Ms_Sql_Params_IN }")
    if Gcs_File_Params.def_gs_cred == "Y":
        service_account_path = None
    else:
        service_account_path = Gcs_File_Params.service_account_path
    if Gcs_File_Params.def_gs_project == "Y":
        google_project_name = None
    else:
        google_project_name = Gcs_File_Params.google_project_name
    try:
        gcs = gcs_manager.GCSManager(service_account_path, google_project_name)
        logging.info("gcs OK!")
    except Exception as err:
        logging.error(f"Error initializing gcs manager: {err}")
        raise

    if Gcs_File_Params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                Gcs_File_Params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, Gcs_File_Params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if Gcs_File_Params.source_list_type == "LIST":
        try:
            files_properties = gcs.list_files_with_properties(Gcs_File_Params.gs_bucket_name,
                                                              Gcs_File_Params.gs_directory)
            logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
            if not files_properties:
                logging.info("No gs files to process. Exiting.")
                db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                          Gcs_File_Params.work_flow_step_log_id, "success",
                                          f"no_gs_files_detected")
                sys.exit(0)
            files = db_manager.select_items_to_process_by_list(files_properties, input_data.step_name,
                                                               input_data.work_flow_log_id,
                                                               Gcs_File_Params.work_flow_step_log_id)
        except Exception as e:
            logging.error(f"Failed to list files or process items: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    elif Gcs_File_Params.source_list_type == "RECORD":
        try:
            files = db_manager.select_items_to_process_by_records(input_data.work_flow_log_id,
                                                                  Gcs_File_Params.work_flow_step_log_id,
                                                                  input_data.step_name)
        except Exception as e:
            logging.error(f"Failed to select items by records: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    # Process files or exit if none
    if not files:
        logging.info("No NEW files on gs to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "succes"
                                                                         "s",
                                  f"no_new_gs_files_detected")
        sys.exit(0)
    try:
        load_gs_file_to_ms_sql(files, Gcs_File_Params, Ms_Sql_Params_IN, db_manager,input_data)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "success",
                                  f"all_gs_files_loaded_to_mssql_table")
    except Exception as e:
        logging.error(f"Failed to process GS to mssql files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "failed",
                                  f"{e}")
        sys.exit(1)


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

