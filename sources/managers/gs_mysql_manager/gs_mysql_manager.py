import sys
import argparse
import logging
import json
import posixpath
from dataclasses import dataclass, field, fields
from core import gcs_manager, secret_manager, gsutil_manager,mysql_bulk_manager,local_util
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
class GcMySqlParams:
    work_flow_step_log_id: str
    source_list_type: str
    get_key_file_sec_name: str
    gs_bucket_name: str
    gs_directory: str
    field_delimiter: str
    row_delimiter: str
    skip_leading_rows:str
    post_process_type: str
    has_post_process: str
    gs_directory_done: str
    gs_bucket_name_done: str
    service_account_path: str
    get_key_file_from_sec: str
    def_gs_cred: str
    def_gs_project: str
    google_project_name: str
    local_data_directory: str
    mysql_host: str
    mysql_user: str
    mysql_database: str
    mysql_password: str
    mysql_port: str
    mysql_conn_secret: str
    mysql_table_in: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")





def load_gs_file_to_mysql_table(file_list,  gc_mysql_params : GcMySqlParams, db_manager: database_manager.DatabaseManager):
    try:
        if gc_mysql_params.def_gs_cred == "Y":
            service_account_path = None
        else:
            service_account_path = gc_mysql_params.service_account_path
        if gc_mysql_params.def_gs_project == "Y":
            google_project_name = None
        else:
            google_project_name = gc_mysql_params.google_project_name
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise
        try:
            gsutil = gsutil_manager.GSUtilClient()

        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise



        for item in file_list:
            logging.info(f"Start Processing Item = {json.dumps(item)}")
            try:
                filename_full = item['filename_full']
                filename_base = item['filename_base']
                gs_bucket_name = item['gs_bucket_name']
                local_temp_file = posixpath.join(gc_mysql_params.local_data_directory, filename_base)
                gsutil.get_file_from_gs_util(gs_bucket_name, filename_full, local_temp_file, filename_base)
                mysql_port_int=int(gc_mysql_params.mysql_port)
                skip_leading_rows_int=int(gc_mysql_params.skip_leading_rows)
                mysql_bulk_manager.MySqlImportExportManager.import_data_file(gc_mysql_params.mysql_host,gc_mysql_params.mysql_database,gc_mysql_params.mysql_table_in,gc_mysql_params.mysql_user,gc_mysql_params.mysql_password,local_temp_file,gc_mysql_params.field_delimiter,mysql_port_int,skip_leading_rows_int,gc_mysql_params.row_delimiter)



                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    gc_mysql_params.work_flow_step_log_id,
                    input_data.step_name,
                    "Destination",
                    item
                )

                json_data_successor = {
                    'filename_full': filename_full,
                    'filename_base': filename_base,
                    'gs_bucket_name': gs_bucket_name
                }

                logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    gc_mysql_params.work_flow_step_log_id,
                    input_data.step_name,
                    "Successor",
                    json_data_successor
                )

            except Exception as err:
                logging.error(f"Error processing item {item}: {err}")
                raise
            if gc_mysql_params.has_post_process == "Y":
                gsutil.gs_post_process(
                    post_process_type=gc_mysql_params.post_process_type,
                    gs_bucket_name=gc_mysql_params.gs_bucket_name,
                    filename_full=filename_full,
                    gs_bucket_name_done=gc_mysql_params.gs_bucket_name_done,
                    gs_done_directory=gc_mysql_params.gs_directory_done
                )
                logging.info(f"Post-processing completed for file {filename_full}.")

            local.clean_local_file(local_temp_file)
            logging.info(f"local_temp_file= {local_temp_file}. Has been deleted")



    except Exception as err:
        logging.error(f"Error in load_gs_file_to_bq: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            gc_mysql_params.work_flow_step_log_id,
            "FAILED",
            str(err)
        )
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
    gcs = gcs_manager.GCSManager()

    variables = db_manager.start_workflow_step_log(input_data.workflow_name, input_data.step_name,
                                                   input_data.work_flow_log_id, input_data.additional_param)
    ##logging.info(variables)
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = json.loads(
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
    )

    gc_mysql_params = GcMySqlParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        mysql_conn_secret=secrets.get_variable_value('mysql_conn_secret', variables, from_secret_list),
        mysql_host=secrets.get_variable_value('mysql_host', variables, from_secret_list),
        mysql_user=secrets.get_variable_value('mysql_user', variables, from_secret_list),
        mysql_database=secrets.get_variable_value('mysql_database', variables, from_secret_list),
        mysql_password=secrets.get_variable_value('mysql_password', variables, from_secret_list),
        mysql_port=secrets.get_variable_value('mysql_port', variables, from_secret_list),
        source_list_type=secrets.get_variable_value('source_list_type', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        post_process_type=secrets.get_variable_value('post_process_type', variables, from_secret_list),
        has_post_process=secrets.get_variable_value('has_post_process', variables, from_secret_list),
        gs_directory_done=secrets.get_variable_value('gs_directory_done', variables, from_secret_list),
        gs_bucket_name_done=secrets.get_variable_value('gs_bucket_name_done', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
       local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
       mysql_table_in=secrets.get_variable_value('mysql_table_in', variables, from_secret_list),
       row_delimiter = secrets.get_variable_value('row_delimiter', variables, from_secret_list),
      skip_leading_rows=secrets.get_variable_value('skip_leading_rows', variables, from_secret_list),

        )

    ##logging.info(f"gc_mysql_params: {gc_mysql_params}")

    if gc_mysql_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting key from secret")
            secret_content = secrets.fetch_secret(gc_mysql_params.get_key_file_sec_name)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gc_mysql_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                     gc_mysql_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if gc_mysql_params.source_list_type == "LIST":
        try:
            files_properties = gcs.list_files_with_properties(gc_mysql_params.gs_bucket_name, gc_mysql_params.gs_directory)
            logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
            if not files_properties:
                logging.info("No gs files to process. Exiting.")
                db_manager.close_step_log(input_data.workflow_name, input_data.step_name,
                                          input_data.work_flow_log_id,
                                          gc_mysql_params.work_flow_step_log_id, "success",
                                          f"no_gs_files_detected")
                sys.exit(0)
            files = db_manager.select_items_to_process_by_list(files_properties, input_data.step_name,
                                                               input_data.work_flow_log_id,
                                                               gc_mysql_params.work_flow_step_log_id)
        except Exception as e:
            logging.error(f"Failed to list files or process items: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_mysql_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    elif gc_mysql_params.source_list_type == "RECORD":
        try:
            files = db_manager.select_items_to_process_by_records(input_data.work_flow_log_id,
                                                                  gc_mysql_params.work_flow_step_log_id,
                                                                  input_data.step_name)
        except Exception as e:
            logging.error(f"Failed to select items by records: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_mysql_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if not files:
        logging.info("No NEW files on gs to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gc_mysql_params.work_flow_step_log_id, "success",
                                  f"no_new_gs_files_detected")
        sys.exit(0)

    try:
        load_gs_file_to_mysql_table(files,  gc_mysql_params, db_manager)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                 gc_mysql_params.work_flow_step_log_id, "success",
                                  f"all_gs_files_loaded_to_bq")
    except Exception as e:
        logging.error(f"Failed to process GS to BQ files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gc_mysql_params.work_flow_step_log_id, "failed", f"{e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input JSON and fetch workflow data.")
    parser.add_argument("--result", required=True, help="Input JSON in dictionary format")
    parser.add_argument("--workflow_name", required=True, help="Name of workflow executing")
    parser.add_argument("--step_name", required=True, help="Name of step to execute")

    args = parser.parse_args()

    try:
        input_dict = json.loads(args.result)
        input_data = InputParams(
            meta_db_secret_name=input_dict['meta_db_secret_name'],
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param')
        )
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
