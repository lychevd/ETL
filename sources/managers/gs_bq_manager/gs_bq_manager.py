import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields

from pyjsparser.parser import false

from core import gcs_manager, secret_manager, bq_manager,gsutil_manager
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
class GcBQParams:
    work_flow_step_log_id: str
    source_list_type: str
    get_key_file_sec_name: str
    gs_bucket_name: str
    gs_directory: str
    field_delimiter: str
    bigquery_project: str
    bigquery_dataset_id: str
    import_format: str
    post_process_type: str
    has_post_process: str
    gs_directory_done: str
    gs_bucket_name_done: str
    service_account_path: str
    write_disposition: str
    create_disposition: str
    autodetect: str
    schema_json: str
    skip_leading_rows: str
    get_key_file_from_sec: str
    def_bq_cred: str
    def_bq_project: str
    table_id: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def str_to_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes", "y", "t")


def load_gs_file_to_bq(file_list, gc_bq_params: GcBQParams, db_manager: database_manager.DatabaseManager):
    try:
        service_account_path = None if gc_bq_params.def_bq_cred == "Y" else gc_bq_params.service_account_path
        bq_project_name = None if gc_bq_params.def_bq_project == "Y" else gc_bq_params.bigquery_project
        logging.info(f"service_account_path: {service_account_path}")
        logging.info(f"bq_project_name: {bq_project_name}")
        if service_account_path:
            try:
                with open(service_account_path, "r") as f:
                    content = f.read()
                   ## logging.info(f"Contents of service_account_path ({service_account_path}):\n{content}")
            except Exception as e:
                logging.error(f"Failed to read service_account_path {service_account_path}: {e}")
        try:
            gsutil = gsutil_manager.GSUtilClient()

        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise

        logging.info("Client GCS set successful!")
        try:
            bq = bq_manager.BQManager(service_account_path, bq_project_name)
        except Exception as err:
            logging.error(f"Error opening bq_manager: {err}")
            raise

        logging.info(f"Client BQ set successful! Using {gc_bq_params.service_account_path}")



        for item in file_list:
            logging.info(f"Start Processing Item = {json.dumps(item)}")
            try:
                filename_full = item['filename_full']
                filename_base = item['filename_base']
                gs_bucket_name = item['gs_bucket_name']

                try:
                    if gc_bq_params.schema_json and gc_bq_params.schema_json.strip():
                        schema_json = json.loads(gc_bq_params.schema_json.strip())
                    else:
                        schema_json = None
                except json.JSONDecodeError:
                    logging.warning("Invalid schema_json provided, defaulting to None")
                    schema_json = None

                bq.load_gcs_file_to_table(
                    bucket_name=gs_bucket_name,
                    filename_full=filename_full,
                    dataset_id=gc_bq_params.bigquery_dataset_id,
                    table_id=gc_bq_params.table_id,
                    file_format=gc_bq_params.import_format,
                    write_disposition=gc_bq_params.write_disposition,
                    create_disposition=gc_bq_params.create_disposition,
                    autodetect=str_to_bool(gc_bq_params.autodetect),
                    schema_json=schema_json,
                    skip_leading_rows=int(gc_bq_params.skip_leading_rows),
                    field_delimiter=gc_bq_params.field_delimiter,
                    project_id=gc_bq_params.bigquery_project
                )

                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    gc_bq_params.work_flow_step_log_id,
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
                    gc_bq_params.work_flow_step_log_id,
                    input_data.step_name,
                    "Successor",
                    json_data_successor
                )

            except Exception as err:
                logging.error(f"Error processing item {item}: {err}")
                raise
            if gc_bq_params.has_post_process == "Y":
                gsutil.gs_post_process(
                    post_process_type=gc_bq_params.post_process_type,
                    gs_bucket_name=gc_bq_params.gs_bucket_name,
                    filename_full=filename_full,
                    gs_bucket_name_done=gc_bq_params.gs_bucket_name_done,
                    gs_done_directory=gc_bq_params.gs_directory_done
                )
                logging.info(f"Post-processing completed for file {filename_full}.")

    except Exception as err:
        logging.error(f"Error in load_gs_file_to_bq: {err}")
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

    gc_bq_params = GcBQParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        source_list_type=secrets.get_variable_value('source_list_type', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list),
        bigquery_dataset_id=secrets.get_variable_value('bigquery_dataset_id', variables, from_secret_list),
        import_format=secrets.get_variable_value('import_format', variables, from_secret_list),
        post_process_type=secrets.get_variable_value('post_process_type', variables, from_secret_list),
        has_post_process=secrets.get_variable_value('has_post_process', variables, from_secret_list),
        gs_directory_done=secrets.get_variable_value('gs_directory_done', variables, from_secret_list),
        gs_bucket_name_done=secrets.get_variable_value('gs_bucket_name_done', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        write_disposition=secrets.get_variable_value('write_disposition', variables, from_secret_list),
        create_disposition=secrets.get_variable_value('create_disposition', variables, from_secret_list),
        autodetect=secrets.get_variable_value('autodetect', variables, from_secret_list),
        schema_json=secrets.get_variable_value('schema_json', variables, from_secret_list),
        skip_leading_rows=secrets.get_variable_value('skip_leading_rows', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        def_bq_cred=secrets.get_variable_value('def_bq_cred', variables, from_secret_list),
        def_bq_project=secrets.get_variable_value('def_bq_project', variables, from_secret_list),
        table_id=secrets.get_variable_value('table_id', variables, from_secret_list)
    )

    ##logging.info(f"gc_bq_params: {gc_bq_params}")

    if gc_bq_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting key from secret")
            secret_content = secrets.fetch_secret(gc_bq_params.get_key_file_sec_name)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gc_bq_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if gc_bq_params.source_list_type == "LIST":
        try:
            files_properties = gcs.list_files_with_properties(gc_bq_params.gs_bucket_name, gc_bq_params.gs_directory)
            logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
            if not files_properties:
                logging.info("No gs files to process. Exiting.")
                db_manager.close_step_log(input_data.workflow_name, input_data.step_name,
                                          input_data.work_flow_log_id,
                                          gc_bq_params.work_flow_step_log_id, "success",
                                          f"no_gs_files_detected")
                sys.exit(0)
            files = db_manager.select_items_to_process_by_list(files_properties, input_data.step_name,
                                                               input_data.work_flow_log_id,
                                                               gc_bq_params.work_flow_step_log_id)
        except Exception as e:
            logging.error(f"Failed to list files or process items: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    elif gc_bq_params.source_list_type == "RECORD":
        try:
            files = db_manager.select_items_to_process_by_records(input_data.work_flow_log_id,
                                                                  gc_bq_params.work_flow_step_log_id,
                                                                  input_data.step_name)
        except Exception as e:
            logging.error(f"Failed to select items by records: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if not files:
        logging.info("No NEW files on gs to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gc_bq_params.work_flow_step_log_id, "success",
                                  f"no_new_gs_files_detected")
        sys.exit(0)

    try:
        load_gs_file_to_bq(files, gc_bq_params, db_manager)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gc_bq_params.work_flow_step_log_id, "success",
                                  f"all_gs_files_loaded_to_bq")
    except Exception as e:
        logging.error(f"Failed to process GS to BQ files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
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
