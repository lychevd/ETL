import sys
import argparse
import logging
import json
import posixpath
from dataclasses import dataclass, field, fields
from core import gcs_manager, secret_manager, bq_manager, gsutil_manager
from db import database_manager
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
    gs_bucket_name: str
    gs_directory: str
    local_data_directory: str
    service_account_path: str
    query: str
    end_boundary_query: str
    field_delimiter: str
    row_delimiter: str
    Start_Boundary: str
    filename_base: str
    print_header: str
    bigquery_project: str
    requires_boundary: str = field(default='Y', metadata={"optional": True})
    get_key_file_from_sec: str = field(default='Y', metadata={"optional": True})
    def_bq_cred: str = field(default='Y', metadata={"optional": True})
    def_bq_project: str = field(default='Y', metadata={"optional": True})
    get_key_file_sec_name: str = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def str_to_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes", "y", "t")


def create_gs_file_from_bq(gc_bq_params: GcBQParams, db_manager: database_manager.DatabaseManager):
    try:
        if gc_bq_params.def_bq_cred == "Y":
            service_account_path = None
        else:
            service_account_path = gc_bq_params.service_account_path

        if gc_bq_params.def_bq_project == "Y":
            bq_project_name = None
        else:
            bq_project_name =gc_bq_params.bigquery_project

        try:
            bq = bq_manager.BQManager(service_account_path, bq_project_name)

        except Exception as err:
            logging.error(f"Error opening bq_manager: {err}")
            raise
        ##value = bq.run_query_and_return_single_value("SELECT COUNT(*) FROM `my_project.my_dataset.my_table`")
        logging.info(f"Client BQ set successful! Using {gc_bq_params.service_account_path}")

        try:
            gcs = gcs_manager.GCSManager()
        except Exception as err:
            logging.error(f"Error opening gcs_manager: {err}")
            raise

        logging.info("client gcp set successful!")

        try:
            gsutil = gsutil_manager.GSUtilClient()
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise

        query = gc_bq_params.query.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query = query.replace("|||WorkFlow_Step_Log_id|||", gc_bq_params.work_flow_step_log_id)

        full_path = os.path.join(gc_bq_params.local_data_directory, gc_bq_params.filename_base)
        include_header = str_to_bool(gc_bq_params.print_header)

        row_count, json_source = bq.export_query_to_local_file(
            query,
            full_path,
            gc_bq_params.field_delimiter,
            gc_bq_params.row_delimiter,
            include_header,
            gc_bq_params.requires_boundary,
            gc_bq_params.end_boundary_query,
            gc_bq_params.Start_Boundary
        )

        logging.info(f"json_source JSON= {json.dumps(json_source)}")
        logging.info(f"row_count= {row_count}")

        if row_count == 0:
            logging.info("No rows were inserted into the temp table. Exiting successfully.")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                json_source
            )
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                "success",
                "no_records_to_output"
            )
            sys.exit(0)

        if not gc_bq_params.gs_directory.endswith('/'):
            gc_bq_params.gs_directory += '/'

        try:
            gsutil.push_file_to_gs_util(gc_bq_params.gs_bucket_name, gc_bq_params.gs_directory, full_path)
        except Exception as err:
            logging.error(f"Error pushing file to GS: {err}")
            raise

        json_data_successor = {
            'filename_full': posixpath.join(gc_bq_params.gs_directory, gc_bq_params.filename_base),
            'filename_base': gc_bq_params.filename_base,
            'gs_bucket_name': gc_bq_params.gs_bucket_name,
            'file_row_count': str(row_count)
        }

        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )
        logging.info('Source created')
        logging.info(f"json_source={json_source}")

        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Successor",
            json_data_successor
        )
        logging.info('Successor created')
        logging.info(f"json_data_successor={json_data_successor}")

    except Exception as e:
        logging.error(f"Unexpected error in create_gs_file_from_bq: {e}")
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
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        input_data.additional_param
    )

    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = json.loads(
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
    )

    gc_bq_params = GcBQParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        query=secrets.get_variable_value('query', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value('end_boundary_query', variables, from_secret_list),
        requires_boundary=secrets.get_variable_value('requires_boundary', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        row_delimiter=secrets.get_variable_value('row_delimiter', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value('Start_Boundary', variables, from_secret_list),
        filename_base=secrets.get_variable_value('filename_base', variables, from_secret_list),
        print_header=secrets.get_variable_value('print_header', variables, from_secret_list),
        def_bq_cred=secrets.get_variable_value('def_bq_cred', variables, from_secret_list),
        def_bq_project=secrets.get_variable_value('def_bq_project', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list)
    )

    if gc_bq_params.get_key_file_from_sec == "Y":
        try:
            secret_content = secrets.fetch_secret(gc_bq_params.get_key_file_sec_name)
            secrets.write_secret_to_file(secret_content, gc_bq_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    create_gs_file_from_bq(gc_bq_params, db_manager)

    db_manager.close_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        gc_bq_params.work_flow_step_log_id,
        "success",
        "file_created_ok"
    )


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
