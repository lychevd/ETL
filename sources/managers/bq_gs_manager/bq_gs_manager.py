import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields

from pyjsparser.parser import false

from core import gcs_manager, secret_manager,bq_manager
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
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    query: str
    end_boundary_query: str
    field_delimiter: str
    google_location: str
    bigquery_project: str
    bigquery_dataset_id: str
    compression: str
    export_format: str
    get_key_file_sec_name: str
    Start_Boundary: str
    filename_base: str
    print_header: bool
    is_header_dynamic: str
    sql_dynamic_header: str
    merge_file: str
    header_line: str
    drop_temp_dir: str
    dest_file_template:str
    row_delimiter:str
    split_count:str
    pad_need:str
    offset: int
    pad_char: str
    left_count: int
    right_count: int
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


def create_gs_file_from_bq(gc_bq_params: GcBQParams,
                           db_manager: database_manager.DatabaseManager):
    try:
        if gc_bq_params.def_bq_cred == "Y":
            service_account_path = None
        else:
            service_account_path = gc_bq_params.service_account_path
        if gc_bq_params.def_bq_project == "Y":
            bq_project_name = None
        else:
            bq_project_name = gc_bq_params.bigquery_project
        try:
            bq = bq_manager.BQManager(service_account_path,
                                      bq_project_name
                                      )
        except Exception as err:
            logging.error(f"Error opening bq_manager: {err}")
            raise
        logging.info(f"Client BQ set successful! Using {gc_bq_params.service_account_path}")

        try:
            gcs = gcs_manager.GCSManager()
        except Exception as err:
            logging.error(f"Error opening gcs_manager: {err}")
            raise
        logging.info(f"client gcp set successful! ")

        query = gc_bq_params.query.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query = query.replace("|||WorkFlow_Step_Log_id|||", gc_bq_params.work_flow_step_log_id)


        row_count, json_source, temp_table_id = bq.run_query_into_table(
            gc_bq_params.bigquery_project,
            gc_bq_params.bigquery_dataset_id,
            gc_bq_params.requires_boundary,
            query,
            gc_bq_params.end_boundary_query,
            gc_bq_params.Start_Boundary
        )
        logging.info(f"json_source JSON= {json.dumps(json_source)}")
        logging.info(f"row_count= {row_count}")


        logging.info(f"temp_table_id= {temp_table_id}")

        if row_count == 0:
            logging.info("No rows were inserted into the temp table. Exiting successfully.")
            logging.info(f"json_source JSON= {json.dumps(json_source)}")
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

        if gc_bq_params.merge_file == "Y":
            temp_dir = gc_bq_params.gs_directory + "_" + os.path.splitext(gc_bq_params.filename_base)[0]
            destination_uri = f"gs://{gc_bq_params.gs_bucket_name}/{temp_dir}/*"
            logging.info(f"destination_uri={destination_uri}")
        elif gc_bq_params.merge_file == "N":
            destination_uri = f"gs://{gc_bq_params.gs_bucket_name}/{gc_bq_params.gs_directory}/{gc_bq_params.filename_base}"
            logging.info(f"destination_uri={destination_uri}")
        else:
            raise ValueError("Invalid merge_file value. Must be 'Y' or 'N'.")

        # Export to GCS
        bq.bq_table_into_gs_file(
            gc_bq_params.merge_file,
            gc_bq_params.export_format,
            gc_bq_params.field_delimiter,
            gc_bq_params.compression,
            gc_bq_params.google_location,
            temp_table_id,
            destination_uri,
            gc_bq_params.print_header
        )

        split_count = int(gc_bq_params.split_count)
        if split_count ==1:
            destination_blob_name=f"{gc_bq_params.gs_directory}{gc_bq_params.filename_base}"
        else:
            destination_blob_name = f"{gc_bq_params.gs_directory}{gc_bq_params.dest_file_template}"
        var_offset=int(gc_bq_params.offset)
        left_count=int(gc_bq_params.left_count)
        right_count=int(gc_bq_params.right_count)
        if gc_bq_params.is_header_dynamic=="Y":
            dm_header=bq.call_get_table_header_as_string( gc_bq_params.sql_dynamic_header)
        else:
            dm_header=gc_bq_params.header_line
        logging.info(f"dm_header={dm_header}")
        if gc_bq_params.merge_file == "Y":
            result_list = gcs.merge_files_in_gcs(
                gc_bq_params.gs_bucket_name,
                temp_dir,
                destination_blob_name,
                dm_header,
                gc_bq_params.print_header,
                gc_bq_params.row_delimiter,
                gc_bq_params.pad_need,
                var_offset,
                gc_bq_params.pad_char,
                left_count,
                right_count,
                gc_bq_params.drop_temp_dir,
                split_count
            )
            for file_meta in result_list:
                logging.info(f"Successor JSON= {json.dumps(file_meta)}")
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    gc_bq_params.work_flow_step_log_id,
                    input_data.step_name,
                    "Successor",
                    file_meta
                )


        if gc_bq_params.merge_file == "N":
            json_data_successor = gcs.get_gs_file_pro(
            gc_bq_params.gs_bucket_name,
            gc_bq_params.gs_directory,
            gc_bq_params.filename_base,
            row_count
        )

        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )
        if gc_bq_params.merge_file == "N":
            logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Successor",
                json_data_successor
            )



    except Exception as err:
        logging.error(f"Error in create_gs_file_from_bq: {err}")
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

    from_secret_list = (
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

    )
    from_secret_list = json.loads(from_secret_list)

    gc_bq_params = GcBQParams(
        work_flow_step_log_id=secrets.get_variable_value( 'WorkFlow_Step_Log_id', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name',  variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        query=secrets.get_variable_value('query', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value('end_boundary_query', variables, from_secret_list),
        requires_boundary=secrets.get_variable_value('requires_boundary', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        google_location=secrets.get_variable_value('google_location', variables, from_secret_list),
        bigquery_dataset_id=secrets.get_variable_value('bigquery_dataset_id', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list),
        compression=secrets.get_variable_value('compression', variables, from_secret_list),
        export_format=secrets.get_variable_value( 'export_format', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value('Start_Boundary', variables, from_secret_list),
        filename_base=secrets.get_variable_value('filename_base', variables, from_secret_list),
        print_header=secrets.get_variable_value( 'print_header', variables, from_secret_list),
        merge_file=secrets.get_variable_value('merge_file', variables, from_secret_list),
        header_line=secrets.get_variable_value('header_line', variables, from_secret_list),
        drop_temp_dir=secrets.get_variable_value('drop_temp_dir', variables, from_secret_list),
        def_bq_cred =secrets.get_variable_value( 'def_bq_cred', variables, from_secret_list),
        def_bq_project =secrets.get_variable_value( 'def_bq_project', variables, from_secret_list),
        dest_file_template= secrets.get_variable_value('dest_file_template', variables, from_secret_list),
        split_count=secrets.get_variable_value( 'split_count', variables, from_secret_list),
        row_delimiter= secrets.get_variable_value( 'row_delimiter', variables, from_secret_list),
        pad_need=secrets.get_variable_value('pad_need', variables, from_secret_list),
        offset=secrets.get_variable_value('offset',variables, from_secret_list),
        pad_char=secrets.get_variable_value('pad_char', variables, from_secret_list),
        left_count=secrets.get_variable_value('left_count', variables, from_secret_list),
        right_count=secrets.get_variable_value('right_count', variables, from_secret_list),
        is_header_dynamic=secrets.get_variable_value( 'is_header_dynamic',variables, from_secret_list),
       sql_dynamic_header=secrets.get_variable_value('sql_dynamic_header',variables, from_secret_list)
    )


    logging.info(f"gc_bq_params: {gc_bq_params}")
    if gc_bq_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                gc_bq_params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            ##logging.info(f"secret_content={secret_content}")
            secrets.write_secret_to_file(secret_content, gc_bq_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gc_bq_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    create_gs_file_from_bq(gc_bq_params, db_manager)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                              gc_bq_params.work_flow_step_log_id, "success", "file_created_ok")



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
       ## logging.info(f"Parsed input JSON: {input_data}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
