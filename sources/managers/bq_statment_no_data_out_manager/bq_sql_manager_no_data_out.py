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


def run_bq_sql_no_data_out(gc_bq_params: GcBQParams,
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
        if gc_bq_params.requires_boundary=="Y":
            start_boundary=gc_bq_params.Start_Boundary
            end_boundary_query=gc_bq_params.end_boundary_query
        else:
            start_boundary = None
            end_boundary_query = None
        query = gc_bq_params.query.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query =query.replace("|||WorkFlow_Step_Log_id|||",gc_bq_params.work_flow_step_log_id)

        json_source=bq.run_query_with_boundaries(gc_bq_params.requires_boundary,query,end_boundary_query,start_boundary)


        logging.info(f"json_source JSON= {json.dumps(json_source)}")




        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
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

    #gcs = gcs_manager.GCSManager()
    db_manager = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database
    )
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
    end_boundary_query= end_boundary_query.replace('| | | TalendJobBundleRunId | | |', input_data.work_flow_log_id)
    end_boundary_query = end_boundary_query.replace('|||TalendLogID|||', gc_bq_params.work_flow_step_log_id)
    gc_bq_params.end_boundary_query = end_boundary_query


    ##logging.info(f"gc_bq_params: {gc_bq_params}")
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

    run_bq_sql_no_data_out(gc_bq_params, db_manager)
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
