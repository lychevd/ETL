import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields



from core import  secret_manager, mysql_sql_client_manager
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
class MySqlParamsOut:
    work_flow_step_log_id: str
    mysql_host: str
    mysql_user: str
    mysql_database: str
    mysql_password: str
    mysql_port: str
    mysql_conn_secret: str
    requires_boundary: str
    query: str
    end_boundary_query: str
    Start_Boundary: str


    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")



def run_mysql_no_data_out( my_sql_params_out : MySqlParamsOut,
                           db_manager: database_manager.DatabaseManager):
    try:     # Associate program with pgp manager

        try:
            my_sql_client = mysql_sql_client_manager.MySQLClient(my_sql_params_out.mysql_host,my_sql_params_out.mysql_database,my_sql_params_out.mysql_user,my_sql_params_out.mysql_password,int(my_sql_params_out.mysql_port))

            logging.info("mysql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise
        if my_sql_params_out.requires_boundary == "Y":
            try:
                End_boundary = my_sql_client.execute_query_autocommit(my_sql_params_out.end_boundary_query, "Y")
                logging.info(f"End boundary: {End_boundary}")
                if End_boundary and len(End_boundary) > 0:
                    End_boundary = End_boundary[0][0]  # Extract the string value
                    logging.info(f"End boundary: {End_boundary}")
                else:
                    logging.error("End boundary query returned no results.")
                    End_boundary = None
            except Exception as e:
                logging.error(f"Failed to fetch end boundary: {e}")
                End_boundary = None
        query_adjusted = my_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", my_sql_params_out.work_flow_step_log_id)

        if my_sql_params_out.requires_boundary == "Y":
            query_adjusted = query_adjusted.replace("|||Start_Boundary|||", my_sql_params_out.Start_Boundary)
            query_adjusted = query_adjusted.replace("|||End_boundary|||", End_boundary)
            logging.info(f"Query after replacement={query_adjusted}")

        my_sql_client.execute_query_autocommit(query_adjusted, "N")

        json_source = {
            'query': query_adjusted,

        }

        if my_sql_params_out.requires_boundary == "Y":
            json_source.update({
                'Start_Boundary':my_sql_params_out.Start_Boundary,
                'End_Boundary': End_boundary,
            })
        logging.info(f"json_source JSON= {json.dumps(json_source)}")



        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            my_sql_params_out.work_flow_step_log_id,
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
            my_sql_params_out.work_flow_step_log_id,
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




    my_sql_params_out = MySqlParamsOut(
        work_flow_step_log_id=secrets.get_variable_value( 'WorkFlow_Step_Log_id', variables, from_secret_list),
        mysql_conn_secret=secrets.get_variable_value('mysql_conn_secret', variables, from_secret_list),
        mysql_host=secrets.get_variable_value('mysql_host', variables, from_secret_list),
        mysql_user=secrets.get_variable_value('mysql_user', variables, from_secret_list),
        mysql_database=secrets.get_variable_value('mysql_database', variables, from_secret_list),
        mysql_password=secrets.get_variable_value('mysql_password', variables, from_secret_list),
        mysql_port=secrets.get_variable_value('mysql_port', variables, from_secret_list),
        requires_boundary=secrets.get_variable_value( 'requires_boundary', variables, from_secret_list),
        query=secrets.get_variable_value( 'query', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value( 'end_boundary_query', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value('Start_Boundary', variables, from_secret_list)

    )




    run_mysql_no_data_out(my_sql_params_out, db_manager)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                              my_sql_params_out.work_flow_step_log_id, "success", "file_created_ok")

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
