import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields



from core import  secret_manager, local_util, mysql_to_mssql_manager,msql_to_mysql_manager,ms_sql_client_manager
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
    mysql_host: str
    mysql_user: str
    mysql_database: str
    mysql_password: str
    mysql_port: str
    insert_query: str



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
    conn_secret: str
    work_flow_step_log_id:str
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




def load_mssql_dataset_to_mysql(ms_sql_params_out : MsSqlParamsOut,my_sql_params_out:MySqlParamsOut,
                           db_manager: database_manager.DatabaseManager):
    try:

        # Associate program with pgp manager
        try:
            bridge =msql_to_mysql_manager.MSSQLToMySQLBridge(
                mysql_host=my_sql_params_out.mysql_host,
                mysql_db=my_sql_params_out.mysql_database,
                mysql_user=my_sql_params_out.mysql_user,
                mysql_password=my_sql_params_out.mysql_password,
                mysql_port=my_sql_params_out.mysql_port,
                mssql_host=ms_sql_params_out.ms_sql_server,
                mssql_db=ms_sql_params_out.ms_sql_database,
                mssql_user=ms_sql_params_out.ms_sql_user,
                mssql_password=ms_sql_params_out.ms_sql_password,
                mssql_port=ms_sql_params_out.ms_sql_port
            )

            logging.info("Ini Ok!")
        except Exception as err:
            logging.error(f"Ini fail!: {err}")
            raise
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise
        try:
            ms_sql_client = ms_sql_client_manager.SQLClient(
                ms_sql_params_out.ms_sql_server, ms_sql_params_out.ms_sql_database,
                ms_sql_params_out.ms_sql_user,
                ms_sql_params_out.ms_sql_password, ms_sql_params_out.ms_sql_port
            )
            logging.info("ms sql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise
        if ms_sql_params_out.requires_boundary == "Y":
            try:
                End_boundary = ms_sql_client.execute_query_autocommit(my_sql_params_out.end_boundary_query, "Y")
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
        query_adjusted = ms_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", ms_sql_params_out.work_flow_step_log_id)

        if ms_sql_params_out.requires_boundary == "Y":
            query_adjusted = query_adjusted.replace("|||Start_Boundary|||",ms_sql_params_out.Start_Boundary)
            query_adjusted = query_adjusted.replace("|||End_boundary|||", End_boundary)
            logging.info(f"Query after replacement={query_adjusted}")

        row_count = bridge.transfer_query_results(
            mssql_query=query_adjusted,
            mysql_insert_query=my_sql_params_out.insert_query
        )


        json_source = {
            'query': query_adjusted,
            'file_row_count': str(row_count)
        }

        if ms_sql_params_out.requires_boundary == "Y":
            json_source.update({
                'Start_Boundary': ms_sql_params_out.Start_Boundary,
                'End_Boundary': End_boundary,
            })
        logging.info(f"json_source JSON= {json.dumps(json_source)}")
        logging.info(f"row_count= {str(row_count)}")


        if row_count == 0:
            logging.info("No was outuputed to the file. Exiting successfully.")
            logging.info(f"json_source JSON= {json.dumps(json_source)}")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                json_source
            )
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                "success",

                "no_records_to_output"
            )
            sys.exit(0)



        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
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
    logging.info(variables)
    ms_sql_params_out  =  MsSqlParamsOut(
        work_flow_step_log_id=secrets.get_variable_value( 'WorkFlow_Step_Log_id', variables, from_secret_list),
        conn_secret=secrets.get_variable_value( 'conn_secret', variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value('ms_sql_server', variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value( 'ms_sql_database', variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value( 'ms_sql_password', variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value( 'ms_sql_port', variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value( 'ms_sql_schema', variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value('ms_sql_user', variables, from_secret_list),
        requires_boundary = secrets.get_variable_value('requires_boundary', variables, from_secret_list),
        query = secrets.get_variable_value('query', variables, from_secret_list),
        end_boundary_query = secrets.get_variable_value('end_boundary_query', variables, from_secret_list),
        Start_Boundary = secrets.get_variable_value('Start_Boundary', variables, from_secret_list),

    )

    my_sql_params_out = MySqlParamsOut(
        mysql_host=secrets.get_variable_value('mysql_host', variables, from_secret_list),
        mysql_user=secrets.get_variable_value('mysql_user', variables, from_secret_list),
        mysql_database=secrets.get_variable_value('mysql_database', variables, from_secret_list),
        mysql_password=secrets.get_variable_value('mysql_password', variables, from_secret_list),
        mysql_port=secrets.get_variable_value('mysql_port', variables, from_secret_list),
        insert_query=secrets.get_variable_value('insert_query', variables, from_secret_list)

    )


    logging.info(f"my_sql_params_out={ my_sql_params_out}")


    load_mssql_dataset_to_mysql(ms_sql_params_out ,my_sql_params_out, db_manager)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                              ms_sql_params_out .work_flow_step_log_id, "success", "records_tranfered_ok")

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
