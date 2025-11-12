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
    ResponseSectionCode: str
    CreateTableStatement: str
    InsIntoStatement: str
    SelectStatement: str
    query:str


    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

def build_recon_sql_block(
    feed_batch_id: str,
    response_section_code: str,
    create_table_statement: str,
    insert_into_statement: str,
    select_statement: str,
    drop_temp_table: int = 1,
    dev_mode: int = 0
) -> str:
    def escape_sql_string(s):
        return s.replace("'", "''")

    return f"""
    SET @in_FeedBatchId := '{escape_sql_string(feed_batch_id)}';
    SET @in_ResponseSectionCode := '{escape_sql_string(response_section_code)}';
    SET @in_CreateTableStatement := '{escape_sql_string(create_table_statement)}';
    SET @in_InsIntoStatement := '{escape_sql_string(insert_into_statement)}';
    SET @in_SelectStatement := '{escape_sql_string(select_statement)}';
    SET @in_DropTempTable := {drop_temp_table};
    SET @in_DevMode := {dev_mode};

    CALL usp_ReconJson(
        @in_FeedBatchId,
        @in_ResponseSectionCode,
        @in_CreateTableStatement,
        @in_InsIntoStatement,
        @in_SelectStatement,
        @in_DropTempTable,
        @in_DevMode,
        @out_NumOfRecords
    );

    SELECT @out_NumOfRecords AS NumOfRecords;
    """

def run_mysql_no_data_out( my_sql_params_out : MySqlParamsOut,
                           db_manager: database_manager.DatabaseManager):
    try:     # Associate program with pgp manager

        try:
            my_sql_client = mysql_sql_client_manager.MySQLClient(my_sql_params_out.mysql_host,my_sql_params_out.mysql_database,my_sql_params_out.mysql_user,my_sql_params_out.mysql_password,int(my_sql_params_out.mysql_port))

            logging.info("mysql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise
        try:
             my_sql_client.execute_query_autocommit(my_sql_params_out.query ,"N")
        except Exception as err:
            logging.error(f"Error processing JSON: {err}")
            raise
        logging.info("JSON PARSING OK!")
        json_source = {
            'query': my_sql_params_out.query

        }





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
        ResponseSectionCode=secrets.get_variable_value('ResponseSectionCode', variables, from_secret_list),
        CreateTableStatement=secrets.get_variable_value('CreateTableStatement', variables, from_secret_list),
        InsIntoStatement=secrets.get_variable_value('InsIntoStatement', variables, from_secret_list),
        SelectStatement=secrets.get_variable_value('SelectStatement', variables, from_secret_list),
       query=secrets.get_variable_value('query', variables, from_secret_list)

    )

    sql_block = build_recon_sql_block(
        feed_batch_id=input_data.work_flow_log_id,
        response_section_code=my_sql_params_out.ResponseSectionCode,
        create_table_statement=my_sql_params_out.CreateTableStatement,
        insert_into_statement=my_sql_params_out.InsIntoStatement,
        select_statement=my_sql_params_out.SelectStatement
    )
    my_sql_params_out.query=sql_block
    logging.info(f"sql_block={sql_block}")
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
