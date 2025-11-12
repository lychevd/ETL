import sys
import argparse
import re
from dataclasses import dataclass, field, fields
import requests
from core import secret_manager, local_util, ms_sql_client_manager
from db import database_manager
import pandas as pd
import logging
import json
import sys
from typing import Optional, List
import datetime

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
    mssql_conn_secret: str
    work_flow_step_log_id: str
    query: str
    insert_query: str
    http_method: str
    headers: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

def submit_api_requests(df: pd.DataFrame, http_method: str, headers: dict, db_client, insert_query: str,
                       work_flow_log_id: str, work_flow_step_log_id: str, return_identity: bool = False) -> Optional[List]:
    """
    Submits API requests for each row in the DataFrame and inserts results into a database using the provided INSERT query.
    Cleans API response body by removing \r and \n. Ignores input Body if it is None or an empty string for requests.
    Adds ExecutionDate as current timestamp for each row.

    Args:
        df (pd.DataFrame): DataFrame with columns 'producer_tracking_id', 'url', 'Body'
        http_method (str): HTTP method ('GET', 'POST', or 'DELETE')
        headers (dict): Dictionary of up to 4 headers (key-value pairs)
        db_client: Database client with insert_single_row method
        insert_query (str): Parameterized SQL INSERT query with 6 placeholders (producer_tracking_id, Body, ERROR_CODE, WorkFlow_Log_id, WorkFlow_Step_Log_id, ExecutionDate)
        work_flow_log_id (str): Workflow log ID
        work_flow_step_log_id (str): Workflow step log ID
        return_identity (bool): If True, returns list of identity values; else returns list of affected row counts

    Returns:
        Optional[List]: List of identity values or affected row counts, or None if no records
    """
    try:
        import requests
        import json
        if not all(col in df.columns for col in ['producer_tracking_id', 'url', 'Body']):
            logging.error("DataFrame missing required columns: producer_tracking_id, url, Body")
            raise ValueError("DataFrame must contain producer_tracking_id, url, and Body columns")

        if http_method.upper() not in ['GET', 'POST', 'DELETE']:
            logging.error(f"Invalid HTTP method: {http_method}")
            raise ValueError("http_method must be GET, POST, or DELETE")

        if len(headers) > 4:
            logging.warning("More than 4 headers provided; using only the first 4")
            headers = dict(list(headers.items())[:4])

        if not insert_query.strip().upper().startswith('INSERT'):
            logging.error("Provided query is not an INSERT statement")
            raise ValueError("insert_query must be a valid SQL INSERT statement")

        results = []
        for _, row in df.iterrows():
            producer_id = row['producer_tracking_id']
            url = row['url']
            body = row['Body']
            logging.info(f"producer_id={producer_id}")
            logging.info(f"body={body}")
            logging.info(f"url={url}")

            try:
                # Parse body as JSON for POST and DELETE requests
                json_body = None
                if http_method.upper() in ['POST', 'DELETE'] and body and body != '':
                    try:
                        json_body = json.loads(body) if isinstance(body, str) else body
                        logging.info(f"Parsed JSON body for producer_tracking_id {producer_id}: {json_body}")
                    except json.JSONDecodeError as e:
                        logging.error(f"Invalid JSON in Body for producer_tracking_id {producer_id}: {e}")
                        response_body = ''
                        error_code = f"JSONDecodeError: {str(e)}"
                        execution_date = datetime.datetime.now()
                        params = (producer_id, response_body, error_code, work_flow_log_id, work_flow_step_log_id, execution_date)
                        result = db_client.insert_single_row(insert_query, params, return_identity)
                        results.append(result)
                        continue

                # Make the API request
                if http_method.upper() == 'GET':
                    response = requests.get(url, headers=headers, params=body if body and body != '' else None)
                elif http_method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=json_body)
                    logging.info(f"Sent POST request for producer_tracking_id {producer_id} with body: {json_body}")
                elif http_method.upper() == 'DELETE':
                    response = requests.delete(url, headers=headers, json=json_body)

                logging.info(f"API request for producer_tracking_id {producer_id}: HTTP {response.status_code}")
                response_body = re.sub(r'[\r\n]', '', response.text)
                error_code = str(response.status_code)
                execution_date = datetime.datetime.now()

                params = (producer_id, response_body, error_code, work_flow_log_id, work_flow_step_log_id, execution_date)
                result = db_client.insert_single_row(insert_query, params, return_identity)
                results.append(result)
                logging.info(f"Inserted result for producer_tracking_id {producer_id}: {result}")

            except requests.RequestException as e:
                logging.error(f"API request failed for producer_tracking_id {producer_id}: {e}")
                response_body = ''
                error_code = str(e)
                execution_date = datetime.datetime.now()

                params = (producer_id, response_body, error_code, work_flow_log_id, work_flow_step_log_id, execution_date)
                result = db_client.insert_single_row(insert_query, params, return_identity)
                results.append(result)
                logging.info(f"Inserted error result for producer_tracking_id {producer_id}: {result}")

        logging.info(f"Inserted {len(results)} records using provided INSERT query")
        return results if results else None

    except Exception as e:
        logging.error(f"Error in submit_api_requests: {e}")
        raise

def get_data_from_sql_and_submit_to_api(ms_sql_params_out: MsSqlParamsOut,
                                       db_manager: database_manager.DatabaseManager) -> None:
    """
    Retrieves data from SQL Server, submits to API, and logs results to a database.
    Cleans API response body by removing \r and \n before insertion.
    Adds ExecutionDate as current timestamp for each inserted row.

    Args:
        ms_sql_params_out (MsSqlParamsOut): SQL connection parameters, query, http_method, headers (JSON string), insert_query
        db_manager (database_manager.DatabaseManager): Workflow and dataset manager
    """
    try:
        # Initialize local utilities
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise

        # Initialize MS SQL client
        try:
            ms_sql_client = ms_sql_client_manager.SQLClient(
                ms_sql_params_out.ms_sql_server,
                ms_sql_params_out.ms_sql_database,
                ms_sql_params_out.ms_sql_user,
                ms_sql_params_out.ms_sql_password,
                ms_sql_params_out.ms_sql_port
            )
            logging.info("ms sql connection successful!")
        except Exception as err:
            logging.error(f"Error opening ms sql connection: {err}")
            raise

        # Validate workflow IDs before query adjustment
        if not input_data.work_flow_log_id or not ms_sql_params_out.work_flow_step_log_id:
            logging.error("Workflow log ID or step log ID is missing")
            raise ValueError("Missing required workflow IDs")

        # Adjust query with workflow IDs
        query_adjusted = ms_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", ms_sql_params_out.work_flow_step_log_id)
        logging.info(f"Adjusted query: {query_adjusted}")

        # Execute query to get DataFrame
        df = ms_sql_client.execute_query_data_pandas(query_adjusted)
        row_count = df.shape[0]
        logging.info(f"Procedure returned DataFrame with {row_count} rows and {df.shape[1]} columns")

        json_source = {
            'query': query_adjusted,
            'file_row_count': str(row_count)
        }
        logging.info(f"json_source JSON= {json.dumps(json_source)}")

        # Log dataset instance
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )

        # Handle empty DataFrame
        if row_count == 0:
            logging.info("No records to submit to API. Exiting successfully.")
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                "success",
                "no_records_to_output"
            )
            sys.exit(0)

        # Submit API requests and insert results
        try:
            insert_query = ms_sql_params_out.insert_query
            http_method = ms_sql_params_out.http_method
            # Convert headers string to dictionary
            try:
                headers = json.loads(
                    ms_sql_params_out.headers) if ms_sql_params_out.headers and ms_sql_params_out.headers != 'null' else {}
                if not isinstance(headers, dict):
                    logging.error("Headers must be a dictionary after parsing")
                    raise ValueError("Parsed headers is not a dictionary")
                if len(headers) > 4:
                    logging.warning("More than 4 headers provided; using only the first 4")
                    headers = dict(list(headers.items())[:4])
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse headers JSON string: {e}")
                db_manager.close_step_log(
                    input_data.workflow_name,
                    input_data.step_name,
                    input_data.work_flow_log_id,
                    ms_sql_params_out.work_flow_step_log_id,
                    "FAILED",
                    f"Invalid headers JSON: {e}"
                )
                raise
            except Exception as e:
                logging.error(f"Error processing headers: {e}")
                db_manager.close_step_log(
                    input_data.workflow_name,
                    input_data.step_name,
                    input_data.work_flow_log_id,
                    ms_sql_params_out.work_flow_step_log_id,
                    "FAILED",
                    f"Error processing headers: {e}"
                )
                raise

            results = submit_api_requests(
                df=df,
                http_method=http_method,
                headers=headers,
                db_client=ms_sql_client,
                insert_query=insert_query,
                work_flow_step_log_id=ms_sql_params_out.work_flow_step_log_id,
                work_flow_log_id=input_data.work_flow_log_id,
                return_identity=False
            )
            if results is None:
                logging.warning("No results returned from submit_api_requests")
            else:
                logging.info(f"API submission results: {results}")

            # Close step log with success
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                "success",
                f"Processed {row_count} records, inserted {len(results) if results else 0} results"
            )

        except Exception as api_err:
            logging.error(f"Error submitting API requests: {api_err}")
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                "FAILED",
                str(api_err)
            )
            raise

    except Exception as err:
        logging.error(f"Error in get_data_from_sql_and_submit_to_api: {err}")
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
    ms_sql_params_out = MsSqlParamsOut(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        mssql_conn_secret=secrets.get_variable_value('mssql_conn_secret', variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value('ms_sql_server', variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value('ms_sql_database', variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value('ms_sql_password', variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value('ms_sql_port', variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value('ms_sql_schema', variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value('ms_sql_user', variables, from_secret_list),
        query=secrets.get_variable_value('query', variables, from_secret_list),
        insert_query=secrets.get_variable_value('insert_query', variables, from_secret_list),
        headers=secrets.get_variable_value('headers', variables, from_secret_list),
        http_method=secrets.get_variable_value('http_method', variables, from_secret_list),
    )

    logging.info(f"my_sql_params_out={ms_sql_params_out}")
    get_data_from_sql_and_submit_to_api(ms_sql_params_out, db_manager)
    db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                             ms_sql_params_out.work_flow_step_log_id, "success", "submitted_ok_to_api")

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