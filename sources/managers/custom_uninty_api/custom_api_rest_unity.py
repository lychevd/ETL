import os
import posixpath
import sys
import argparse
from dataclasses import dataclass, field, fields
from core import gcs_manager, pgp_manager, secret_manager, sftp_manager,gsutil_manager,local_util,rest_api_manager,secret_manager
from db import database_manager
import requests
import logging
import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from typing import List
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class InputParams:
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    meta_db_secret_name: str
    additional_param: str = field(default=None, metadata={"optional": True})

@dataclass
class RestParameters:
    rest_api_conn_secret: str
    headers_to_get_auth: str
    url_to_get_auth: str
    app_key: str
    base_url: str
    app_name: str
    app_os: str
    num_days:str
@dataclass
class GsParams:
    work_flow_step_log_id: str
    local_data_directory: str
    gs_bucket_name_destination: str
    gs_directory_destination: str
    service_account_path: str
    google_project_name: str
    get_key_file_from_sec: str
    get_key_file_sec_name: str
    def_gs_cred: str
    def_gs_project: str
    service_account_path: str
    clean_local_dir: str


def main():
    db_manager = None
    variables = None

    try:
        # Create the db_manager
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

        from_secret_list = (
            variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

        )
        from_secret_list = json.loads(from_secret_list)
        #logging.info(f"variables={variables}")
        rest_params = RestParameters(
            rest_api_conn_secret=secrets.get_variable_value( 'rest_api_conn_secret',  variables, from_secret_list),
            headers_to_get_auth=secrets.get_variable_value( 'headers_to_get_auth',  variables, from_secret_list),
            url_to_get_auth=secrets.get_variable_value('url_to_get_auth', variables, from_secret_list),
            app_key=secrets.get_variable_value('app_key', variables, from_secret_list),
            base_url=secrets.get_variable_value( 'base_url', variables, from_secret_list),
            app_name=secrets.get_variable_value('app_name', variables, from_secret_list),
            app_os=secrets.get_variable_value( 'app_os',variables, from_secret_list),
            num_days=secrets.get_variable_value( 'num_days', variables, from_secret_list)
        )
        #logging.info(f"rest_params={rest_params}")
        file_name_downloaded=""
        gs_params =  GsParams(
            work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id',  variables, from_secret_list),
            service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
            google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
            local_data_directory=secrets.get_variable_value( 'local_data_directory', variables, from_secret_list),
            get_key_file_from_sec=secrets.get_variable_value( 'get_key_file_from_sec', variables, from_secret_list),
            get_key_file_sec_name=secrets.get_variable_value( 'get_key_file_sec_name', variables, from_secret_list),
            def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
            def_gs_project=secrets.get_variable_value( 'def_gs_project', variables, from_secret_list),
            clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list),
            gs_bucket_name_destination=secrets.get_variable_value('gs_bucket_name_destination', variables, from_secret_list),
            gs_directory_destination=secrets.get_variable_value( 'gs_directory_destination', variables, from_secret_list)
        )

        #logging.info(f"gs_params={gs_params}")
        secrets = secret_manager.SecretManager()
        if gs_params.get_key_file_from_sec == "Y":
            try:
                logging.info("Getting  key from secret")
                secret_content = secrets.fetch_secret(
                    gs_params.get_key_file_sec_name
                )
                logging.info(f"Secret content fetched successfully")
                secrets.write_secret_to_file(secret_content, gs_params.service_account_path)
            except Exception as e:
                logging.error(f"Failed to process secret: {e}")
                raise e
        if gs_params.def_gs_cred == "Y":
            service_account_path = None
        else:
            service_account_path = gs_params.service_account_path
        if gs_params.def_gs_project == "Y":
            google_project_name = None
        else:
            google_project_name = gs_params.google_project_name
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise

        # Convert the JSON string into a Python dict
        headers_to_get_auth_json = json.loads(rest_params.headers_to_get_auth)
        # Instantiate your RestApiManager (assuming it's a class, you might need parentheses)
        rest_api = rest_api_manager.RestApiManager
        logging.info(f"headers_to_get_auth_json={headers_to_get_auth_json}")
        logging.info(f"rest_params.url_to_get_auth={rest_params.url_to_get_auth}")

        # Get token
        try:
            token = rest_api.get_string_response(rest_params.url_to_get_auth, headers_to_get_auth_json)
        except requests.exceptions.RequestException as req_err:
            logging.error(f"An error occurred: {req_err}")
            raise req_err
        logging.info(f"token={token}")

        # Clean the token string if needed
        token = token.replace('"', '')
        logging.info(f"Bearer {token}")

        # Prepare headers for data retrieval
        headers_to_get_data = {
            "Authorization": f"Bearer {token}"
        }
        try:
            num_days = int(rest_params.num_days)
        except ValueError as e:
            logging.error(f"Cannot convert '{rest_params.num_days}' to an integer.")
            raise e
        # Example: Get the last 10 days (local time)
        dates_custom_format = rest_api.get_previous_dates(
            num_days=num_days,
            use_utc=False,
            date_format="%Y-%m-%d"
        )

        # Iterate over each date
        for date_str in dates_custom_format:
            logging.info(f"Processing date: {date_str}")

            params_to_get_link = {
                "appKey": f"{rest_params.app_key}",
                "date": f"{date_str}"
            }
            try:
                data_url = rest_api.get_string_response(rest_params.base_url, headers_to_get_data, None,
                                                        params_to_get_link)
            except Exception as e:
                logging.error(f"data_url: {data_url}")
                raise e
            logging.info(f"data_url: {data_url}")

            if data_url != "no_data_for_report":
                parsed_data = json.loads(data_url)

                try:
                    download_url = parsed_data["urls"][0]
                except KeyError as e:
                    logging.error("The JSON does not contain the expected 'urls' key or the list is empty.")
                    raise e
                local_data_directory = gs_params.local_data_directory
                if not local_data_directory.endswith("/"):
                    local_data_directory += "/"
                file_name = f"{local_data_directory}{rest_params.app_name}_{rest_params.app_os}_{rest_params.app_key}_{date_str}.gz"

                logging.info(f"Downloading data from {download_url} using file {file_name}")
                try:
                    file_name_downloaded = rest_api.download_file_using_url(
                        download_url,
                        file_name,
                        None,
                        chunk_size=8192
                    )
                except Exception as e:
                    logging.error(f"Failed to load file data_url: {data_url}")
                    raise e
                logging.info(f"After getting file")
                logging.info(f"Report file ={file_name_downloaded}")
                gs_directory_destination = gs_params.gs_directory_destination
                if not gs_directory_destination.endswith("report_date="):
                     gs_directory_destination += "report_date="

                gs_directory_destination=gs_directory_destination+date_str+"/"
                logging.info(f"gs_directory_destination ={gs_directory_destination}")
                try:
                    gsutil.push_file_to_gs_util(gs_params.gs_bucket_name_destination, gs_directory_destination, file_name_downloaded)
                except Exception as e:
                    logging.error(e)
                    raise e
                logging.info(
                    f"Uploaded {file_name_downloaded} to GS bucket {gs_params.gs_bucket_name_destination}. as {gs_directory_destination}/{file_name_downloaded} ) ")
                file_name_downloaded_short=os.path.basename(file_name_downloaded)
                json_data_successor = {
                    'filename_full': posixpath.join(gs_directory_destination, file_name_downloaded_short),
                    'filename_base': file_name_downloaded_short,
                    'gs_bucket_name': gs_params.gs_bucket_name_destination
                }
                json_data_destination = {
                    'data_url':data_url,
                    'app_key':rest_params.app_key,
                    'app_name': rest_params.app_name,
                    'app_os': rest_params.app_os,
                    'report_date':date_str,
                    'filename_full': posixpath.join(gs_directory_destination, file_name_downloaded),
                    'filename_base': file_name_downloaded,
                    'gs_bucket_name': gs_params.gs_bucket_name_destination
                }
                db_manager.create_dataset_instance(input_data.work_flow_log_id, gs_params.work_flow_step_log_id,
                                                   input_data.step_name, "Destination", json_data_destination)

                db_manager.create_dataset_instance(input_data.work_flow_log_id, gs_params.work_flow_step_log_id,
                                                   input_data.step_name, "Successor", json_data_successor)
                if gs_params.clean_local_dir == "Y" and file_name_downloaded != "":
                    os.remove(file_name_downloaded)
            if  data_url == "no_data_for_report":
                json_data_destination = {
                    'data_url': data_url,
                    'app_key': rest_params.app_key,
                    'app_name': rest_params.app_name,
                    'app_os': rest_params.app_os,
                    'report_date': date_str}
                db_manager.create_dataset_instance(input_data.work_flow_log_id, gs_params.work_flow_step_log_id,
                                                   input_data.step_name, "Destination", json_data_destination)
        # e.g. build rest_params from variables, run logic, etc.



        # If everything completes, mark the step as succeeded
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            gs_params.work_flow_step_log_id,
            "success",
            "All dates processed successfully."
        )

    except Exception as top_level_error:
        logging.error(f"Top-level error in processing: {top_level_error}")

        # Only try to close the log if db_manager and variables actually exist
        if db_manager and variables is not None:
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                gs_params.work_flow_step_log_id,
                "failed",
                f"{top_level_error}"
            )

        # Re-raise the exception to stop execution (or use sys.exit(1))
        sys.exit(1)


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
        logging.info(f"Parsed input JSON: {input_data}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()






