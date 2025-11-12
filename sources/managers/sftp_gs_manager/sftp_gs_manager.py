import sys
import argparse
import subprocess
import posixpath
import logging
import json
import os
from dataclasses import dataclass, field, fields
from core import gcs_manager, pgp_manager, secret_manager, sftp_manager,gsutil_manager,local_util,sftp_client_subprocess
from db import database_manager
import tempfile

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
class SftpGsParm:
    work_flow_step_log_id: str
    sftp_conn_secret:str
    sftp_host: str
    sftp_username: str
    sftp_password: str
    sftp_remote_path: str
    pattern: str
    local_data_directory: str
    post_process_type: str = field(default='DELETE')
    has_post_process: str = field(default='N')
    push_file_to_gs: str = field(default='Y')
    archive_pgp_to_gs: str = field(default='Y')
    delete_source_record: str = field(default='Y', metadata={"optional": True})
    sftp_port: int = field(default=22, metadata={"optional": True})
    sftp_done_directory:  str=field(default=None, metadata={"optional": True})
    gs_directory: str = field(default=None, metadata={"optional": True})
    gs_bucket_name: str = field(default=None, metadata={"optional": True})
    gs_directory_archive: str = field(default=None, metadata={"optional": True})
    gs_bucket_name_archive: str = field(default=None, metadata={"optional": True})
    get_key_file_from_sec: str = field(default=None, metadata={"optional": True})
    get_key_file_sec_name: str = field(default=None, metadata={"optional": True})
    def_gs_cred: str = field(default=None, metadata={"optional": True})
    def_gs_project: str = field(default=None, metadata={"optional": True})
    service_account_path: str = field(default=None, metadata={"optional": True})
    google_project_name: str = field(default=None, metadata={"optional": True})
    clean_local_dir: str = field(default=None, metadata={"optional": True})


    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class PGPParams:
    pgp_output_directory: str
    pgp_name_method: str
    secret_name_get_pgp_file_from_sec: str
    temp_keyring:str
    get_pgp_file_from_sec: str = field(default="Y")
    decrypt_inbound_file: str = field(default='Y', metadata={"optional": True})
    private_key_file: str = field(default=None, metadata={"optional": True})
    passphrase: str = field(default=None, metadata={"optional": True})
    pgp_number_of_char_rem: int = field(default=None, metadata={"optional": True})
    pgp_char_to_add: str = field(default=None, metadata={"optional": True})



    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")





def process_sftp_files(file_list, sftp_gs_params: SftpGsParm, pgp_params: PGPParams,
                       db_manager: database_manager.DatabaseManager, sftp_manager: sftp_manager.SFTPManager,sftp_client_subprocess: sftp_client_subprocess.SFTPClientSubprocess):
    # Associate program with PGP manager
    if sftp_gs_params.def_gs_cred=="Y":
        service_account_path=None
    else:
        service_account_path=sftp_gs_params.service_account_path
    if sftp_gs_params.def_gs_project=="Y":
        google_project_name=None
    else:
        google_project_name=sftp_gs_params.google_project_name
    try:
        gsutil=gsutil_manager.GSUtilClient(service_account_path,google_project_name)
        logging.info("gsutil OK!")
    except Exception as err:
        logging.error(f"Error initializing gsutil manager: {err}")
        raise

    try:
        pgp = pgp_manager.PGPManager()
        logging.info("PGP initialization successful!")
    except Exception as err:
        logging.error(f"Error initializing PGP manager: {err}")
        raise
    try:
        local = local_util.LocalUtil()
        logging.info("local initialization successful!")
    except Exception as err:
        logging.error(f"Error initializing local manager: {err}")
        raise


    logging.info(F"sft_gs_params={sftp_gs_params}")
    logging.info(F"pgp_params={pgp_params}")
    if pgp_params.get_pgp_file_from_sec=="Y" and pgp_params.decrypt_inbound_file=="Y":
        pgp.import_private_key_with_passphrase(pgp_params.private_key_file, pgp_params.passphrase,pgp_params.temp_keyring)
    if not sftp_gs_params.sftp_done_directory:
        sftp_gs_params.sftp_done_directory = posixpath.join(sftp_gs_params.sftp_remote_path, "Done/")

    try:
        for item in file_list:
            # Assign values to variables
            filename_full = item['filename_full']
            filename_base = item['filename_base']
            local_temp_file = posixpath.join(sftp_gs_params.local_data_directory, filename_base)

            if sftp_gs_params.push_file_to_gs == "N":
                json_data_successor = {
                    'filename_full': local_temp_file,
                    'filename_base': filename_base
                }

            # Download file from SFTP
            ##ft.download(filename_full,local_temp_file)
            #sftp_download_via_docker(
            #    host=sftp_gs_params.sftp_host,
            #    port=22,
            #    username=sftp_gs_params.sftp_username,
            #    password=sftp_gs_params.sftp_password,
            #    remote_path=filename_full,
            #    local_path=local_temp_file
            #)
            sftp_client_subprocess.download(filename_full,local_temp_file)
            logging.info(f"Downloaded {filename_full} from SFTP as {local_temp_file}.")

            # Decrypt file if required
            if pgp_params.decrypt_inbound_file == "Y":
                output_file = pgp.gpg_name_for_output(
                    local_temp_file,
                    pgp_params.pgp_output_directory,
                    pgp_params.pgp_name_method,
                    pgp_params.pgp_number_of_char_rem,
                    pgp_params.pgp_char_to_add
                )
                pgp.decrypt_file(
                    input_file=local_temp_file,
                    output_file=output_file,
                    passphrase=pgp_params.passphrase,
                    temp_keyring=pgp_params.temp_keyring
                )
                pgp_original_file=local_temp_file
                local_temp_file = output_file


            # Handle file processing and GCS uploading
            if sftp_gs_params.push_file_to_gs == "N":
                json_data_successor = {
                    'filename_full': local_temp_file,
                    'filename_base': filename_base
                }
            elif sftp_gs_params.push_file_to_gs == "Y" and pgp_params.decrypt_inbound_file == "N":
                json_data_successor = {
                    'filename_full': posixpath.join(sftp_gs_params.gs_directory, filename_base),
                    'filename_base': filename_base,
                    'gs_bucket_name': sftp_gs_params.gs_bucket_name
                }
            elif sftp_gs_params.push_file_to_gs == "Y" and pgp_params.decrypt_inbound_file == "Y":
                json_data_successor = {
                    'filename_full': posixpath.join(sftp_gs_params.gs_directory, local_temp_file),
                    'filename_base': posixpath.basename(local_temp_file),
                    'gs_bucket_name': sftp_gs_params.gs_bucket_name
                }

            if sftp_gs_params.push_file_to_gs == "Y":
                gsutil.push_file_to_gs_util(sftp_gs_params.gs_bucket_name, sftp_gs_params.gs_directory, local_temp_file)

            # Execute stored procedures
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id, sftp_gs_params.work_flow_step_log_id,
                input_data.step_name, "Destination", item
            )
            logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id, sftp_gs_params.work_flow_step_log_id,
                input_data.step_name, "Successor", json_data_successor
            )

            # Post-processing
            if sftp_gs_params.has_post_process == "Y":
                sftp_manager.sftp_post_process(sftp_gs_params.post_process_type, filename_full, filename_base, sftp_gs_params.sftp_done_directory)


            if sftp_gs_params.push_file_to_gs == "Y" and sftp_gs_params.archive_pgp_to_gs == "Y":
                gsutil.push_file_to_gs_util(sftp_gs_params.gs_bucket_name, sftp_gs_params.gs_directory, pgp_original_file)

            if sftp_gs_params.clean_local_dir == "Y":
                local.clean_local_directory( sftp_gs_params.local_data_directory)
                local.clean_local_directory( pgp_params.pgp_output_directory)
            if sftp_gs_params.clean_local_dir == "N" and sftp_gs_params.push_file_to_gs == "Y":
                local.clean_local_file(local_temp_file)
                logging.info(f"local_temp_file= {local_temp_file}. Has been deleted")
                if pgp_params.decrypt_inbound_file == "Y":
                    local.clean_local_file(pgp_original_file)
                    logging.info(f"orig_text_file= {pgp_original_file}. Has been deleted")





    except Exception as top_level_error:
        logging.error(f"Top-level error in process_sftp_files: {top_level_error}")
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            sftp_gs_params.work_flow_step_log_id, "failed", f"{top_level_error}"
        )
        raise


def main():
    files = []
    secrets = secret_manager.SecretManager()
    gcs = gcs_manager.GCSManager()
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
    logging.info(f"variables={variables}")
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = (
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

    )
    from_secret_list = json.loads(from_secret_list)
    sftp_gs_params = SftpGsParm(
        work_flow_step_log_id=secrets.get_variable_value( 'WorkFlow_Step_Log_id', variables, from_secret_list),
        sftp_conn_secret=secrets.get_variable_value('sftp_conn_secret', variables, from_secret_list),
        sftp_password=secrets.get_variable_value('sftp_password',  variables, from_secret_list),
        sftp_host=secrets.get_variable_value( 'sftp_host',  variables, from_secret_list),
        sftp_username=secrets.get_variable_value( 'sftp_username',  variables, from_secret_list),
        sftp_remote_path=secrets.get_variable_value( 'sftp_remote_path',  variables, from_secret_list),
        pattern=secrets.get_variable_value( 'pattern',  variables, from_secret_list),
        local_data_directory=secrets.get_variable_value( 'local_data_directory',  variables, from_secret_list),
        post_process_type=secrets.get_variable_value( 'post_process_type',  variables, from_secret_list),
        has_post_process=secrets.get_variable_value( 'has_post_process',  variables, from_secret_list),
        delete_source_record=secrets.get_variable_value('delete_source_record',  variables, from_secret_list),
        sftp_done_directory=secrets.get_variable_value( 'sftp_done_directory',  variables, from_secret_list),
        push_file_to_gs=secrets.get_variable_value( 'push_file_to_gs',  variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory',  variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value( 'gs_bucket_name',  variables, from_secret_list),
        archive_pgp_to_gs=secrets.get_variable_value( 'archive_pgp_to_gs',  variables, from_secret_list),
        gs_bucket_name_archive=secrets.get_variable_value( 'gs_bucket_name_archive',  variables, from_secret_list),
        gs_directory_archive=secrets.get_variable_value( 'gs_directory_archive',  variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value( 'get_key_file_from_sec',  variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value( 'get_key_file_sec_name',  variables, from_secret_list),
        service_account_path=secrets.get_variable_value( 'service_account_path',  variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred',  variables, from_secret_list),
        def_gs_project=secrets.get_variable_value( 'def_gs_project',  variables, from_secret_list),
        google_project_name=secrets.get_variable_value( 'google_project_name',  variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value( 'clean_local_dir',  variables, from_secret_list),
        sftp_port=secrets.get_variable_value('sftp_port',  variables, from_secret_list)

    )

    # Associate program with SFTP manager
    try:
        sftp = sftp_manager.SFTPManager(
            host=sftp_gs_params.sftp_host,
            port= int(sftp_gs_params.sftp_port),
            username=sftp_gs_params.sftp_username,
            password=sftp_gs_params.sftp_password
        )
        logging.info("SFTP connection successful!")
    except Exception as err:
        logging.error(f"Error opening SFTP connection: {err}")
        raise
    try:
        sftp_sp=sftp_client_subprocess.SFTPClientSubprocess(
            host=sftp_gs_params.sftp_host,
            port=sftp_gs_params.sftp_port,
            username=sftp_gs_params.sftp_username,
            password=sftp_gs_params.sftp_password
        )
        logging.info("SFTP connection successful!")
    except Exception as err:
        logging.error(f"Error opening SFTP connection: {err}")
        raise



    pgp_params = PGPParams(
        pgp_output_directory=secrets.get_variable_value( 'pgp_output_directory',  variables, from_secret_list),
        pgp_number_of_char_rem=secrets.get_variable_value( 'pgp_number_of_char_rem', variables, from_secret_list),
        pgp_char_to_add=secrets.get_variable_value( 'pgp_char_to_add', variables, from_secret_list),
        pgp_name_method=secrets.get_variable_value( 'pgp_name_method', variables, from_secret_list),
        decrypt_inbound_file=secrets.get_variable_value('decrypt_inbound_file', variables, from_secret_list),
        private_key_file=secrets.get_variable_value( 'private_key_file', variables, from_secret_list),
        secret_name_get_pgp_file_from_sec=secrets.get_variable_value( 'secret_name_get_pgp_file_from_sec', variables, from_secret_list),
        get_pgp_file_from_sec=secrets.get_variable_value( 'get_pgp_file_from_sec', variables, from_secret_list),
        passphrase=secrets.get_variable_value( 'passphrase', variables, from_secret_list),
        temp_keyring=secrets.get_variable_value('temp_keyring', variables, from_secret_list)
    )

    # Fetch PGP key if conditions are met
    logging.info(f"decrypt_inbound_file BEFORE EVALUATION: {pgp_params.decrypt_inbound_file}")
    logging.info(f"get_pgp_file_from_sec BEFORE EVALUATION: {pgp_params.get_pgp_file_from_sec}")
    if pgp_params.decrypt_inbound_file == "Y" and pgp_params.get_pgp_file_from_sec == "Y":
        try:
            logging.info("Getting PGP key from secret")
            secret_content = secrets.fetch_secret(pgp_params.secret_name_get_pgp_file_from_sec)
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, pgp_params.private_key_file)

        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      sftp_gs_params.work_flow_step_log_id, "failed", f"{e}")
            raise
    if sftp_gs_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                sftp_gs_params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, sftp_gs_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      sftp_gs_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    if sftp_gs_params.pattern == "None":
        pattern = None
    else:
        pattern = sftp_gs_params.pattern

    # Handle source_list_type for listing files
    try:
        files_properties = sftp.fetch_files(sftp_gs_params.sftp_remote_path,pattern)  # Ensure sftp_remote_path exists
        logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
        if not files_properties:
            logging.info("No SFTP files to process. Exiting.")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      sftp_gs_params.work_flow_step_log_id, "success", "no_gs_files_detected")
            sys.exit(0)
        files = db_manager.select_items_to_process_by_list(
            files_properties,
            input_data.step_name,
            input_data.work_flow_log_id,
            sftp_gs_params.work_flow_step_log_id
        )
    except Exception as e:
        logging.error(f"Failed to list files or process items: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  sftp_gs_params.work_flow_step_log_id, "failed", f"{e}")
        sys.exit(1)

    # Process files or exit if none
    if not files:
        logging.info("No NEW files on SFTP to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  sftp_gs_params.work_flow_step_log_id, "success", "no_new_gs_files_detected")
        sys.exit(0)
    try:
        process_sftp_files(files, sftp_gs_params, pgp_params, db_manager,sftp,sftp_sp)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  sftp_gs_params.work_flow_step_log_id, "success", "all_gs_files_loaded_on_sftp")
    except Exception as e:
        logging.error(f"Failed to process GS to SFTP files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  sftp_gs_params.work_flow_step_log_id, "failed", f"{e}")
        sys.exit(1)


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
