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
class GcParams:
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    google_project_name: str = field(default=None)

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class GcsSftpParams:
    work_flow_step_log_id: str
    sftp_host: str
    sftp_username: str
    sftp_password: str
    sftp_remote_path: str
    local_data_directory: str
    sftp_conn_secret:str
    source_list_type: str = field(default="RECORD")
    post_process_type: str = field(default='DELETE')
    has_post_process: str = field(default='N')
    delete_source_record: str = field(default='Y', metadata={"optional": True})
    sftp_port: int = field(default=22, metadata={"optional": True})
    gs_directory_done: str = field(default=None, metadata={"optional": True})
    gs_bucket_name_done: str = field(default=None, metadata={"optional": True})
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
    pgp_number_of_char_rem: int
    pgp_name_method: str
    pgp_char_to_add: str
    secret_name_get_pgp_file_from_sec: str
    temp_keyring: str
    get_pgp_file_from_sec: str = field(default="Y")
    pgp_outbound_file: str = field(default='N', metadata={"optional": True})
    public_key_file: str = field(default=None, metadata={"optional": True})
    recipient: str = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def process_gs_to_sftp_files(file_list, gcs_sftp_params: GcsSftpParams, pgp_params: PGPParams,
                             db_manager: database_manager.DatabaseManager, gc_params: GcParams):
    try:
        filename_full = None
        logging.info(f"Files NOT PROCESSED FILES from inside of function= {json.dumps(file_list)}")
        if gcs_sftp_params.def_gs_cred == "Y":
            service_account_path = None
        else:
            service_account_path = gcs_sftp_params.service_account_path
        if gcs_sftp_params.def_gs_project == "Y":
            google_project_name = None
        else:
            google_project_name = gcs_sftp_params.google_project_name
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise
        try:
            sftp_sp = sftp_client_subprocess.SFTPClientSubprocess(
                host=gcs_sftp_params.sftp_host,
                port=gcs_sftp_params .sftp_port,
                username=gcs_sftp_params.sftp_username,
                password=gcs_sftp_params.sftp_password
            )
            logging.info("SFTP connection successful!")
        except Exception as err:
            logging.error(f"Error opening SFTP connection: {err}")
            raise

        # Associate program with sftp manager
        try:
            sftp = sftp_manager.SFTPManager(
                host=gcs_sftp_params.sftp_host,
                port=gcs_sftp_params.sftp_port,
                username=gcs_sftp_params.sftp_username,
                password=gcs_sftp_params.sftp_password
            )
            logging.info("SFTP connection successful!")
        except Exception as err:
            logging.error(f"Error opening SFTP connection: {err}")
            raise
        try:
            gcs = gcs_manager.GCSManager()
        except Exception as err:
            logging.error(f"Error opening gcs_manager: {err}")
            raise
        # Associate program with pgp manager
        try:
            pgp = pgp_manager.PGPManager()
            logging.info("pgp init successful!")
        except Exception as err:
            logging.error(f"Error opening SFTP connection: {err}")
            raise
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise
        if pgp_params.pgp_outbound_file == "Y":
            pgp.import_public_key_file(pgp_params.public_key_file,pgp_params.temp_keyring)
        for item in file_list:
            logging.info(f"Start Processing Item= {json.dumps(item)}")
            try:
                # Process each file
                filename_full = item['filename_full']
                filename_base = item['filename_base']
                gs_bucket_name = item['gs_bucket_name']
                local_temp_file = posixpath.join(gcs_sftp_params.local_data_directory, filename_base)
                gsutil.get_file_from_gs_util(gs_bucket_name,filename_full,local_temp_file,filename_base)

                # Handle PGP encryption if enabled
                orig_text_file=""
                if pgp_params.pgp_outbound_file == "Y":
                    output_file = pgp.gpg_name_for_output(
                        local_temp_file,
                        pgp_params.pgp_output_directory,
                        pgp_params.pgp_name_method,
                        pgp_params.pgp_number_of_char_rem,
                        pgp_params.pgp_char_to_add
                    )
                    logging.info(f"output_file={output_file}")
                    pgp.gpg_encrypt_file(
                        input_file=local_temp_file,
                        output_file=output_file,
                        recipient=pgp_params.recipient,
                        public_key_file=pgp_params.public_key_file,
                        temp_keyring=pgp_params.temp_keyring
                    )
                    orig_text_file = local_temp_file
                    local_temp_file = output_file

                # Upload file to SFTP
                sftp_local_file_name = local_temp_file
                sftp_remote_file_name = posixpath.join(gcs_sftp_params.sftp_remote_path,
                                                       posixpath.basename(local_temp_file))
                ##sftp.upload_file(sftp_local_file_name, sftp_remote_file_name)
                sftp_sp.upload(sftp_local_file_name, sftp_remote_file_name)
                logging.info(f"Uploaded {sftp_local_file_name} to SFTP as {sftp_remote_file_name}.")

                # Execute stored procedures
                db_manager.create_dataset_instance(input_data.work_flow_log_id, gcs_sftp_params.work_flow_step_log_id,
                                                   input_data.step_name, "Destination", item)
                filename_base_successor = os.path.basename(sftp_remote_file_name)

                # Extract the directory path
                sftp_remote_path_successor = os.path.dirname(sftp_remote_file_name) + "/"  # Adding trailing slash
                json_data_successor = {
                    'filename_full': sftp_remote_file_name,
                    'filename_base': filename_base_successor,
                    'sftp_remote_path': sftp_remote_path_successor
                }
                logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
                db_manager.create_dataset_instance(input_data.work_flow_log_id, gcs_sftp_params.work_flow_step_log_id,
                                                   input_data.step_name, "Successor", json_data_successor)

                # Handle post-processing
                if gcs_sftp_params.has_post_process == "Y":
                    gsutil.gs_post_process(
                        post_process_type=gcs_sftp_params.post_process_type,
                        gs_bucket_name=gc_params.gs_bucket_name,
                        filename_full=filename_full,
                        gs_bucket_name_done=gcs_sftp_params.gs_bucket_name_done,
                        gs_done_directory=gcs_sftp_params.gs_directory_done
                    )
                    logging.info(f"Post-processing completed for file {filename_full}.")


                if gcs_sftp_params.clean_local_dir == "Y":
                    local.clean_local_directory(gcs_sftp_params.local_data_directory)
                    local.clean_local_directory(pgp_params.pgp_output_directory)
                else:
                    local.clean_local_file(local_temp_file)
                    logging.info(f"local_temp_file= {local_temp_file}. Has been deleted")
                    if pgp_params.pgp_outbound_file=="Y":
                        local.clean_local_file(orig_text_file)
                        logging.info(f"orig_text_file= {orig_text_file}. Has been deleted")

            except Exception as e:
                logging.error(f"Error processing file {filename_full}: {e}")
                raise  # Stop processing on any error

    except Exception as top_level_error:
        logging.error(f"Top-level error in process_gs_to_sftp_files: {top_level_error}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gcs_sftp_params.work_flow_step_log_id, "failed",
                                  f"{top_level_error}")
        raise


def main():
    files = []
    secrets = secret_manager.SecretManager()
    meta_connection=secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)


    db_manager = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database
    )

    variables = db_manager.start_workflow_step_log(input_data.workflow_name, input_data.step_name,
                                                   input_data.work_flow_log_id, input_data.additional_param)
    logging.info(f"variables={variables}")
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = (
        variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]

    )
    from_secret_list = json.loads(from_secret_list)
    #from_secret_list=variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
    gc_params = GcParams(
        gs_bucket_name=secrets.get_variable_value( 'gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory',variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list)
    )
    logging.info(f"gc_params={gc_params}")
    gcs_sftp_params = GcsSftpParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        sftp_conn_secret=secrets.get_variable_value('sftp_conn_secret',  variables, from_secret_list),
        sftp_host=secrets.get_variable_value('sftp_host', variables, from_secret_list),
        sftp_username=secrets.get_variable_value('sftp_username', variables, from_secret_list),
        sftp_port=secrets.get_variable_value('sftp_port', variables, from_secret_list),
        sftp_password=secrets.get_variable_value('sftp_password', variables, from_secret_list),
        sftp_remote_path=secrets.get_variable_value('sftp_remote_path', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        post_process_type=secrets.get_variable_value('post_process_type', variables, from_secret_list),
        has_post_process=secrets.get_variable_value('has_post_process', variables, from_secret_list),
        delete_source_record=secrets.get_variable_value('delete_source_record', variables, from_secret_list),
        source_list_type=secrets.get_variable_value('source_list_type', variables, from_secret_list),
        gs_directory_done=secrets.get_variable_value('gs_directory_done', variables, from_secret_list),
        gs_bucket_name_done=secrets.get_variable_value('gs_bucket_name_done', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list)
    )

    logging.info(f"gcs_sftp_params={gcs_sftp_params}")
    pgp_params = PGPParams(
        pgp_output_directory=secrets.get_variable_value('pgp_output_directory',variables, from_secret_list),
        pgp_number_of_char_rem=secrets.get_variable_value('pgp_number_of_char_rem',variables, from_secret_list),
        pgp_name_method=secrets.get_variable_value('pgp_name_method',variables, from_secret_list),
        pgp_char_to_add=secrets.get_variable_value( 'pgp_char_to_add',variables, from_secret_list),
        pgp_outbound_file=secrets.get_variable_value('pgp_outbound_file', variables, from_secret_list),
        public_key_file=secrets.get_variable_value('public_key_file', variables, from_secret_list),
        recipient=secrets.get_variable_value('recipient', variables, from_secret_list),
        secret_name_get_pgp_file_from_sec=
        secrets.get_variable_value( 'secret_name_get_pgp_file_from_sec', variables, from_secret_list),
        get_pgp_file_from_sec=secrets.get_variable_value('get_pgp_file_from_sec',  variables, from_secret_list),
        temp_keyring=secrets.get_variable_value( 'temp_keyring', variables, from_secret_list)

    )
    logging.info(f"pgp_params={pgp_params}")
    # Fetch PGP key if conditions are met
    logging.info(f"pgp_outbound_file BEFORE EVALUATION: {pgp_params.pgp_outbound_file}")
    logging.info(f"get_pgp_file_from_sec BEFORE EVALUATION: {pgp_params.get_pgp_file_from_sec}")



    if gcs_sftp_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                gcs_sftp_params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gcs_sftp_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gcs_sftp_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    if gcs_sftp_params.def_gs_cred == "Y":
        service_account_path = None
    else:
        service_account_path = gcs_sftp_params.service_account_path
    if gcs_sftp_params.def_gs_project == "Y":
        google_project_name = None
    else:
        google_project_name = gcs_sftp_params.google_project_name
    try:
        gcs = gcs_manager.GCSManager(service_account_path, google_project_name)
        logging.info("gcs OK!")
    except Exception as err:
        logging.error(f"Error initializing gcs manager: {err}")
        raise

    if pgp_params.pgp_outbound_file == "Y" and pgp_params.get_pgp_file_from_sec == "Y":
        try:
            logging.info("Getting PGP key from secret")
            secret_content = secrets.fetch_secret(
                pgp_params.secret_name_get_pgp_file_from_sec
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, pgp_params.public_key_file)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gcs_sftp_params.work_flow_step_log_id, "failed", f"{e}")
            raise

    # Handle source_list_type for listing files
    if gcs_sftp_params.source_list_type == "LIST":
        try:
            files_properties = gcs.list_files_with_properties(gc_params.gs_bucket_name, gc_params.gs_directory)
            logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
            if not files_properties:
                logging.info("No gs files to process. Exiting.")
                db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                          gcs_sftp_params.work_flow_step_log_id, "success",
                                          f"no_gs_files_detected")
                sys.exit(0)
            files = db_manager.select_items_to_process_by_list(files_properties, input_data.step_name,
                                                               input_data.work_flow_log_id,
                                                               gcs_sftp_params.work_flow_step_log_id)
        except Exception as e:
            logging.error(f"Failed to list files or process items: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gcs_sftp_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    elif gcs_sftp_params.source_list_type == "RECORD":
        try:
            files = db_manager.select_items_to_process_by_records(input_data.work_flow_log_id,
                                                                  gcs_sftp_params.work_flow_step_log_id,
                                                                  input_data.step_name)
        except Exception as e:
            logging.error(f"Failed to select items by records: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      gcs_sftp_params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    # Process files or exit if none
    if not files:
        logging.info("No NEW files on gs to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gcs_sftp_params.work_flow_step_log_id, "success",
                                  f"no_new_gs_files_detected")
        sys.exit(0)
    try:
        process_gs_to_sftp_files(files, gcs_sftp_params, pgp_params, db_manager,gc_params)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gcs_sftp_params.work_flow_step_log_id, "success",
                                  f"all_gs_files_loaded_on_sftp")
    except Exception as e:
        logging.error(f"Failed to process GS to SFTP files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  gcs_sftp_params.work_flow_step_log_id, "failed",
                                  f"{e}")
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
