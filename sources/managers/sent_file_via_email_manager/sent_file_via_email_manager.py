import sys
import argparse
import subprocess
import posixpath
import logging
import json
import os
from dataclasses import dataclass, field, fields
from core import gcs_manager, pgp_manager, secret_manager,gsutil_manager,local_util,email_manager
from db import database_manager
import time

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
class GcsFileParamsOut:
    work_flow_step_log_id: str
    local_data_directory: str
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    pgp_output_directory: str
    pgp_number_of_char_rem: int
    pgp_name_method: str
    pgp_char_to_add: str
    secret_name_get_pgp_file_from_sec: str
    source_list_type: str
    post_process_type: str
    has_post_process: str
    delete_source_record: str
    google_project_name: str
    gs_directory_done: str
    gs_bucket_name_done: str
    get_key_file_from_sec: str
    get_key_file_sec_name: str
    def_gs_cred: str
    def_gs_project: str
    service_account_path: str
    clean_local_dir: str
    get_pgp_file_from_sec: str
    pgp_outbound_file: str
    public_key_file: str
    recipient: str
    temp_keyring: str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class EmailParams:
    smtp_conn_secret: str
    smtp_server: str
    smtp_port: str
    sender_email: str
    sender_password: str
    use_tls: str
    EMAIL_TO: str
    is_html:str
    body: str
    subject:str
    from_email:str

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def str_to_bool(value: str) -> bool:
    val = value.strip().lower()
    if val == "true":
        return True
    elif val == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean string: '{value}'")

def sent_file_via_emai(file_list, Gcs_File_Params: GcsFileParamsOut, email_params: EmailParams,
                           db_manager: database_manager.DatabaseManager):
    import time  # ensure available even if not imported at module level
    try:
        filename_full = None
        logging.info(f"Files NOT PROCESSED FILES from inside of function= {json.dumps(file_list)}")

        # Resolve GCS auth
        service_account_path = None if Gcs_File_Params.def_gs_cred == "Y" else Gcs_File_Params.service_account_path
        google_project_name = None if Gcs_File_Params.def_gs_project == "Y" else Gcs_File_Params.google_project_name
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise
        # Managers

        try:
            sender = email_manager.EmailSender(
                smtp_server=email_params.smtp_server,
                smtp_port=int(email_params.smtp_port),
                login_email=email_params.sender_email,
                login_password=email_params.sender_password,  # App password (not your Gmail password!)
                use_tls=str_to_bool(email_params.use_tls)
            )
            logging.info("smtp  connection successful!")
        except Exception as err:
            logging.error(f"Error opening smpt connection: {err}")
            raise



        # PGP manager
        try:
            pgp = pgp_manager.PGPManager()
            logging.info("pgp init successful!")
        except Exception as err:
            logging.error(f"Error initializing PGP manager: {err}")
            raise

        # Local util
        try:
            local = local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise

        # Import public key if needed
        if Gcs_File_Params.pgp_outbound_file == "Y":
            pgp.import_public_key_file(Gcs_File_Params.public_key_file, Gcs_File_Params.temp_keyring)

        # Process each file
        for item in file_list:
            logging.info(f"Start Processing Item= {json.dumps(item)}")
            try:
                filename_full = item['filename_full']
                filename_base = item['filename_base']
                gs_bucket_name = item['gs_bucket_name']
                file_row_count = item['file_row_count']

                body_adjusted = email_params.body
                body_adjusted = body_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
                body_adjusted = body_adjusted.replace("|||WorkFlow_Step_Log_id|||",
                                                        Gcs_File_Params.work_flow_step_log_id)
                body_adjusted = body_adjusted.replace("|||File_name|||",
                                                      filename_base)
                body_adjusted = body_adjusted.replace("|||File_row_count|||",
                                                      file_row_count)
                subject_adjusted = email_params.subject
                subject_adjusted = subject_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
                subject_adjusted = subject_adjusted.replace("|||WorkFlow_Step_Log_id|||",
                                                            Gcs_File_Params.work_flow_step_log_id)
                subject_adjusted = subject_adjusted.replace("|||File_name|||",
                                                            filename_base)
                subject_adjusted = subject_adjusted.replace("|||File_row_count|||",
                                                            file_row_count)
                local_temp_file = posixpath.join(Gcs_File_Params.local_data_directory, filename_base)
                gsutil.get_file_from_gs_util(gs_bucket_name, filename_full, local_temp_file, filename_base)

                orig_text_file = ""

                # Optional PGP
                if Gcs_File_Params.pgp_outbound_file == "Y":
                    output_file = pgp.gpg_name_for_output(
                        local_temp_file,
                        Gcs_File_Params.pgp_output_directory,
                        Gcs_File_Params.pgp_name_method,
                        Gcs_File_Params.pgp_number_of_char_rem,
                        Gcs_File_Params.pgp_char_to_add
                    )
                    logging.info(f"output_file={output_file}")
                    pgp.gpg_encrypt_file(
                        input_file=local_temp_file,
                        output_file=output_file,
                        recipient=Gcs_File_Params.recipient,
                        temp_keyring=Gcs_File_Params.temp_keyring,
                        public_key_file=Gcs_File_Params.public_key_file
                    )
                    orig_text_file = local_temp_file
                    local_temp_file = output_file



                # email
                try:
                    sender.send_email(recipient_email=email_params.EMAIL_TO, subject=subject_adjusted,body=body_adjusted,attachments=local_temp_file,from_email=email_params.from_email)
                    logging.info(f"Sent file  {local_temp_file} to {email_params.EMAIL_TO}")
                except Exception as upload_err:
                    logging.error(f"Fail to sent  {local_temp_file}  to {Gcs_File_Params.recipient} via email error: {upload_err}")
                    raise



                # Log dataset instance (destination)
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    Gcs_File_Params.work_flow_step_log_id,
                    input_data.step_name,
                    "Destination",
                    item
                )


                # Post-process in GCS if needed
                if Gcs_File_Params.has_post_process == "Y":
                    gsutil.gs_post_process(
                        post_process_type=Gcs_File_Params.post_process_type,
                        gs_bucket_name=Gcs_File_Params.gs_bucket_name,
                        filename_full=filename_full,
                        gs_bucket_name_done=Gcs_File_Params.gs_bucket_name_done,
                        gs_done_directory=Gcs_File_Params.gs_directory_done
                    )
                    logging.info(f"Post-processing completed for file {filename_full}.")

                # Cleanup
                if Gcs_File_Params.clean_local_dir == "Y":
                    local.clean_local_directory(Gcs_File_Params.local_data_directory)
                    local.clean_local_directory(Gcs_File_Params.pgp_output_directory)
                else:
                    local.clean_local_file(local_temp_file)
                    logging.info(f"local_temp_file= {local_temp_file}. Has been deleted")
                    if Gcs_File_Params.pgp_outbound_file == "Y" and orig_text_file:
                        local.clean_local_file(orig_text_file)
                        logging.info(f"orig_text_file= {orig_text_file}. Has been deleted")

            except Exception as e:
                logging.error(f"Error processing file {filename_full}: {e}")
                raise  # Stop processing on any error

    except Exception as top_level_error:
        logging.error(f"Top-level error in process_gs_to_s3_files: {top_level_error}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id,
            "failed",
            f"{top_level_error}"
        )
        raise



def main():
    files = []
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

    Gcs_File_Params = GcsFileParamsOut(
        gs_bucket_name=secrets.get_variable_value( 'gs_bucket_name',  variables, from_secret_list),
        gs_directory=secrets.get_variable_value( 'gs_directory',  variables, from_secret_list),
        service_account_path=secrets.get_variable_value(  'service_account_path',  variables, from_secret_list),
        google_project_name=secrets.get_variable_value(  'google_project_name',  variables, from_secret_list),
        work_flow_step_log_id=secrets.get_variable_value(  'WorkFlow_Step_Log_id',  variables, from_secret_list),
        local_data_directory=secrets.get_variable_value( 'local_data_directory',  variables, from_secret_list),
        post_process_type=secrets.get_variable_value(  'post_process_type',  variables, from_secret_list),
        has_post_process=secrets.get_variable_value(  'has_post_process',  variables, from_secret_list),
        delete_source_record=secrets.get_variable_value(  'delete_source_record',  variables, from_secret_list),
        source_list_type=secrets.get_variable_value( 'source_list_type',  variables, from_secret_list),
        gs_directory_done=secrets.get_variable_value( 'gs_directory_done',  variables, from_secret_list),
        gs_bucket_name_done=secrets.get_variable_value(  'gs_bucket_name_done',  variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value(  'get_key_file_from_sec',  variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value(  'get_key_file_sec_name',  variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value( 'def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value( 'def_gs_project',  variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value( 'clean_local_dir',  variables, from_secret_list),
        pgp_output_directory=secrets.get_variable_value(  'pgp_output_directory',  variables, from_secret_list),
        pgp_number_of_char_rem=secrets.get_variable_value( 'pgp_number_of_char_rem',  variables, from_secret_list),
        pgp_name_method=secrets.get_variable_value(  'pgp_name_method',  variables, from_secret_list),
        pgp_char_to_add=secrets.get_variable_value(  'pgp_char_to_add',  variables, from_secret_list),
        pgp_outbound_file=secrets.get_variable_value( 'pgp_outbound_file',  variables, from_secret_list),
        public_key_file=secrets.get_variable_value( 'public_key_file',  variables, from_secret_list),
        recipient=secrets.get_variable_value( 'recipient',  variables, from_secret_list),
        secret_name_get_pgp_file_from_sec=
        secrets.get_variable_value('secret_name_get_pgp_file_from_sec',  variables, from_secret_list),
        get_pgp_file_from_sec=secrets.get_variable_value(  'get_pgp_file_from_sec', variables, from_secret_list),
        temp_keyring=secrets.get_variable_value(  'temp_keyring',  variables, from_secret_list)
    )

    email_params = EmailParams(
        smtp_conn_secret=secrets.get_variable_value('smtp_conn_secret', variables, from_secret_list),
        smtp_server=secrets.get_variable_value('smtp_server', variables, from_secret_list),
        smtp_port=secrets.get_variable_value('smtp_port', variables, from_secret_list),
        sender_email=secrets.get_variable_value('sender_email', variables, from_secret_list),
        sender_password=secrets.get_variable_value('sender_password', variables, from_secret_list),
        use_tls=secrets.get_variable_value('use_tls', variables, from_secret_list),
        EMAIL_TO=secrets.get_variable_value('EMAIL_TO', variables, from_secret_list),
        is_html=secrets.get_variable_value('is_html', variables, from_secret_list),
        subject=secrets.get_variable_value('subject', variables, from_secret_list),
        body=secrets.get_variable_value('body', variables, from_secret_list),
        from_email=secrets.get_variable_value('from_email', variables, from_secret_list)

    )


    logging.info(f"pgp_outbound_file BEFORE EVALUATION: {Gcs_File_Params.pgp_outbound_file}")
    logging.info(f"get_pgp_file_from_sec BEFORE EVALUATION: {Gcs_File_Params.get_pgp_file_from_sec}")

    if Gcs_File_Params.def_gs_cred=="Y":
        service_account_path=None
    else:
        service_account_path=Gcs_File_Params.service_account_path
    if Gcs_File_Params.def_gs_project=="Y":
        google_project_name=None
    else:
        google_project_name=Gcs_File_Params.google_project_name
    try:
        gcs = gcs_manager.GCSManager(service_account_path,google_project_name)
        logging.info("gcs OK!")
    except Exception as err:
        logging.error(f"Error initializing gcs manager: {err}")
        raise

    if Gcs_File_Params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting  key from secret")
            secret_content = secrets.fetch_secret(
                Gcs_File_Params.get_key_file_sec_name
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, Gcs_File_Params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)

    if Gcs_File_Params.pgp_outbound_file == "Y" and Gcs_File_Params.get_pgp_file_from_sec == "Y":
        try:
            logging.info("Getting PGP key from secret")
            secret_content = secrets.fetch_secret(
               Gcs_File_Params.secret_name_get_pgp_file_from_sec
            )
            logging.info(f"Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, Gcs_File_Params.public_key_file)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            raise

    # Handle source_list_type for listing files
    if Gcs_File_Params.source_list_type == "LIST":
        try:
            files_properties = gcs.list_files_with_properties(Gcs_File_Params.gs_bucket_name, Gcs_File_Params.gs_directory)
            logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
            if not files_properties:
                logging.info("No gs files to process. Exiting.")
                db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                         Gcs_File_Params.work_flow_step_log_id, "success",
                                          f"no_gs_files_detected")
                sys.exit(0)
            files = db_manager.select_items_to_process_by_list(files_properties, input_data.step_name,
                                                               input_data.work_flow_log_id,
                                                               Gcs_File_Params.work_flow_step_log_id)
        except Exception as e:
            logging.error(f"Failed to list files or process items: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                      Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    elif Gcs_File_Params.source_list_type == "RECORD":
        try:
            files = db_manager.select_items_to_process_by_records(input_data.work_flow_log_id,
                                                                  Gcs_File_Params.work_flow_step_log_id,
                                                                  input_data.step_name)
        except Exception as e:
            logging.error(f"Failed to select items by records: {e}")
            db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                     Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}")
            sys.exit(1)
    # Process files or exit if none
    if not files:
        logging.info("No NEW files on gs to process. Exiting.")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "success",
                                  f"no_new_gs_files_detected")
        sys.exit(0)
    try:
        sent_file_via_emai(files, Gcs_File_Params,email_params, db_manager)
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "success",
                                  f"all_gs_files_loaded_on_s3")
    except Exception as e:
        logging.error(f"Failed to process GS to SFTP files: {e}")
        db_manager.close_step_log(input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                                  Gcs_File_Params.work_flow_step_log_id, "failed",
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
