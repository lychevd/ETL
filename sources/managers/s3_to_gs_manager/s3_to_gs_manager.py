import sys
import argparse
import subprocess
import posixpath
import logging
import json
import os
from dataclasses import dataclass, field, fields
from core import gcs_manager, pgp_manager, secret_manager, gsutil_manager, local_util, s3_manager
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
    google_project_name: str
    get_key_file_from_sec: str
    get_key_file_sec_name: str
    def_gs_cred: str
    def_gs_project: str
    clean_local_dir: str
    push_file_to_gs: str
    archive_pgp_to_gs: str = field(default="N", metadata={"optional": True})
    gs_directory_archive: str = field(default=None, metadata={"optional": True})
    gs_bucket_name_archive: str = field(default=None, metadata={"optional": True})  # NEW

    def __post_init__(self):
        missing_fields = [
            field.name for field in fields(self)
            if getattr(self, field.name) is None and not field.metadata.get("optional", False)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


@dataclass
class S3Params:
    s3_conn_secret: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region_name: str
    aws_session_token: str
    aws_role_arn: str
    aws_role_session_name: str
    aws_session_duration: str
    aws_external_id: str
    s3_bucket_name: str
    s3_directory: str
    recursive: str

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
    temp_keyring: str
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


def _none_if_empty(val):
    """Return None if val is None, 'None', or '', else val."""
    return None if val in (None, "None", "") else val


def normalize_s3_params(S3_Params) -> dict:
    """
    Normalize S3 auth params from your S3_Params dataclass/object into a dict
    suitable for s3_manager.S3Client(**dict).
    """
    aws_region_name       = _none_if_empty(S3_Params.aws_region_name)
    aws_session_token     = _none_if_empty(S3_Params.aws_session_token)
    aws_role_arn          = _none_if_empty(S3_Params.aws_role_arn)
    aws_role_session_name = _none_if_empty(S3_Params.aws_role_session_name)
    aws_external_id       = _none_if_empty(S3_Params.aws_external_id)

    # duration: to int or None
    raw_dur = _none_if_empty(S3_Params.aws_session_duration)
    if raw_dur is not None:
        try:
            aws_session_duration = int(raw_dur)
        except (TypeError, ValueError):
            aws_session_duration = None
    else:
        aws_session_duration = None

    return {
        "aws_access_key_id":     _none_if_empty(S3_Params.aws_access_key_id),
        "aws_secret_access_key": _none_if_empty(S3_Params.aws_secret_access_key),
        "aws_region_name":       aws_region_name,
        "aws_session_token":     aws_session_token,
        "aws_role_arn":          aws_role_arn,
        "aws_role_session_name": aws_role_session_name,
        "aws_session_duration":  aws_session_duration,
        "aws_external_id":       aws_external_id,
    }


def str_to_bool_strict(s: str) -> bool:
    """
    Accepts only: 'true', 'True', 'Y', 'y', 'False', 'false', 'N', 'n'.
    Raises ValueError for anything else.
    """
    if s in ("true", "True", "Y", "y"):
        return True
    if s in ("False", "false", "N", "n"):
        return False
    raise ValueError(
        f"Invalid boolean string: {s!r}. Expected one of: "
        "true, True, Y, y, False, false, N, n"
    )


def create_s3_client_from_params(S3_Params):
    """
    Convenience wrapper: normalize, then construct the client.
    """
    try:
        auth = normalize_s3_params(S3_Params)
        s3 = s3_manager.S3Client(**auth)
        logging.info("S3 connection successful!")
        return s3
    except Exception as err:
        logging.error(f"Error opening S3 connection: {err}")
        raise


def load_s3_files_to_gs(
    file_list,
    Gcs_File_Params: GcsFileParamsOut,
    S3_Params: S3Params,
    pgp_params: PGPParams,
    db_manager: database_manager.DatabaseManager,
    s3: s3_manager.S3Client
):
    try:
        logging.info(f"Files NOT PROCESSED FILES from inside of function= {json.dumps(file_list)}")

        # Resolve GCS auth
        service_account_path = None if Gcs_File_Params.def_gs_cred == "Y" else Gcs_File_Params.service_account_path
        google_project_name = None if Gcs_File_Params.def_gs_project == "Y" else Gcs_File_Params.google_project_name

        # Managers
        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            raise

        try:
            gcs = gcs_manager.GCSManager(service_account_path, google_project_name)
        except Exception as err:
            logging.error(f"Error opening gcs_manager: {err}")
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

        # Import private key if we will decrypt
        if pgp_params.get_pgp_file_from_sec == "Y" and pgp_params.decrypt_inbound_file == "Y":
            pgp.import_private_key_with_passphrase(
                pgp_params.private_key_file,
                pgp_params.passphrase,
                pgp_params.temp_keyring
            )

        # Process each file
        for item in file_list:
            logging.info(f"Start Processing Item= {json.dumps(item)}")
            try:
                s3_uri = item['s3_uri']
                filename_base = item['filename_base']
                filename_full = item['filename_full']

                # Download from S3
                local_temp_file = s3.download_file_from_s3(s3_uri=s3_uri)

                # Optional decrypt
                pgp_original_file = None
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
                    pgp_original_file = local_temp_file        # keep the original (pre-decrypt)
                    local_temp_file = output_file               # now work with decrypted

                # Prepare successor json and optionally push to GCS
                if Gcs_File_Params.push_file_to_gs == "N":
                    json_data_successor = {
                        'filename_full': local_temp_file,
                        'filename_base': os.path.basename(local_temp_file)
                    }
                elif Gcs_File_Params.push_file_to_gs == "Y" and pgp_params.decrypt_inbound_file == "N":
                    json_data_successor = {
                        'filename_full': posixpath.join(Gcs_File_Params.gs_directory, filename_base),
                        'filename_base': filename_base,
                        'gs_bucket_name': Gcs_File_Params.gs_bucket_name
                    }
                else:  # push_file_to_gs == "Y" and decrypted == "Y"
                    json_data_successor = {
                        'filename_full': posixpath.join(Gcs_File_Params.gs_directory, os.path.basename(local_temp_file)),
                        'filename_base': os.path.basename(local_temp_file),
                        'gs_bucket_name': Gcs_File_Params.gs_bucket_name
                    }

                # Push decrypted/plain (or original if not decrypted) to GCS
                if Gcs_File_Params.push_file_to_gs == "Y":
                    gsutil.push_file_to_gs_util(
                        Gcs_File_Params.gs_bucket_name,
                        Gcs_File_Params.gs_directory,
                        local_temp_file
                    )

                # OPTIONAL ARCHIVE of original pre-decrypt file
                if pgp_original_file and Gcs_File_Params.archive_pgp_to_gs == "Y":
                    archive_dir = (
                        Gcs_File_Params.gs_directory_archive
                        if _none_if_empty(Gcs_File_Params.gs_directory_archive)
                        else posixpath.join(Gcs_File_Params.gs_directory, "archive")
                    )
                    archive_bucket = (
                        Gcs_File_Params.gs_bucket_name_archive
                        if _none_if_empty(Gcs_File_Params.gs_bucket_name_archive)
                        else Gcs_File_Params.gs_bucket_name
                    )
                    logging.info(
                        f"Archiving original file to gs://{archive_bucket}/{archive_dir} -> {pgp_original_file}"
                    )
                    gsutil.push_file_to_gs_util(
                        archive_bucket,
                        archive_dir,
                        pgp_original_file
                    )

                # Log dataset instance (destination)
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    Gcs_File_Params.work_flow_step_log_id,
                    input_data.step_name,
                    "Destination",
                    item
                )

                logging.info(f"Successor JSON= {json.dumps(json_data_successor)}")
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id,
                    Gcs_File_Params.work_flow_step_log_id,
                    input_data.step_name,
                    "Successor",
                    json_data_successor
                )

                # Cleanup
                if Gcs_File_Params.clean_local_dir == "Y":
                    local.clean_local_directory(Gcs_File_Params.local_data_directory)
                    if pgp_params.decrypt_inbound_file == "Y":
                        local.clean_local_directory(pgp_params.pgp_output_directory)
                else:
                    # Remove delivered/processed file if we pushed it
                    if Gcs_File_Params.push_file_to_gs == "Y":
                        local.clean_local_file(local_temp_file)
                        logging.info(f"local_temp_file= {local_temp_file}. Has been deleted")
                    # Remove original if we either archived it or pushed (to avoid leaving secrets)
                    if pgp_original_file and (
                        Gcs_File_Params.archive_pgp_to_gs == "Y" or Gcs_File_Params.push_file_to_gs == "Y"
                    ):
                        local.clean_local_file(pgp_original_file)
                        logging.info(f"orig_text_file= {pgp_original_file}. Has been deleted")

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

    variables = db_manager.start_workflow_step_log(
        input_data.workflow_name, input_data.step_name,
        input_data.work_flow_log_id, input_data.additional_param
    )
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
    from_secret_list = json.loads(from_secret_list)

    Gcs_File_Params = GcsFileParamsOut(
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
        def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
        clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list),
        push_file_to_gs=secrets.get_variable_value('push_file_to_gs', variables, from_secret_list),
        archive_pgp_to_gs=secrets.get_variable_value('archive_pgp_to_gs', variables, from_secret_list),
        gs_directory_archive=secrets.get_variable_value('gs_directory_archive', variables, from_secret_list),
        gs_bucket_name_archive=secrets.get_variable_value('gs_bucket_name_archive', variables, from_secret_list),
    )

    logging.info(f"Gcs_File_Params={Gcs_File_Params}")
    S3_Params = S3Params(
        s3_conn_secret=secrets.get_variable_value('s3_conn_secret', variables, from_secret_list),
        aws_access_key_id=secrets.get_variable_value('aws_access_key_id', variables, from_secret_list),
        aws_secret_access_key=secrets.get_variable_value('aws_secret_access_key', variables, from_secret_list),
        s3_bucket_name=secrets.get_variable_value('s3_bucket_name', variables, from_secret_list),
        aws_region_name=secrets.get_variable_value('aws_region_name', variables, from_secret_list),
        s3_directory=secrets.get_variable_value('s3_directory', variables, from_secret_list),
        aws_session_token=secrets.get_variable_value('aws_session_token', variables, from_secret_list),
        aws_role_arn=secrets.get_variable_value('aws_role_arn', variables, from_secret_list),
        aws_role_session_name=secrets.get_variable_value('aws_role_session_name', variables, from_secret_list),
        aws_session_duration=secrets.get_variable_value('aws_session_duration', variables, from_secret_list),
        aws_external_id=secrets.get_variable_value('aws_external_id', variables, from_secret_list),
        recursive=secrets.get_variable_value('recursive', variables, from_secret_list),
    )
    logging.info(f"S3_Params={S3_Params}")

    # Build PGP params
    pgp_params = PGPParams(
        pgp_output_directory=secrets.get_variable_value('pgp_output_directory', variables, from_secret_list),
        pgp_name_method=secrets.get_variable_value('pgp_name_method', variables, from_secret_list),
        secret_name_get_pgp_file_from_sec=secrets.get_variable_value('secret_name_get_pgp_file_from_sec', variables, from_secret_list),
        temp_keyring=secrets.get_variable_value('temp_keyring', variables, from_secret_list),
        get_pgp_file_from_sec=secrets.get_variable_value('get_pgp_file_from_sec', variables, from_secret_list),
        decrypt_inbound_file=secrets.get_variable_value('decrypt_inbound_file', variables, from_secret_list),
        private_key_file=secrets.get_variable_value('private_key_file', variables, from_secret_list),
        passphrase=secrets.get_variable_value('passphrase', variables, from_secret_list),
        pgp_number_of_char_rem=secrets.get_variable_value('pgp_number_of_char_rem', variables, from_secret_list),
        pgp_char_to_add=secrets.get_variable_value('pgp_char_to_add', variables, from_secret_list)
    )

    s3 = create_s3_client_from_params(S3_Params)
    recursive = str_to_bool_strict(S3_Params.recursive)

    if Gcs_File_Params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting key from secret")
            secret_content = secrets.fetch_secret(Gcs_File_Params.get_key_file_sec_name)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, Gcs_File_Params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_manager.close_step_log(
                input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}"
            )
            sys.exit(1)

    # Fetch PGP key if conditions are met
    logging.info(f"decrypt_inbound_file BEFORE EVALUATION: {pgp_params.decrypt_inbound_file}")
    logging.info(f"get_pgp_file_from_sec BEFORE EVALUATION: {pgp_params.get_pgp_file_from_sec}")

    if pgp_params.decrypt_inbound_file == "Y" and pgp_params.get_pgp_file_from_sec == "Y":
        try:
            logging.info("Getting PGP key from secret")
            secret_content = secrets.fetch_secret(pgp_params.secret_name_get_pgp_file_from_sec)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, pgp_params.private_key_file)
        except Exception as e:
            logging.error(f"Failed to process PGP secret: {e}")
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                Gcs_File_Params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            raise

    # List S3 files
    try:
        files_properties = s3.list_files_to_json(
            bucket_name=S3_Params.s3_bucket_name,
            prefix=S3_Params.s3_directory,
            recursive=recursive
        )
        logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
        if not files_properties:
            logging.info("No S3 files to process. Exiting.")
            db_manager.close_step_log(
                input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
                Gcs_File_Params.work_flow_step_log_id, "success", "no_s3_files_detected"
            )
            sys.exit(0)

        files = db_manager.select_items_to_process_by_list(
            files_properties,
            input_data.step_name,
            input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id
        )
    except Exception as e:
        logging.error(f"Failed to list files or process items: {e}")
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}"
        )
        sys.exit(1)

    # Process files or exit if none
    if not files:
        logging.info("No NEW files on S3 to process. Exiting.")
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id, "success", "no_new_s3_files_detected"
        )
        sys.exit(0)

    try:
        load_s3_files_to_gs(files, Gcs_File_Params, S3_Params, pgp_params, db_manager, s3)
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id, "success", "all_s3_files_loaded_on_gs"
        )
    except Exception as e:
        logging.error(f"Failed to process S3 to GCS files: {e}")
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            Gcs_File_Params.work_flow_step_log_id, "failed", f"{e}"
        )
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
        input_dict = json.loads(args.result)
        input_data = InputParams(
            meta_db_secret_name=input_dict['meta_db_secret_name'],
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param')
        )
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
