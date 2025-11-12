import sys
import argparse
import subprocess
import posixpath
import logging
import json
import os
from dataclasses import dataclass, field, fields

from core import (
    gcs_manager,
    pgp_manager,
    secret_manager,
    gsutil_manager,
    local_util,
    s3_manager,
)
from db import database_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sftp_file_mover.log"),
        logging.StreamHandler(sys.stdout),
    ],
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
            field.name
            for field in fields(self)
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
    work_flow_step_log_id: str

    def __post_init__(self):
        missing_fields = [
            field.name
            for field in fields(self)
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
    aws_region_name = _none_if_empty(S3_Params.aws_region_name)
    aws_session_token = _none_if_empty(S3_Params.aws_session_token)
    aws_role_arn = _none_if_empty(S3_Params.aws_role_arn)
    aws_role_session_name = _none_if_empty(S3_Params.aws_role_session_name)
    aws_external_id = _none_if_empty(S3_Params.aws_external_id)

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
        "aws_access_key_id": _none_if_empty(S3_Params.aws_access_key_id),
        "aws_secret_access_key": _none_if_empty(S3_Params.aws_secret_access_key),
        "aws_region_name": aws_region_name,
        "aws_session_token": aws_session_token,
        "aws_role_arn": aws_role_arn,
        "aws_role_session_name": aws_role_session_name,
        "aws_session_duration": aws_session_duration,
        "aws_external_id": aws_external_id,
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

def main():
    files = []
    secrets = secret_manager.SecretManager()

    meta_connection = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)
    db_manager = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database,
    )

    variables = db_manager.start_workflow_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        input_data.additional_param,
    )
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']

    from_secret_list = variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
    from_secret_list = json.loads(from_secret_list)

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
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
    )
    logging.info(f"S3_Params={S3_Params}")

    s3 = create_s3_client_from_params(S3_Params)
    recursive = str_to_bool_strict(S3_Params.recursive)

    # List S3 files
    try:
        files_properties = s3.list_files_to_json(
            bucket_name=S3_Params.s3_bucket_name,
            prefix=S3_Params.s3_directory,
            recursive=recursive,
        )
        logging.info(f"files_properties FULL LIST OF FILES = {json.dumps(files_properties)}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            S3_Params.work_flow_step_log_id,
            "success",
            "all_s3_files_loaded_on_gs",
        )
    except Exception as e:
        logging.error(f"Failed to process S3 to GCS files: {e}")
        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            S3_Params.work_flow_step_log_id, "failed", f"{e}"
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
            additional_param=input_dict.get('additional_param'),
        )
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
