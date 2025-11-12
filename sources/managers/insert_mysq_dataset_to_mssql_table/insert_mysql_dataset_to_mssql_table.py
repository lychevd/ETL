import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields
from typing import Optional

from sqlalchemy import text  # used for boundary query via the transfer's engine

from core import secret_manager, local_util
from core.mysql_to_mssql_manager import MySQLToMSSQLSA  # <-- your new class (SQLAlchemy-based)
from db import database_manager

# --------------------------- Logging ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("sftp_file_mover.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --------------------------- Dataclasses ---------------------------
@dataclass
class InputParams:
    meta_db_secret_name: str
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    additional_param: Optional[str] = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing = [f_.name for f_ in fields(self)
                   if getattr(self, f_.name) is None and not f_.metadata.get("optional", False)]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

@dataclass
class MySqlParamsOut:
    mysql_host: str
    mysql_user: str
    mysql_database: str
    mysql_password: str
    mysql_port: str
    requires_boundary: str
    query: str
    end_boundary_query: str
    Start_Boundary: str

    def __post_init__(self):
        missing = [f_.name for f_ in fields(self)
                   if getattr(self, f_.name) is None and not f_.metadata.get("optional", False)]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

@dataclass
class MsSqlParamsOut:
    ms_sql_server: str
    ms_sql_database: str
    ms_sql_password: str
    ms_sql_user: str
    ms_sql_port: str
    ms_sql_schema: str
    conn_secret: str
    work_flow_step_log_id: str
    insert_query: str

    def __post_init__(self):
        missing = [f_.name for f_ in fields(self)
                   if getattr(self, f_.name) is None and not f_.metadata.get("optional", False)]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

# --------------------------- Main transfer wrapper ---------------------------
def load_mysql_dataset_to_mssql(
    ms_sql_params_out: MsSqlParamsOut,
    my_sql_params_out: MySqlParamsOut,
    db_manager: database_manager.DatabaseManager,
):
    try:
        # Local util (kept for parity with your environment)
        try:
            local_util.LocalUtil()
            logging.info("local initialization successful!")
        except Exception as err:
            logging.error(f"Error initializing local manager: {err}")
            raise

        # Build query from template placeholders
        query_adjusted = my_sql_params_out.query
        query_adjusted = query_adjusted.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query_adjusted = query_adjusted.replace("|||WorkFlow_Step_Log_id|||", ms_sql_params_out.work_flow_step_log_id)

        # Create transfer engine (uses SQLAlchemy for MySQL + pyodbc for SQL Server under the hood)
        transfer = MySQLToMSSQLSA(
            mysql_host=my_sql_params_out.mysql_host,
            mysql_db=my_sql_params_out.mysql_database,
            mysql_user=my_sql_params_out.mysql_user,
            mysql_password=my_sql_params_out.mysql_password,
            mysql_port=int(my_sql_params_out.mysql_port),
            force_writer=True,  # ensures we don’t accidentally read from a replica
            mssql_host=ms_sql_params_out.ms_sql_server,
            mssql_db=ms_sql_params_out.ms_sql_database,
            mssql_user=ms_sql_params_out.ms_sql_user,
            mssql_password=ms_sql_params_out.ms_sql_password,
            mssql_port=int(ms_sql_params_out.ms_sql_port),
            mysql_driver="pymysql"
        )

        # Resolve boundary (via transfer’s SQLAlchemy engine)
        End_boundary = None
        if my_sql_params_out.requires_boundary == "Y":
            try:
                # NOTE: if your class exposes a different attribute than `mysql_engine`, change it here.
                with transfer.mysql_engine.connect() as conn:  # type: ignore[attr-defined]
                    res = conn.execute(text(my_sql_params_out.end_boundary_query))
                    row = res.fetchone()
                    End_boundary = row[0] if row and len(row) > 0 else None

                query_adjusted = query_adjusted.replace(
                    "|||Start_Boundary|||", my_sql_params_out.Start_Boundary or ""
                )
                query_adjusted = query_adjusted.replace(
                    "|||End_boundary|||", "" if End_boundary is None else str(End_boundary)
                )
                logging.info(f"Query after boundary replacement: {query_adjusted}")
            except Exception as e:
                logging.error(f"Failed to fetch end boundary via engine: {e}")

        # Guard: no unresolved placeholders
        if "|||" in query_adjusted:
            raise ValueError(f"Unresolved placeholder in query: {query_adjusted}")

        # Transfer rows (the class performs its own session prep, preview/logging, and row counting)
        row_count = transfer.transfer_fetchall(
            select_sql=query_adjusted,
            insert_sql=ms_sql_params_out.insert_query,
            preview_rows=10
        )

        # Build metadata for step log
        json_source = {
            "query": query_adjusted,
            "file_row_count": str(row_count),
        }
        if my_sql_params_out.requires_boundary == "Y":
            json_source.update(
                {"Start_Boundary": my_sql_params_out.Start_Boundary, "End_Boundary": End_boundary}
            )

        logging.info(f"json_source JSON= {json.dumps(json_source)}")
        logging.info(f"row_count= {row_count}")

        # Zero-row short-circuit
        if row_count == 0:
            logging.info("No records output. Exiting successfully.")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                json_source,
            )
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                ms_sql_params_out.work_flow_step_log_id,
                "success",
                "no_records_to_output",
            )
            sys.exit(0)

        # Normal OK path
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source,
        )

    except Exception as err:
        logging.error(f"Error in load_mysql_dataset_to_mssql: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            ms_sql_params_out.work_flow_step_log_id,
            "FAILED",
            str(err),
        )
        raise

# --------------------------- Entry ---------------------------
def main():
    secrets = secret_manager.SecretManager()

    meta_conn = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)
    db_manager = database_manager.DatabaseManager(
        meta_conn.mysql_host,
        meta_conn.mysql_user,
        meta_conn.mysql_password,
        meta_conn.mysql_database,
    )

    variables = db_manager.start_workflow_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        input_data.additional_param,
    )
    if variables.loc[variables["key"] == "from_secret_list"].empty:
        variables.loc[len(variables)] = ["from_secret_list", "[]"]

    from_secret_list = json.loads(
        variables.loc[variables["key"] == "from_secret_list", "value"].values[0]
    )
    logging.info(variables)

    ms_sql_params_out = MsSqlParamsOut(
        work_flow_step_log_id=secrets.get_variable_value("WorkFlow_Step_Log_id", variables, from_secret_list),
        conn_secret=secrets.get_variable_value("conn_secret", variables, from_secret_list),
        ms_sql_server=secrets.get_variable_value("ms_sql_server", variables, from_secret_list),
        ms_sql_database=secrets.get_variable_value("ms_sql_database", variables, from_secret_list),
        ms_sql_password=secrets.get_variable_value("ms_sql_password", variables, from_secret_list),
        ms_sql_port=secrets.get_variable_value("ms_sql_port", variables, from_secret_list),
        ms_sql_schema=secrets.get_variable_value("ms_sql_schema", variables, from_secret_list),
        ms_sql_user=secrets.get_variable_value("ms_sql_user", variables, from_secret_list),
        insert_query=secrets.get_variable_value("insert_query", variables, from_secret_list),
    )

    my_sql_params_out = MySqlParamsOut(
        mysql_host=secrets.get_variable_value("mysql_host", variables, from_secret_list),
        mysql_user=secrets.get_variable_value("mysql_user", variables, from_secret_list),
        mysql_database=secrets.get_variable_value("mysql_database", variables, from_secret_list),
        mysql_password=secrets.get_variable_value("mysql_password", variables, from_secret_list),
        mysql_port=secrets.get_variable_value("mysql_port", variables, from_secret_list),
        requires_boundary=secrets.get_variable_value("requires_boundary", variables, from_secret_list),
        query=secrets.get_variable_value("query", variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value("end_boundary_query", variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value("Start_Boundary", variables, from_secret_list),
    )

    logging.info("my_sql_params_out=%s", my_sql_params_out)

    load_mysql_dataset_to_mssql(ms_sql_params_out, my_sql_params_out, db_manager)

    db_manager.close_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        ms_sql_params_out.work_flow_step_log_id,
        "success",
        "records_transfered_ok",
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input JSON and fetch workflow data.")
    parser.add_argument("--result", required=True, help="Input JSON in dictionary format")
    parser.add_argument("--workflow_name", required=True, help="Name of workflow executing")
    parser.add_argument("--step_name", required=True, help="Name of step to execute")
    args = parser.parse_args()

    try:
        input_dict = json.loads(args.result)
        input_data = InputParams(
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict["WorkFlow_Log_id"],
            additional_param=input_dict.get("additional_param"),
            meta_db_secret_name=input_dict["meta_db_secret_name"],
        )
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
