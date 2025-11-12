import sys
import argparse
import logging
import json
from dataclasses import dataclass, field, fields
from typing import Optional

import uuid
import os

from core import gcs_manager, secret_manager, bq_manager
from db import database_manager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sftp_file_mover.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class InputParams:
    meta_db_secret_name: str
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    additional_param: Optional[str] = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing = [
            f.name for f in fields(self)
            if getattr(self, f.name) is None and not f.metadata.get("optional", False)
        ]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")


@dataclass
class GcBQParams:
    # Required
    work_flow_step_log_id: str
    gs_bucket_name: str
    gs_directory: str
    service_account_path: str
    query: str
    end_boundary_query: str
    field_delimiter: str
    google_location: str
    bigquery_project: str
    bigquery_dataset_id: str
    compression: str
    export_format: str
    get_key_file_sec_name: str
    Start_Boundary: str
    filename_base: str
    print_header: bool
    is_header_dynamic: str
    sql_dynamic_header: str
    merge_file: str
    header_line: str
    drop_temp_dir: str
    dest_file_template: str
    split_count: str
    pad_need: str
    offset: int
    pad_char: str
    left_count: int
    right_count: int

    # Optional
    get_key_file_from_sec: str = field(default='Y', metadata={"optional": True})
    requires_boundary: str = field(default='Y', metadata={"optional": True})
    def_bq_cred: str = field(default='Y', metadata={"optional": True})
    def_bq_project: str = field(default='Y', metadata={"optional": True})
    dest_gs_directory: Optional[str] = field(default=None, metadata={"optional": True})
    dest_bucket_name: Optional[str] = field(default=None, metadata={"optional": True})

    def __post_init__(self):
        missing = [
            f.name for f in fields(self)
            if getattr(self, f.name) is None and not f.metadata.get("optional", False)
        ]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# Core Workflow
# ---------------------------------------------------------------------------
def create_gs_file_from_bq(gc_bq_params: GcBQParams,
                           db_manager: database_manager.DatabaseManager):
    """
    1) Materialize main query into a temp table (row_count + avoid recompute)
    2) For split_count > 1: export N slices with EXPORT DATA (wildcard URIs, header=False),
       then compose shards (+ header blob if requested) into exactly N final files, keeping 00,10,20… pattern.
       For split_count == 1: same approach but produce one final file.
    3) Header handling:
       - If print_header is true, we build a single header line using the chosen delimiter and compose it once.
       - This avoids duplicate headers across shards.
    4) Always drop the temp table in finally.
    """
    import json
    import logging
    import os
    import re
    import uuid

    temp_table_id = None
    bq = None

    try:
        # --- Clients ----------------------------------------------------------
        service_account_path = None if gc_bq_params.def_bq_cred == "Y" else gc_bq_params.service_account_path
        bq_project_name     = None if gc_bq_params.def_bq_project == "Y" else gc_bq_params.bigquery_project

        bq  = bq_manager.BQManager(service_account_path, bq_project_name)
        gcs = gcs_manager.GCSManager(service_account_path, bq_project_name)

        # --- Prepare query (inject workflow ids) ------------------------------
        query = gc_bq_params.query.replace("|||WorkFlow_Log_id|||", input_data.work_flow_log_id)
        query = query.replace("|||WorkFlow_Step_Log_id|||", gc_bq_params.work_flow_step_log_id)

        # --- Materialize into temp table -------------------------------------
        row_count, json_source, temp_table_id = bq.run_query_into_table(
            gc_bq_params.bigquery_project,
            gc_bq_params.bigquery_dataset_id,
            gc_bq_params.requires_boundary,
            query,
            gc_bq_params.end_boundary_query,
            gc_bq_params.Start_Boundary
        )
        logging.info(f"row_count={row_count}, temp_table_id={temp_table_id}")
        logging.info(f"json_source={json.dumps(json_source)}")

        # --- No data short-circuit -------------------------------------------
        if row_count == 0:
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Source",
                json_source
            )
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                "success",
                "no_records_to_output"
            )
            return

        # --- Final destination (bucket/dir) ----------------------------------
        if not gc_bq_params.gs_directory.endswith('/'):
            gc_bq_params.gs_directory += '/'

        final_bucket_name = (gc_bq_params.dest_bucket_name or gc_bq_params.gs_bucket_name)
        final_dir         = (gc_bq_params.dest_gs_directory or gc_bq_params.gs_directory)
        if not final_dir.endswith('/'):
            final_dir += '/'

        final_bucket = gcs.client.bucket(final_bucket_name)

        # --- Naming / formatting params --------------------------------------
        split_count = int(gc_bq_params.split_count or "1")
        var_offset  = int(gc_bq_params.offset or 0)
        left_count  = int(gc_bq_params.left_count or 0)
        right_count = int(gc_bq_params.right_count or 0)

        raw_delim = gc_bq_params.field_delimiter or ","
        delim_for_export = "\t" if raw_delim == "TAB" else raw_delim

        # When we compose shards (always in this function), compression must be NONE.
        comp_for_export = (gc_bq_params.compression or "NONE")
        if comp_for_export != "NONE":
            logging.warning(
                "Compression '%s' overridden to 'NONE' because we compose shards/headers in GCS.",
                comp_for_export
            )
            comp_for_export = "NONE"

        # --- Header handling --------------------------------------------------
        # If print_header is true, we need a single header line we can prepend during compose.
        want_header = str(gc_bq_params.print_header).lower() in ("true", "t", "1", "y", "yes")
        header_line_str = None
        if want_header:
            if gc_bq_params.is_header_dynamic == "Y" and gc_bq_params.sql_dynamic_header:
                try:
                    header_line_str = bq.call_get_table_header_as_string(gc_bq_params.sql_dynamic_header)
                except Exception:
                    header_line_str = gc_bq_params.header_line
            else:
                header_line_str = gc_bq_params.header_line

            # If still not provided, derive from table schema
            if not header_line_str or not header_line_str.strip():
                table = bq.bigquery.get_table(temp_table_id)
                cols = [f.name for f in table.schema]
                header_line_str = delim_for_export.join(cols)
            else:
                # Normalize spaces -> delimiter
                cols = re.split(r"\s+", header_line_str.strip())
                header_line_str = delim_for_export.join(cols)

        # --- Deterministic bucket expression (0..split_count-1) ---------------
        def bucket_expr():
            # MOD(MOD(FARM_FINGERPRINT(...), N) + N, N) avoids negatives
            return (
                f"MOD(MOD(FARM_FINGERPRINT(TO_JSON_STRING((SELECT AS STRUCT t.*))), {split_count}) + "
                f"{split_count}, {split_count})"
            )

        # --- Export + compose -------------------------------------------------
        successors = []
        tmp_root = f"{final_dir}._export_{uuid.uuid4().hex}"  # lives in FINAL bucket/dir (easy cleanup)

        if split_count > 1:
            # Produce exactly N final files
            for k in range(split_count):
                # Final short name (00,10,20… with your pad/offset rules)
                filename_template = gc_bq_params.dest_file_template or "part-{part_index}.csv"
                final_name_only = gcs._render_part_filename(
                    template=filename_template,
                    part_index_one_based=(k + 1),
                    offset=var_offset,
                    pad_need=(gc_bq_params.pad_need or "N"),
                    pad_char=(gc_bq_params.pad_char or "0"),
                    left_count=(left_count if left_count > 0 else None),
                    right_count=(right_count if right_count > 0 else None),
                )

                # Export this slice to a wildcard shard prefix (header=False)
                slice_prefix = f"{tmp_root}/k{k:02d}"
                shard_uri = f"gs://{final_bucket_name}/{slice_prefix}/part-*.csv"
                slice_query = (
                    f"SELECT * FROM `{temp_table_id}` AS t "
                    f"WHERE {bucket_expr()} = {k}"
                )
                info = bq.export_query_to_gcs_simple(
                    query=slice_query,
                    destination_uri=shard_uri,     # MUST be wildcard for EXPORT DATA
                    fmt=(gc_bq_params.export_format or "CSV"),
                    header=False,                  # we'll add header once during compose
                    field_delimiter=delim_for_export,
                    compression=comp_for_export,
                    location=(gc_bq_params.google_location or None)
                )
                logging.info(f"slice {k} export info: {info}")

                # List shard blobs for this slice
                shard_blobs = list(gcs.client.list_blobs(final_bucket_name, prefix=f"{slice_prefix}/"))
                shard_blobs = [b for b in shard_blobs if not b.name.endswith('/')]
                logging.info(f"slice {k} shards: {len(shard_blobs)}")

                # If no shards (no rows in this slice), still produce a file:
                components = []
                header_blob = None
                if want_header and header_line_str:
                    header_blob = gcs._make_header_blob(
                        bucket=final_bucket,
                        tmp_prefix=tmp_root,
                        header_line=header_line_str,
                        content_type="text/csv",
                    )
                    components.append(header_blob)

                if shard_blobs:
                    components.extend(shard_blobs)
                else:
                    # create an empty data blob so compose has at least one component
                    empty_data = final_bucket.blob(f"{slice_prefix}/empty.csv")
                    empty_data.upload_from_string("", content_type="text/csv", client=gcs.client)
                    components.append(empty_data)

                # Compose components -> final single file
                final_blob = final_bucket.blob(f"{final_dir}{final_name_only}")
                gcs._compose_many(
                    bucket=final_bucket,
                    components=components,
                    dest_blob=final_blob,
                    content_type="text/csv",
                    tmp_prefix=f"{tmp_root}/compose_tmp/k{k:02d}",
                    delete_temps=True,
                )
                final_blob.reload()

                # Clean up components for this slice (best-effort)
                try:
                    for b in gcs.client.list_blobs(final_bucket_name, prefix=f"{slice_prefix}/"):
                        try:
                            b.delete()
                        except Exception:
                            pass
                    if header_blob:
                        try:
                            header_blob.delete()
                        except Exception:
                            pass
                except Exception:
                    pass

                # Record successor metadata
                meta = gcs.get_gs_file_pro(
                    final_bucket_name, final_dir, final_name_only, row_count=None
                )
                successors.append(meta)

            # Remove the tmp root folder
            try:
                for b in gcs.client.list_blobs(final_bucket_name, prefix=f"{tmp_root}/"):
                    try:
                        b.delete()
                    except Exception:
                        pass
            except Exception:
                pass

        else:
            # Single final file
            final_name_only = gc_bq_params.filename_base

            # Export to wildcard shard prefix (header=False)
            slice_prefix = f"{tmp_root}/single"
            shard_uri = f"gs://{final_bucket_name}/{slice_prefix}/part-*.csv"
            info = bq.export_query_to_gcs_simple(
                query=f"SELECT * FROM `{temp_table_id}`",
                destination_uri=shard_uri,       # MUST be wildcard
                fmt=(gc_bq_params.export_format or 'CSV'),
                header=False,                    # add header once in compose if requested
                field_delimiter=delim_for_export,
                compression=comp_for_export,
                location=(gc_bq_params.google_location or None)
            )
            logging.info(f"single export (no header) info: {info}")

            shard_blobs = list(gcs.client.list_blobs(final_bucket_name, prefix=f"{slice_prefix}/"))
            shard_blobs = [b for b in shard_blobs if not b.name.endswith('/')]

            components = []
            header_blob = None
            if want_header and header_line_str:
                header_blob = gcs._make_header_blob(
                    bucket=final_bucket,
                    tmp_prefix=tmp_root,
                    header_line=header_line_str,
                    content_type="text/csv",
                )
                components.append(header_blob)

            if shard_blobs:
                components.extend(shard_blobs)
            else:
                empty_data = final_bucket.blob(f"{slice_prefix}/empty.csv")
                empty_data.upload_from_string("", content_type="text/csv", client=gcs.client)
                components.append(empty_data)

            final_blob = final_bucket.blob(f"{final_dir}{final_name_only}")
            gcs._compose_many(
                bucket=final_bucket,
                components=components,
                dest_blob=final_blob,
                content_type="text/csv",
                tmp_prefix=f"{tmp_root}/compose_tmp/single",
                delete_temps=True,
            )
            final_blob.reload()

            # Cleanup tmp
            try:
                for b in gcs.client.list_blobs(final_bucket_name, prefix=f"{slice_prefix}/"):
                    try:
                        b.delete()
                    except Exception:
                        pass
                if header_blob:
                    try:
                        header_blob.delete()
                    except Exception:
                        pass
                for b in gcs.client.list_blobs(final_bucket_name, prefix=f"{tmp_root}/"):
                    try:
                        b.delete()
                    except Exception:
                        pass
            except Exception:
                pass

            successors.append(
                gcs.get_gs_file_pro(final_bucket_name, final_dir, final_name_only, row_count)
            )

        # --- Persist Source + Successors --------------------------------------
        db_manager.create_dataset_instance(
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            input_data.step_name,
            "Source",
            json_source
        )
        for file_meta in successors:
            logging.info(f"Successor JSON= {json.dumps(file_meta)}")
            db_manager.create_dataset_instance(
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                input_data.step_name,
                "Successor",
                file_meta
            )

    except Exception as err:
        logging.error(f"Error in create_gs_file_from_bq: {err}")
        db_manager.close_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            gc_bq_params.work_flow_step_log_id,
            "FAILED",
            str(err)
        )
        raise
    finally:
        # Always try to drop the temp table
        try:
            if temp_table_id and bq is not None:
                bq.drop_bq_table(temp_table_id)
        except Exception as e:
            logging.warning(f"Best-effort cleanup: failed to drop temp table {temp_table_id}: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    secrets = secret_manager.SecretManager()
    meta_connection = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)

    db_mgr = database_manager.DatabaseManager(
        meta_connection.mysql_host,
        meta_connection.mysql_user,
        meta_connection.mysql_password,
        meta_connection.mysql_database
    )
    _ = gcs_manager.GCSManager()  # instantiate if you need early validation

    variables = db_mgr.start_workflow_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        input_data.additional_param
    )

    # Ensure 'from_secret_list' exists
    if variables.loc[variables['key'] == 'from_secret_list'].empty:
        variables.loc[len(variables)] = ['from_secret_list', '[]']
    from_secret_list = json.loads(variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0])

    # Build params
    gc_bq_params = GcBQParams(
        work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
        gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
        gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
        service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
        query=secrets.get_variable_value('query', variables, from_secret_list),
        end_boundary_query=secrets.get_variable_value('end_boundary_query', variables, from_secret_list),
        requires_boundary=secrets.get_variable_value('requires_boundary', variables, from_secret_list),
        get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
        get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
        field_delimiter=secrets.get_variable_value('field_delimiter', variables, from_secret_list),
        google_location=secrets.get_variable_value('google_location', variables, from_secret_list),
        bigquery_dataset_id=secrets.get_variable_value('bigquery_dataset_id', variables, from_secret_list),
        bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list),
        compression=secrets.get_variable_value('compression', variables, from_secret_list),
        export_format=secrets.get_variable_value('export_format', variables, from_secret_list),
        Start_Boundary=secrets.get_variable_value('Start_Boundary', variables, from_secret_list),
        filename_base=secrets.get_variable_value('filename_base', variables, from_secret_list),
        print_header=secrets.get_variable_value('print_header', variables, from_secret_list),
        merge_file=secrets.get_variable_value('merge_file', variables, from_secret_list),
        header_line=secrets.get_variable_value('header_line', variables, from_secret_list),
        drop_temp_dir=secrets.get_variable_value('drop_temp_dir', variables, from_secret_list),
        def_bq_cred=secrets.get_variable_value('def_bq_cred', variables, from_secret_list),
        def_bq_project=secrets.get_variable_value('def_bq_project', variables, from_secret_list),
        dest_file_template=secrets.get_variable_value('dest_file_template', variables, from_secret_list),
        split_count=secrets.get_variable_value('split_count', variables, from_secret_list),
        pad_need=secrets.get_variable_value('pad_need', variables, from_secret_list),
        offset=secrets.get_variable_value('offset', variables, from_secret_list),
        pad_char=secrets.get_variable_value('pad_char', variables, from_secret_list),
        left_count=secrets.get_variable_value('left_count', variables, from_secret_list),
        right_count=secrets.get_variable_value('right_count', variables, from_secret_list),
        is_header_dynamic=secrets.get_variable_value('is_header_dynamic', variables, from_secret_list),
        sql_dynamic_header=secrets.get_variable_value('sql_dynamic_header', variables, from_secret_list),
        dest_gs_directory=secrets.get_variable_value('dest_gs_directory', variables, from_secret_list),
        dest_bucket_name=secrets.get_variable_value('dest_bucket_name', variables, from_secret_list),
    )

    logging.info(f"gc_bq_params: {gc_bq_params}")

    # Optionally fetch service account key from Secret Manager
    if gc_bq_params.get_key_file_from_sec == "Y":
        try:
            logging.info("Getting key from secret")
            secret_content = secrets.fetch_secret(gc_bq_params.get_key_file_sec_name)
            logging.info("Secret content fetched successfully")
            secrets.write_secret_to_file(secret_content, gc_bq_params.service_account_path)
        except Exception as e:
            logging.error(f"Failed to process secret: {e}")
            db_mgr.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                gc_bq_params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            sys.exit(1)

    # Run the export workflow
    create_gs_file_from_bq(gc_bq_params, db_mgr)

    # Close step as success (if not already closed in an early-return branch)
    db_mgr.close_step_log(
        input_data.workflow_name,
        input_data.step_name,
        input_data.work_flow_log_id,
        gc_bq_params.work_flow_step_log_id,
        "success",
        "file_created_ok"
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input JSON and fetch workflow data.")
    parser.add_argument("--result", required=True, help="Input JSON in dictionary format")
    parser.add_argument("--workflow_name", required=True, help="Name of workflow executing")
    parser.add_argument("--step_name", required=True, help="Name of step to execute")
    args = parser.parse_args()

    # Parse input JSON for secrets/workflow IDs
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
