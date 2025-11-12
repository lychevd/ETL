import uuid
import logging
import re
import os
from typing import Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError


import uuid
import logging
import re
import os
from typing import Dict, Any

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from google.auth.transport.requests import Request  # <— import here

class BQManager:
    def __init__(self, service_account_path=None, bigquery_project=None):
        """
        Creates a BigQuery client based on provided parameters.
        """
        try:
            if service_account_path is not None:
                self.bigquery = bigquery.Client.from_service_account_json(
                    service_account_path, project=bigquery_project
                )
            elif bigquery_project is not None:
                self.bigquery = bigquery.Client(project=bigquery_project)
            else:
                self.bigquery = bigquery.Client()

            logging.info("Client initialized successfully.")
            self._log_bigquery_identity()  # <— call as instance method

        except Exception as e:
            logging.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def _log_bigquery_identity(self) -> None:
        """
        Log the effective BigQuery principal (service account/user) and project.
        """
        principal = None

        # Try via credentials on the client
        try:
            creds = getattr(self.bigquery, "_credentials", None)
            if creds is not None:
                try:
                    creds.refresh(Request())  # populate email/subject if needed
                except Exception:
                    pass
                principal = (
                    getattr(creds, "service_account_email", None)
                    or getattr(creds, "subject", None)
                )
        except Exception:
            pass

        # Fallback: ask BigQuery
        if not principal:
            try:
                row = next(self.bigquery.query("SELECT SESSION_USER() AS user").result())
                principal = row.user
            except Exception:
                principal = None

        logging.info("BigQuery project: %s", self.bigquery.project)
        logging.info("BigQuery principal: %s", principal or "<unknown>")





    def run_query_into_table(self,bigquery_project, bigquery_dataset_id,requires_boundary, query,end_boundary_query=None,Start_Boundary=None):
        """
        Executes a query based on gc_bq_params, possibly using start and end boundaries,
        creates a temporary table, retrieves the row count, and returns a tuple (json_source, row_count).

        gc_bq_params should have:
        - requires_boundary: "Y" or "N"
        - end_boundary_query (required if requires_boundary == "Y")
        - Start_Boundary
        - query: The main query to run
        - bigquery_project, bigquery_dataset_id: For creating temp table

        Returns:
            (json_source: dict, row_count: int)

        Raises:
            Exception: If queries fail or if required data is missing.
        """
        End_Boundary = ""
        try:
            if requires_boundary == "Y":
                if  end_boundary_query is None :
                    raise ValueError("requires_boundary is 'Y' but end_boundary_query is not defined.")

                query_job = self.bigquery.query(end_boundary_query)
                df = query_job.to_dataframe()
                if df.empty:
                    raise Exception("end_boundary_query returned no rows. Cannot determine End_Boundary.")

                End_Boundary = df.iat[0, 0]
                logging.info(f"End_Boundary={End_Boundary}")
        except Exception as e:
            logging.error(f"Failed to get End_Boundary: {e}")
            raise

        # Replace values in the query
        try:
            query_adjusted =query
            query_adjusted = query_adjusted.replace("|||Start_Boundary|||",Start_Boundary)
            query_adjusted = query_adjusted.replace("|||End_Boundary|||", End_Boundary)
            logging.info(f"Query after replacement={query_adjusted}")
        except Exception as e:
            logging.error(f"Failed to adjust query boundaries: {e}")
            raise

        # Create temporary table
        temp_table_id = f"{bigquery_project}.{bigquery_dataset_id}.temp_table_{uuid.uuid4().hex}"
        job_config = bigquery.QueryJobConfig(
            destination=temp_table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            use_query_cache=False
        )

        try:
            query_job = self.bigquery.query(query_adjusted, job_config=job_config)
            query_job.result()  # Waits for query to finish
            logging.info(f"Creating temp table ={temp_table_id} OK")
        except Exception as e:
            logging.error(f"Failed to create temp table using query: {e}")
            raise Exception("Failed to create temporary table.")

        # Get row count
        try:
            table = self.bigquery.get_table(temp_table_id)
            row_count = table.num_rows
            logging.info(f"Number of rows in temp table {temp_table_id}: {row_count}")
        except NotFound:
            logging.error(f"Temp table {temp_table_id} not found after query.")
            raise Exception("Temporary table not found after query execution.")
        except Exception as e:
            logging.error(f"Failed to retrieve row count: {e}")
            raise

        # Prepare the JSON source
        json_source = {
            'query': query_adjusted,
            'file_row_count': row_count
        }

        if requires_boundary == "Y":
            json_source.update({
                'Start_Boundary': Start_Boundary,
                'End_Boundary': End_Boundary,
            })
        if row_count == 0:
            try:
                self.bigquery.delete_table(temp_table_id, not_found_ok=True)
                logging.info(f"Temporary table {temp_table_id} deleted.")
            except Exception as e:
                logging.warning(f"Failed to delete temporary table {temp_table_id}: {e}")

        return row_count,json_source,temp_table_id
    def drop_bq_table(self,temp_table_id):
        try:
            self.bigquery.delete_table(temp_table_id, not_found_ok=True)
            logging.info(f"Temporary table {temp_table_id} deleted.")
        except Exception as e:
            logging.warning(f"Failed to delete temporary table {temp_table_id}: {e}")



    def bq_table_into_gs_file(self, merge_file, export_format, field_delimiter, compression, google_location,
                              temp_table_id, destination_uri, print_header):
        try:
            # Retrieve the table
            table = self.bigquery.get_table(temp_table_id)
            logging.info(f"Table {temp_table_id} retrieved successfully.")
            if field_delimiter=="TAB":
              field_delimiter="\t"

            # Configure the extract job

            if merge_file == "Y":
                extract_job_config = bigquery.job.ExtractJobConfig(
                    destination_format=export_format,
                    field_delimiter=field_delimiter,
                    print_header=False
                )
            elif merge_file == "N":
                extract_job_config = bigquery.job.ExtractJobConfig(
                    destination_format=export_format,
                    field_delimiter=field_delimiter,
                    print_header=print_header
                )
            else:
                raise ValueError(f"Invalid value for merge_file: {merge_file}. Expected 'Y' or 'N'.")

            if compression != "NONE":
                extract_job_config.compression = compression

            # Start the export job
            logging.info(f"Start exporting to GCS file: {destination_uri}")
            extract_job = self.bigquery.extract_table(
                table.reference,
                destination_uri,
                location=google_location,
                job_config=extract_job_config
            )
            extract_job.result()  # Wait for the job to complete
            logging.info(f"Finish exporting to GCS file: {destination_uri}")

        except NotFound as e:
            logging.error(f"Table {temp_table_id} not found: {e}")
            raise
        except BadRequest as e:
            logging.error(f"Bad request during export: {e}")
            raise
        except GoogleAPICallError as e:
            logging.error(f"Google Cloud API error during export: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            raise
        else:
            try:
                # Delete the temporary table
                self.bigquery.delete_table(table.reference)
                logging.info("Temp table deleted successfully.")
            except GoogleAPICallError as e:
                logging.error(f"Error deleting temp table: {e}")
                raise
            except Exception as e:
                logging.error(f"Unexpected error during table deletion: {e}")
                raise

    def run_query_with_boundaries(
            self,
            requires_boundary: str,
            query: str,
            end_boundary_query: str = None,
            start_boundary: str = None
    ) -> dict[str, str | None | Any]:
        """
        Executes a BigQuery query that may include placeholders for start/end boundaries.

        If requires_boundary == "Y", end_boundary_query is used to retrieve an end boundary value.

        Args:
            requires_boundary (str): "Y" or "N" indicating if we need to fetch an end boundary.
            query (str): Main query with optional placeholders:
                         '|||start_boudary|||' and '|||end_boudary|||'.
            end_boundary_query (str, optional): Query to fetch the end boundary if requires_boundary == "Y".
            start_boundary (str, optional): Value to replace '|||start_boudary|||' if present in query.

        Raises:
            ValueError: If requires_boundary == "Y" but end_boundary_query is missing.
            Exception: For failures in boundary retrieval or query execution.
        """

        end_boundary = ""

        # 1. Get end boundary if needed
        if requires_boundary == "Y":
            if not end_boundary_query:
                raise ValueError("requires_boundary is 'Y' but no end_boundary_query was provided.")

            try:
                boundary_job = self.bigquery.query(end_boundary_query)
                boundary_df = boundary_job.to_dataframe()

                if boundary_df.empty:
                    raise Exception("end_boundary_query returned no rows. Cannot determine end boundary.")

                end_boundary = boundary_df.iat[0, 0]  # Get the first column in the first row
                logging.info(f"Retrieved End Boundary: {end_boundary}")

            except Exception as e:
                logging.error(f"Failed to fetch End Boundary: {e}")
                raise

        # 2. Replace placeholders in the main query
        query_adjusted = query
        try:
            if start_boundary is not None:
                query_adjusted = query_adjusted.replace("|||Start_Boundary|||", start_boundary)
                query_adjusted = query_adjusted.replace("|||End_Boundary|||", end_boundary)
                logging.info(f"Query after replacement: {query_adjusted}")

        except Exception as e:
            logging.error(f"Failed to adjust query with boundaries: {e}")
            raise
        json_source = {
            'query': query_adjusted,

        }

        if requires_boundary == "Y":
            json_source.update({
                'Start_Boundary': start_boundary,
                'End_Boundary': end_boundary
            })

        # 3. Execute the query (no table creation, no row count retrieval)
        try:
            query_job = self.bigquery.query(query_adjusted)
            query_job.result()  # Wait for the query to finish
            logging.info("Query executed successfully.")
            return json_source
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise

    def run_query_and_return_json(
            self,
            requires_boundary: str,
            query: str,
            end_boundary_query: str = None,
            start_boundary: str = None
    ) -> dict[str, Any]:
        """
        Executes a BigQuery query that may include placeholders for start/end boundaries
        and returns the result as a list of JSON records.

        Args:
            requires_boundary (str): "Y" or "N" - whether an end boundary needs to be fetched.
            query (str): Main query with optional placeholders:
                         '|||Start_Boundary|||' and '|||End_Boundary|||'.
            end_boundary_query (str, optional): Query to fetch the end boundary.
            start_boundary (str, optional): Value to replace '|||Start_Boundary|||'.

        Returns:
            dict: {
                'query': query string executed,
                'Start_Boundary': ...,
                'End_Boundary': ...,
                'rows': [ list of dicts representing each row ]
            }

        Raises:
            ValueError, Exception: on errors in boundary or query execution.
        """

        end_boundary = ""

        # Step 1: Fetch end boundary if required
        if requires_boundary == "Y":
            if not end_boundary_query:
                raise ValueError("requires_boundary is 'Y' but no end_boundary_query was provided.")
            try:
                boundary_job = self.bigquery.query(end_boundary_query)
                boundary_df = boundary_job.to_dataframe()
                if boundary_df.empty:
                    raise Exception("end_boundary_query returned no rows.")
                end_boundary = boundary_df.iat[0, 0]
                logging.info(f"Retrieved End Boundary: {end_boundary}")
            except Exception as e:
                logging.error(f"Failed to fetch End Boundary: {e}")
                raise

        # Step 2: Replace placeholders
        query_adjusted = query
        try:
            if start_boundary is not None:
                query_adjusted = query_adjusted.replace("|||Start_Boundary|||", start_boundary)
            if requires_boundary == "Y":
                query_adjusted = query_adjusted.replace("|||End_Boundary|||", end_boundary)
            logging.info(f"Query after replacement: {query_adjusted}")
        except Exception as e:
            logging.error(f"Failed to replace boundaries: {e}")
            raise

        # Step 3: Execute the query and convert to JSON
        try:
            query_job = self.bigquery.query(query_adjusted)
            df = query_job.to_dataframe()
            json_rows = df.to_dict(orient="records")

            result = {
                'query': query_adjusted,
                'rows': json_rows
            }

            if requires_boundary == "Y":
                result.update({
                    'Start_Boundary': start_boundary,
                    'End_Boundary': end_boundary
                })

            logging.info("Query executed and data converted to JSON.")
            return result

        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise

    def call_get_table_header_as_string(self, call_statement: str) -> str:
        """
        Executes a BigQuery stored procedure (the entire CALL statement must be
        passed as a single string) and returns the 'header' column from the result.

        If the 'header' column is None or no rows are returned, this method raises
        a ValueError.

        Example usage:
            call_stmt = '''
                CALL `prod-ds-outbound.it_liveramp.get_table_header`(
                  'prod-ds-outbound',
                  'it_liveramp',
                  'nv0_full_final_export',
                  ','
                );
            '''
            header_str = bq_manager.call_get_table_header_as_string(call_stmt)
        """
        try:
            # 1) Run the CALL statement
            query_job = self.bigquery.query(call_statement)

            # 2) Convert the results to a list of rows
            rows = list(query_job.result())
            if not rows:
                raise ValueError("Stored procedure returned zero rows; cannot retrieve 'header'.")

            # 3) The procedure should return a single row with a column named 'header'
            header_str = rows[0].get("header")

            if header_str is None:
                # The header field is present but is NULL, or the column doesn't exist
                raise ValueError("Stored procedure returned NULL for 'header' or 'header' column was not found.")

            return header_str

        except Exception as e:
            logging.error(f"Failed to call stored procedure and retrieve 'header': {e}")
            raise

    def load_gcs_file_to_table(
            self,
            bucket_name: str,
            filename_full: str,
            dataset_id: str,
            table_id: str,
            file_format: str = 'CSV',
            write_disposition: str = 'WRITE_APPEND',
            create_disposition: str = 'CREATE_IF_NEEDED',
            autodetect: bool = True,
            schema_json: list = None,
            skip_leading_rows: int = 0,
            field_delimiter: str = ",",
            project_id: str = None
    ):
        """
        Load a file from GCS into a BigQuery table.
        """
        try:
            table_ref = f"{project_id}.{dataset_id}.{table_id}" if project_id else f"{dataset_id}.{table_id}"
            uri = f"gs://{bucket_name}/{filename_full}"
            if field_delimiter == "TAB":
                field_delimiter = "\t"

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = getattr(bigquery.SourceFormat, file_format.upper())
            job_config.write_disposition = write_disposition
            job_config.create_disposition = create_disposition

            # Determine schema behavior
            if not autodetect and skip_leading_rows == 0:
                # Pull schema from existing table
                table = self.bigquery.get_table(table_ref)
                job_config.schema = table.schema
                job_config.autodetect = False

            elif autodetect and skip_leading_rows == 1:
                # Use autodetect, skip header
                job_config.autodetect = True
                job_config.skip_leading_rows = 1

            elif autodetect and skip_leading_rows == 0 and create_disposition == "CREATE_IF_NEEDED":
                # Invalid: No schema, no headers, but table might not exist
                raise ValueError(
                    "Invalid config: autodetect=True, skip_leading_rows=0, CREATE_IF_NEEDED, but schema is not provided. Autodetect needs headers.")

            elif not autodetect and not schema_json:
                raise ValueError("autodetect=False, but schema_json is not provided.")

            elif not autodetect and schema_json:
                job_config.schema = [
                    bigquery.SchemaField(field["name"], field["type"], field.get("mode", "NULLABLE"))
                    for field in schema_json
                ]
                job_config.autodetect = False

            # Common config
            if file_format.upper() == "CSV":
                job_config.field_delimiter = field_delimiter
            if skip_leading_rows > 0 and not job_config.skip_leading_rows:
                job_config.skip_leading_rows = skip_leading_rows

            logging.info(f"Loading file {uri} into {table_ref}")
            load_job = self.bigquery.load_table_from_uri(uri, table_ref, job_config=job_config)
            load_job.result()
            logging.info(f"Loaded {load_job.output_rows} rows into {table_ref}")

            return load_job

        except Exception as e:
            logging.error(f"Failed to load GCS file to BigQuery: {e}")
            raise

    def export_query_to_local_file(
            self,
            query: str,
            output_path: str,
            field_delim: str = ",",
            row_delim: str = "\n",
            include_header: bool = True,
            requires_boundary: str = "N",
            end_boundary_query: str = None,
            start_boundary: str = None
    ) -> tuple[int, dict]:
        """
        Runs a BigQuery query and writes the result to a local file with custom field/row delimiters.
        """

        if field_delim == "TAB":
            field_delim_val = "\t"
        else:
            field_delim_val = field_delim


        if row_delim == "NEW_LINE":
            row_delimiter_val = "\n"
        elif row_delim == "CR_NEW_LINE":
            row_delimiter_val = "\r\n"
        else:
            row_delimiter_val = "\n"  # fallback
        end_boundary = ""

        # Step 1: Fetch end boundary if required
        if requires_boundary == "Y":
            if not end_boundary_query:
                raise ValueError("requires_boundary is 'Y' but no end_boundary_query was provided.")
            try:
                boundary_job = self.bigquery.query(end_boundary_query)
                boundary_df = boundary_job.to_dataframe()
                if boundary_df.empty:
                    raise Exception("end_boundary_query returned no rows.")
                end_boundary = boundary_df.iat[0, 0]
                logging.info(f"Retrieved End Boundary: {end_boundary}")
            except Exception as e:
                logging.error(f"Failed to fetch End Boundary: {e}")
                raise

        # Step 2: Replace placeholders
        query_adjusted = query
        try:
            if start_boundary is not None:
                query_adjusted = query_adjusted.replace("|||Start_Boundary|||", start_boundary)
            if requires_boundary == "Y":
                query_adjusted = query_adjusted.replace("|||End_Boundary|||", end_boundary)
            logging.info(f"Query after replacement: {query_adjusted}")
        except Exception as e:
            logging.error(f"Failed to replace boundaries: {e}")
            raise

        try:
            query_job = self.bigquery.query(query_adjusted)
            results = query_job.result()

            with open(output_path, "w", encoding="utf-8") as f:
                if include_header:
                    header = field_delim_val.join([field.name for field in results.schema])
                    f.write(header + row_delimiter_val)

                for row in results:
                    line = field_delim_val.join(
                        [str(row[field.name]) if row[field.name] is not None else "" for field in results.schema]
                    )
                    f.write(line + row_delimiter_val)

            logging.info(f"Exported {results.total_rows} rows to {output_path}")
            result = {
                'query': query_adjusted,
                'file_row_count': str(results.total_rows)
            }

            if requires_boundary == "Y":
                result.update({
                    'Start_Boundary': start_boundary,
                    'End_Boundary': end_boundary
                })
            return results.total_rows,result

        except Exception as e:
            logging.error(f"Failed to export query to file: {e}")
            raise

    def run_query_and_return_single_value(self, query: str) -> Any:
        """
        Executes a BigQuery query and returns the first column of the first row.

        Args:
            query (str): A SQL query expected to return one row and one column.

        Returns:
            Any: The value of the first column in the first row.

        Raises:
            ValueError: If the query returns no rows.
            Exception: If query execution fails.
        """
        try:
            query_job = self.bigquery.query(query)
            rows = list(query_job.result())

            if not rows:
                raise ValueError("Query returned no rows.")
            value = rows[0][0]
            logging.info(f"Single value fetched: {value}")
            return value

        except Exception as e:
            logging.error(f"Failed to fetch single value from query: {e}")
            raise

    def export_query_to_gcs_simple(
            self,
            query: str,
            destination_uri: str,  # e.g. "gs://my-bucket/path/prefix-*.csv" or "gs://my-bucket/path/single.csv"
            *,
            fmt: str = "CSV",  # "CSV", "PARQUET", "AVRO", "ORC", "JSON"
            header: bool = True,  # CSV/JSON only
            field_delimiter: str = ",",  # CSV only: ",", "\t" or "TAB"
            compression: str = "NONE",  # "NONE","GZIP","DEFLATE","SNAPPY","ZSTD" (subset depends on format)
            overwrite: bool = True,
            location: str | None = None,  # e.g. "US", "EU"
            requires_boundary: str = "N",
            end_boundary_query: str | None = None,
            start_boundary: str | None = None,

            # NEW:
            min_files: int | None = None,  # if >1, export exactly that many files
            temp_dataset: str | None = None,  # if set, materialize once: <project>.<temp_dataset>.__export_tmp_<uuid>
    ) -> dict:
        """
        Export the result of a query to GCS using BigQuery's EXPORT DATA.

        Default behavior (min_files is None or <= 1):
          - Single EXPORT DATA to the given destination_uri (wildcard allowed).
          - BigQuery may shard if you use '*' and/or if file size limits are exceeded.

        If min_files > 1:
          - Force EXACTLY `min_files` output files by running `min_files` EXPORTs,
            each exporting a disjoint hash-bucket of the result rows.
          - If temp_dataset is provided, we first CREATE TABLE AS SELECT (CTAS) once
            and then export from that table to avoid re-running your heavy query.

        Returns:
          {
            "destination_uris": [...],
            "format": "CSV" | ...,
            "header": True/False | None,
            "field_delimiter": "," | "\t" | None,
            "compression": "...",
            "overwrite": True/False,
            "location": "...",
            "query_executed": "<final SELECT used as base>",
            "total_rows": <int or None>,         # filled if temp_dataset used
            "used_temp_dataset": True/False,
          }
        """
        # Local imports to keep this self-contained
        import os
        import re
        import uuid

        # --- boundary handling ---------------------------------------------------
        end_boundary = ""
        if requires_boundary == "Y":
            if not end_boundary_query:
                raise ValueError("requires_boundary='Y' but end_boundary_query is missing.")
            bjob = self.bigquery.query(end_boundary_query, location=location)
            bdf = bjob.to_dataframe()
            if bdf.empty:
                raise ValueError("end_boundary_query returned 0 rows.")
            end_boundary = bdf.iat[0, 0]

        # --- normalize inputs ----------------------------------------------------
        if field_delimiter == "TAB":
            field_delimiter = "\t"

        # Strip trailing semicolon so we can safely append "AS <query>"
        query_clean = re.sub(r";\s*$", "", query)

        # Replace placeholders
        q_final = query_clean
        if start_boundary is not None:
            q_final = q_final.replace("|||Start_Boundary|||", str(start_boundary))
        if requires_boundary == "Y":
            q_final = q_final.replace("|||End_Boundary|||", str(end_boundary))

        # Simple SQL-string escaper for OPTIONS literals
        def qstr(s: str) -> str:
            return s.replace("'", "''")

        fmt_u = fmt.upper()
        comp_u = compression.upper()

        def build_options(uri: str, hdr: bool) -> str:
            """Build the EXPORT DATA OPTIONS(...) fragment."""
            opts = [f"uri='{qstr(uri)}'", f"format='{fmt_u}'"]
            if fmt_u in ("CSV", "JSON"):
                opts.append(f"header={'TRUE' if hdr else 'FALSE'}")
            if fmt_u == "CSV":
                opts.append(f"field_delimiter='{qstr(field_delimiter)}'")
            if comp_u != "NONE":
                opts.append(f"compression='{comp_u}'")
            if overwrite:
                opts.append("overwrite=TRUE")
            return ", ".join(opts)

        # --- Simple path: single export -----------------------------------------
        if not min_files or min_files <= 1:
            sql = f"EXPORT DATA OPTIONS ({build_options(destination_uri, header)}) AS {q_final}"
            job = self.bigquery.query(sql, location=location)
            job.result()
            return {
                "destination_uris": [destination_uri],
                "format": fmt_u,
                "header": header if fmt_u in ("CSV", "JSON") else None,
                "field_delimiter": field_delimiter if fmt_u == "CSV" else None,
                "compression": comp_u,
                "overwrite": overwrite,
                "location": location,
                "query_executed": q_final,
                "total_rows": None,
                "used_temp_dataset": False,
            }

        # --- Forced multi-file export: exactly min_files files -------------------
        n = int(min_files)

        # Derive a URI template for N outputs
        if "*" in destination_uri:
            uri_tmpl = destination_uri.replace("*", "{i:02d}")
        else:
            base, ext = os.path.splitext(destination_uri)
            uri_tmpl = f"{base}-" + "{i:02d}" + (ext or "")

        total_rows = None
        used_temp = False

        # Hash-bucketing expression that is safe for negatives (no ABS overflow):
        # bucket in [1..n]
        def bucket_expr(struct_alias: str) -> str:
            # TO_JSON_STRING((SELECT AS STRUCT alias.*)) serializes the entire row deterministically
            return f"1 + MOD(MOD(FARM_FINGERPRINT(TO_JSON_STRING((SELECT AS STRUCT {struct_alias}.*))), {n}) + {n}, {n})"

        if temp_dataset:
            # Materialize once to a temp table in the given dataset
            used_temp = True
            tmp_id = f"{self.bigquery.project}.{temp_dataset}.__export_tmp_{uuid.uuid4().hex.replace('-', '')}"
            create_sql = f"CREATE TABLE `{tmp_id}` AS {q_final}"
            self.bigquery.query(create_sql, location=location).result()

            try:
                # Count rows (optional but handy to return)
                table = self.bigquery.get_table(tmp_id)
                total_rows = table.num_rows

                # Export N hash buckets from the materialized table
                for i in range(1, n + 1):
                    uri_i = uri_tmpl.format(i=i)
                    export_sql = f"""
                    EXPORT DATA OPTIONS ({build_options(uri_i, header)}) AS
                    SELECT * EXCEPT(bucket) FROM (
                      SELECT s.*,
                             {bucket_expr('s')} AS bucket
                      FROM `{tmp_id}` AS s
                    )
                    WHERE bucket = {i}
                    """
                    self.bigquery.query(export_sql, location=location).result()
            finally:
                # Best-effort cleanup of the temp table
                try:
                    self.bigquery.delete_table(tmp_id, not_found_ok=True)
                except Exception:
                    pass

        else:
            # No temp table â€” re-run the base query N times with in-line bucketing
            # (simpler, but can be more expensive for heavy queries)
            for i in range(1, n + 1):
                uri_i = uri_tmpl.format(i=i)
                export_sql = f"""
                EXPORT DATA OPTIONS ({build_options(uri_i, header)}) AS
                SELECT * EXCEPT(bucket) FROM (
                  SELECT t.*,
                         {bucket_expr('t')} AS bucket
                  FROM ({q_final}) AS t
                )
                WHERE bucket = {i}
                """
                self.bigquery.query(export_sql, location=location).result()

        return {
            "destination_uris": [uri_tmpl.format(i=i) for i in range(1, n + 1)],
            "format": fmt_u,
            "header": header if fmt_u in ("CSV", "JSON") else None,
            "field_delimiter": field_delimiter if fmt_u == "CSV" else None,
            "compression": comp_u,
            "overwrite": overwrite,
            "location": location,
            "query_executed": q_final,
            "total_rows": total_rows,  # filled when temp_dataset is used
            "used_temp_dataset": used_temp,
        }



