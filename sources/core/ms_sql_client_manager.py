import pyodbc
import logging
import socket
import ssl
import requests
import pandas as pd

class SQLClient:
    @staticmethod
    def check_port(host, port, timeout=5):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                logging.info(f"Port check OK: {host}:{port}")
                return True
        except (socket.timeout, socket.error) as e:
            ##logging.error(f"Port check failed: {host}:{port} - {e}")
            return False

    @staticmethod
    def check_tls_version(host, port):
        try:
            context = ssl.create_default_context()
            with socket.create_connection((host, port), timeout=5) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    tls_version = ssock.version()
                    logging.info(f"TLS version used to connect to {host}:{port} is: {tls_version}")
                    return tls_version
        except Exception as e:
            logging.warning(f"Unable to determine TLS version for {host}:{port}: {e}")
            return None

    @staticmethod
    def get_my_ip():
        try:
            # Get private (local) IP address
            hostname = socket.gethostname()
            private_ip = socket.gethostbyname(hostname)
            logging.info(f"Private IP (local): {private_ip}")
        except Exception as e:
            private_ip = None
            logging.error(f"Error detecting private IP: {e}")

        try:
            # Get public IP address
            public_ip = requests.get("https://api.ipify.org", timeout=5).text
            logging.info(f"Public IP: {public_ip}")
        except Exception as e:
            public_ip = None
            logging.error(f"Error detecting public IP: {e}")

        return {"private_ip": private_ip, "public_ip": public_ip}

    def __init__(self, server, database, username, password, port):
        self.server = server
        self.port = int(port)
        self.database = database
        self.username = username
        self.password = password

        self.get_my_ip()

        if not self.check_port(server, self.port):
            raise ConnectionError(f"Port {port} on {server} is not reachable.")
        self.get_my_ip()
        # Optional TLS version check before pyodbc
        ##self.check_tls_version(server, self.port)

        try:
            self.connection = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=yes"
            )
            self.cursor = self.connection.cursor()
            logging.info(f"Successfully connected to SQL Server {server}:{port}!")
        except Exception as e:
            logging.error(f"Error connecting to SQL Server {server}:{port}: {e}")
            raise

    def execute_query_autocommit(self, query, data_return):
        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(query)

            if data_return == "Y":
                results = cursor.fetchall()
                logging.info(f"Successfully executed query: {query}")
                return results
            else:
                logging.info(f"Successfully executed query: {query}")
                return None
        except Exception as e:
            logging.error(f"Error executing query {query}: {e}")
            raise

    def insert_from_file(
            self,
            data_file_path,
            insert_query,
            delimiter="TAB",
            row_delimiter="NEW_LINE",
            skip_header_rows=0
    ):
        """
        Reads a text file and inserts rows into the target SQL Server table.

        Args:
            data_file_path (str): Path to the text file.
            insert_query (str): SQL INSERT statement with placeholders (?, ?, ...).
            delimiter (str): Field delimiter ("TAB", ",", or other string).
            row_delimiter (str): Row delimiter keyword ("NEW_LINE", "CR_NEW_LINE", "CUSTOM").
            skip_header_rows (int): Number of top rows to skip (e.g., header).
        """
        try:
            # Map delimiters
            if delimiter == "TAB":
                field_delimiter_val = "\t"
            else:
                field_delimiter_val = delimiter

            if row_delimiter == "NEW_LINE":
                row_delimiter_val = "\n"
            elif row_delimiter == "CR_NEW_LINE":
                row_delimiter_val = "\r\n"
            else:
                row_delimiter_val = row_delimiter

            logging.info(f"Reading file: {data_file_path}")
            logging.info(f"Field delimiter: {field_delimiter_val}")
            logging.info(f"Row delimiter: {repr(row_delimiter_val)}")
            logging.info(f"Skip header rows: {skip_header_rows}")

            # Read file content
            with open(data_file_path, "r", encoding="utf-8") as f:
                content = f.read()
            lines = content.split(row_delimiter_val)
            lines = [line for line in lines if line.strip()]

            logging.info(f"Total lines before skipping headers: {len(lines)}")

            # Skip headers
            if skip_header_rows > 0:
                lines = lines[skip_header_rows:]

            logging.info(f"Total lines after skipping headers: {len(lines)}")
            if not lines:
                logging.warning("No data found to insert after skipping headers.")
                return 0

            # Parse lines into list of tuples
            rows = [tuple(line.split(field_delimiter_val)) for line in lines]

            logging.info(f"Prepared {len(rows)} rows for insertion.")

            # Perform insertion
            self.cursor.executemany(insert_query, rows)
            self.connection.commit()

            logging.info(f"Successfully inserted {len(rows)} rows into database.")
            return len(rows)

        except Exception as e:
            logging.error(f"Error inserting data from file: {e}")
            raise

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        logging.info("SQL connection closed.")

    def execute_query_data_pandas(self, query):
        """
        Executes a query with autocommit enabled and returns the results as a Pandas DataFrame.
        """
        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()

            # Suppress rowcount result sets from stored procs
            sql = f"SET NOCOUNT ON; {query}"
            cursor.execute(sql)

            # If the statement didn't return a tabular result
            if cursor.description is None:
                logging.info("No result set returned.")
                return pd.DataFrame()

            # Fetch all data and column names
            results = cursor.fetchall()
            logging.info(results)
            columns = [desc[0] for desc in cursor.description]
            logging.info(columns)

            # Ensure pandas gets a proper 2-D record set
            rows = [tuple(r) for r in results]

            # Guard against width mismatches
            if rows and len(rows[0]) != len(columns):
                row_w = len(rows[0])
                col_w = len(columns)
                logging.warning(f"Width mismatch: row={row_w} vs cols={col_w}. Trimming to {min(row_w, col_w)}.")
                width = min(row_w, col_w)
                columns = columns[:width]
                rows = [r[:width] for r in rows]

            df = pd.DataFrame.from_records(rows, columns=columns)
            logging.info(f"Successfully executed query: {query} (shape={df.shape})")
            return df

        except Exception as e:
            logging.error(f"Error executing query {query}: {e}")
            raise

    def insert_single_row(self, insert_query, params, return_identity=False):
        """
        Insert a single row using a parameterized INSERT query.
        - If return_identity=True, returns SCOPE_IDENTITY().
        - Else returns rowcount (may be -1 when SET NOCOUNT ON is in effect).
        """
        try:
            self.connection.autocommit = False
            cur = self.connection.cursor()
            cur.execute(insert_query, params)

            if return_identity:
                cur.execute("SELECT SCOPE_IDENTITY()")
                identity_val = cur.fetchone()[0]
                self.connection.commit()
                logging.info(f"Insert committed; identity={identity_val}")
                return identity_val
            else:
                affected = cur.rowcount
                self.connection.commit()
                if affected == -1:
                    logging.info("Insert committed (rowcount suppressed by NOCOUNT).")
                else:
                    logging.info(f"Inserted {affected} row(s).")
                return affected

        except Exception as e:
            try:
                self.connection.rollback()
            except Exception:
                pass
            logging.error(f"Error inserting single row: {e}")
            raise

    def insert_from_file_with_fields(
            self,
            data_file_path,
            insert_query,
            delimiter="TAB",
            row_delimiter="NEW_LINE",
            skip_header_rows=0,
            *,
            static_fields=None,  # NEW: list/tuple of up to 4 constant values to append to each row
            expected_file_cols=None,  # NEW: optional int to validate how many columns are in the file per row
            batch_size=1000  # NEW: executemany() batching for large loads
    ):
        """
        Reads a text file and inserts rows into the target SQL Server table.

        Args:
            data_file_path (str): Path to the text file.
            insert_query (str): SQL INSERT statement with placeholders (?, ?, ...).
            delimiter (str): Field delimiter: "TAB", "," or custom string.
            row_delimiter (str): "NEW_LINE", "CR_NEW_LINE", or custom string (e.g., "\n", "\r\n").
            skip_header_rows (int): Number of top rows to skip (e.g., header).
            static_fields (Sequence | None): Up to 4 constant values appended to each row (same order for every row).
            expected_file_cols (int | None): If set, validates each file row has exactly this many columns *before*
                                             static_fields are appended.
            batch_size (int): How many rows per executemany() call.

        Returns:
            int: Number of rows inserted.

        Raises:
            ValueError: if static_fields > 4, or row widths don’t match expected_file_cols.
        """
        try:
            # --- Map delimiters ---
            if delimiter == "TAB":
                field_delimiter_val = "\t"
            else:
                field_delimiter_val = delimiter

            if row_delimiter == "NEW_LINE":
                row_delimiter_val = "\n"
            elif row_delimiter == "CR_NEW_LINE":
                row_delimiter_val = "\r\n"
            else:
                row_delimiter_val = row_delimiter  # use custom as-is

            # --- Normalize static fields ---
            if static_fields is None:
                static_fields_tuple = tuple()
            elif isinstance(static_fields, (list, tuple)):
                if len(static_fields) > 4:
                    raise ValueError("static_fields may contain at most 4 values.")
                static_fields_tuple = tuple(static_fields)
            else:
                raise ValueError("static_fields must be a list/tuple or None.")

            logging.info(f"Reading file: {data_file_path}")
            logging.info(f"Field delimiter: {repr(field_delimiter_val)}")
            logging.info(f"Row delimiter: {repr(row_delimiter_val)}")
            logging.info(f"Skip header rows: {skip_header_rows}")
            logging.info(f"Static fields to append (count={len(static_fields_tuple)}): {static_fields_tuple}")

            # --- Read file content ---
            with open(data_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Split rows by the specified row delimiter and drop entirely blank lines
            lines = [line for line in content.split(row_delimiter_val) if line.strip() != ""]
            total_lines = len(lines)
            logging.info(f"Total non-blank lines before skipping headers: {total_lines}")

            # --- Skip headers if requested ---
            if skip_header_rows > 0:
                lines = lines[skip_header_rows:]
            logging.info(f"Total lines after skipping headers: {len(lines)}")
            if not lines:
                logging.warning("No data found to insert after skipping headers.")
                return 0

            # --- Build parameter rows ---
            rows = []
            line_num_offset = skip_header_rows  # for clearer error messages
            for idx, line in enumerate(lines, start=1):
                # Do not strip within fields; only remove a single trailing newline style endings defensively
                # Split into raw columns
                cols = line.split(field_delimiter_val)

                # Validate file cols if requested (before static fields)
                if expected_file_cols is not None and len(cols) != int(expected_file_cols):
                    raise ValueError(
                        f"Row {line_num_offset + idx} has {len(cols)} columns; expected {expected_file_cols}. "
                        f"Line preview: {line[:200]}"
                    )

                # Append static fields (if any)
                param_tuple = tuple(cols) + static_fields_tuple
                rows.append(param_tuple)

            logging.info(
                f"Prepared {len(rows)} rows for insertion (each row length = file_cols + {len(static_fields_tuple)}).")

            # --- Insert in batches ---
            inserted = 0
            self.connection.autocommit = False
            cur = self.connection.cursor()

            for start in range(0, len(rows), batch_size):
                batch = rows[start:start + batch_size]
                cur.executemany(insert_query, batch)
                inserted += len(batch)
                logging.info(f"Inserted batch rows {start + 1}..{start + len(batch)} (batch_size={len(batch)})")

            self.connection.commit()
            logging.info(f"Successfully inserted {inserted} rows into database.")
            return inserted

        except Exception as e:
            try:
                self.connection.rollback()
            except Exception:
                pass
            logging.error(f"Error inserting data from file: {e}")
            raise

