import logging
import socket
import ssl
import requests
import pandas as pd
import pymssql  # <— NEW

class SQLClient_PYmsql:
    @staticmethod
    def check_port(host, port, timeout=5):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                logging.info(f"Port check OK: {host}:{port}")
                return True
        except (socket.timeout, socket.error):
            # logging at INFO here to avoid noisy logs in infra checks
            logging.info(f"Port check failed: {host}:{port}")
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
            # Private/local IP
            hostname = socket.gethostname()
            private_ip = socket.gethostbyname(hostname)
            logging.info(f"Private IP (local): {private_ip}")
        except Exception as e:
            private_ip = None
            logging.error(f"Error detecting private IP: {e}")

        try:
            # Public IP
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

        # Optional TLS check (works only if the server speaks TLS on that port)
        # self.check_tls_version(server, self.port)

        try:
            # Important: pymssql parameter names are explicit; no ODBC driver string.
            # encrypt=True relies on FreeTDS capabilities and server settings.
            # You can also pass tds_version='7.4' if your environment needs it.
            self.connection = pymssql.connect(
                server=self.server,
                user=self.username,
                password=self.password,
                database=self.database,
                port=self.port,
                login_timeout=5,
                timeout=0,           # 0 = no statement timeout; set seconds if you want a limit
                as_dict=False,       # keep tuples; we’ll map columns separately
                charset='UTF-8'
            )
            self.cursor = self.connection.cursor()
            logging.info(f"Successfully connected to SQL Server {server}:{port} with pymssql!")
        except Exception as e:
            logging.error(f"Error connecting to SQL Server {server}:{port} via pymssql: {e}")
            raise

    def execute_query_autocommit(self, query, data_return):
        """
        Executes a T-SQL batch with autocommit ON.
        Note: With pymssql, autocommit affects transaction behavior; result handling is the same.
        """
        try:
            self.connection.autocommit(True)
            cursor = self.connection.cursor()
            cursor.execute(query)

            if data_return == "Y":
                results = cursor.fetchall()
                logging.info(f"Successfully executed query: {query}")
                return results
            else:
                logging.info(f"Successfully executed query (no return): {query}")
                return None
        except Exception as e:
            logging.error(f"Error executing query {query}: {e}")
            raise
        finally:
            # Return autocommit to default OFF for other methods that manage transactions
            self.connection.autocommit(False)

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

        NOTE for pymssql:
        - Use %s placeholders in insert_query (NOT '?').
        """
        try:
            # Map delimiters
            field_delimiter_val = "\t" if delimiter == "TAB" else delimiter
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

            with open(data_file_path, "r", encoding="utf-8") as f:
                content = f.read()

            lines = [line for line in content.split(row_delimiter_val) if line.strip()]
            logging.info(f"Total lines before skipping headers: {len(lines)}")

            if skip_header_rows > 0:
                lines = lines[skip_header_rows:]
            logging.info(f"Total lines after skipping headers: {len(lines)}")

            if not lines:
                logging.warning("No data found to insert after skipping headers.")
                return 0

            # Prepare rows for executemany
            rows = [tuple(line.split(field_delimiter_val)) for line in lines]
            logging.info(f"Prepared {len(rows)} rows for insertion.")

            cur = self.connection.cursor()
            cur.executemany(insert_query, rows)  # placeholders must be %s
            self.connection.commit()

            logging.info(f"Successfully inserted {len(rows)} rows into database.")
            return len(rows)

        except Exception as e:
            try:
                self.connection.rollback()
            except Exception:
                pass
            logging.error(f"Error inserting data from file: {e}")
            raise

    def close_connection(self):
        try:
            self.cursor.close()
        finally:
            self.connection.close()
        logging.info("SQL connection closed.")

    def execute_query_data_pandas(self, query):
        """
        Executes a query with autocommit enabled and returns the results as a Pandas DataFrame.
        Mirrors your pyodbc behavior, including SET NOCOUNT ON.
        """
        try:
            self.connection.autocommit(True)
            cursor = self.connection.cursor()

            sql = f"SET NOCOUNT ON; {query}"
            cursor.execute(sql)

            if cursor.description is None:
                logging.info("No result set returned.")
                return pd.DataFrame()

            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            rows = [tuple(r) for r in results]

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
        finally:
            self.connection.autocommit(False)

    def insert_single_row(self, insert_query, params, return_identity=False):
        """
        Insert a single row using a parameterized INSERT query (use %s placeholders).
        - If return_identity=True, returns SCOPE_IDENTITY() as INT/BIGINT.
        - Else returns rowcount (may be -1 when NOCOUNT is ON).
        """
        try:
            self.connection.autocommit(False)
            cur = self.connection.cursor()
            cur.execute(insert_query, params)

            if return_identity:
                # Cast avoids Decimal and ensures a Python int
                cur.execute("SELECT CAST(SCOPE_IDENTITY() AS BIGINT)")
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
            logging.error(f"Error inserting single row via pymssql: {e}")
            raise
