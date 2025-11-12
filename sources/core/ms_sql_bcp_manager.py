import subprocess
import logging
import os
import pyodbc

class MsSqlBcpManager:
    @staticmethod
    def generate_bcp_format_file(server, database, table, user, password, output_format_file, port=None):
        """
        Generates a BCP format file for a given table in an SQL Server database.

        Args:
            server (str): SQL Server name or IP.
            database (str): Database name.
            table (str): Table name to generate the format file.
            user (str): SQL Server username.
            password (str): SQL Server password.
            output_format_file (str): Path to save the generated format file.
            port (int, optional): Port number for the SQL Server.

        Returns:
            bool: True if the format file was generated successfully, False otherwise.
        """
        try:
            server_with_port = f"{server},{port}" if port else server
            bcp_command = [
                "bcp", f"{database}.dbo.{table}", "format", "nul",
                "-c", "-f", output_format_file, "-S", server_with_port, "-U", user, "-P", password
            ]
            logging.info("Generating BCP format file...")
            subprocess.run(bcp_command, check=True)
            logging.info(f"Format file generated successfully: {output_format_file}")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"BCP command failed: {e.stderr}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return False

    @staticmethod
    def import_data_file(server, database, table, user, password, data_file_path,
                         delimiter, port, ms_sql_schema, row_delimiter="NEW_LINE", skip_header_rows=0):
        """
        Imports data from a file into a SQL Server table using BCP.

        Args:
            server (str): SQL Server name or IP.
            database (str): Database name.
            table (str): Table name to import data into.
            user (str): SQL Server username.
            password (str): SQL Server password.
            data_file_path (str): Path to the data file to import.
            delimiter (str): Field delimiter in the data file.
            port (int): Port number for the SQL Server.
            ms_sql_schema (str): Schema name.
            row_delimiter (str): Row delimiter keyword ("NEW_LINE", "CR_NEW_LINE", "CUSTOM").
            skip_header_rows (int): Number of header rows to skip (default 0).

        Returns:
            int: Number of rows inserted, or -1 if an error occurs.
        """
        try:
            if delimiter == "TAB":
                delimiter = r"\t"

            if row_delimiter == "NEW_LINE":
                row_delimiter_val = r"\n"
            elif row_delimiter == "CR_NEW_LINE":
                row_delimiter_val = r"\r\n"
            else:
                row_delimiter_val = row_delimiter  # use custom value as-is

            logging.info(f"delimiter={delimiter}")
            logging.info(f"row_delimiter={row_delimiter_val}")
            logging.info(f"data_file_path={data_file_path}")
            logging.info(f"table={table}")
            logging.info(f"database={database}")
            logging.info(f"server={server}")
            logging.info(f"user={user}")

            server_with_port = f"{server},{port}" if port else server
            bcp_command = [
                "bcp", f"{database}.{ms_sql_schema}.{table}", "in", data_file_path,
                "-c", "-t", delimiter, "-r", row_delimiter_val,
                "-S", server_with_port, "-U", user, "-P", password
            ]

            # Add header skip option
            if skip_header_rows > 0:
                first_row = skip_header_rows + 1
                bcp_command.extend(["-F", str(first_row)])

            logging.info(f"Executing BCP command: {' '.join(bcp_command)}")
            result = subprocess.run(bcp_command, capture_output=True, text=True, check=True)
            output = result.stdout
            logging.info(f"output={output}")

            rows_inserted = -1
            for line in output.splitlines():
                if "rows copied" in line:
                    rows_inserted = int(line.split()[0])
                    break

            logging.info(f"Data imported successfully. Rows inserted: {rows_inserted}")
            return rows_inserted

        except subprocess.CalledProcessError as e:
            logging.error(f"BCP command failed: stderr={e.stderr}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

    @staticmethod
    def bcp_query_out(server, database, query, user, password, output_file, delimiter, row_delimiter, port=None):
        """
        Exports data using BCP with a custom query (queryout), field and row delimiters.

        Args:
            server (str): SQL Server name or IP.
            database (str): Database name.
            query (str): Custom SQL query to export data.
            user (str): SQL Server username.
            password (str): SQL Server password.
            output_file (str): Path to save the exported data.
            delimiter (str): Field delimiter (e.g., '|', ',', 'TAB').
            row_delimiter (str): Row delimiter: 'LF', 'CRLF', or 'CR'.
            port (int, optional): Port number for SQL Server.

        Returns:
            int: Number of rows exported, or 0 if none.
        """

        try:
            # Handle field delimiter
            delimiter_map = {
                "TAB": '\t'
            }
            delimiter_clean = delimiter_map.get(delimiter.upper(), delimiter)

            # Handle row delimiter
            row_delimiter_map = {
                "CRLF": '\r\n',
                "LF": '\n',
                "CR": '\r'
            }
            row_delimiter_clean = row_delimiter_map.get(row_delimiter.upper(), '\n')  # default to LF

            server_with_port = f"{server},{port}" if port else server

            bcp_command = [
                "bcp",
                query,
                "queryout",
                output_file,
                "-c",
                "-t", delimiter_clean,
                "-r", row_delimiter_clean,
                "-S", server_with_port,
                "-U", user,
                "-P", password
            ]

            if database:
                bcp_command.extend(["-d", database])

            logging.info(f"Executing BCP command: {' '.join(bcp_command)}")

            result = subprocess.run(bcp_command, capture_output=True, text=True, check=True)
            output = result.stdout.strip()

            logging.info(f"BCP command output:\n{output}")

            rows_exported = 0
            for line in output.splitlines():
                if "rows copied" in line.lower():
                    try:
                        rows_exported = int(line.split()[0])
                    except (IndexError, ValueError):
                        pass
                    break

            logging.info(f"BCP export completed. Rows exported: {rows_exported}")
            return rows_exported

        except subprocess.CalledProcessError as e:
            logging.error(f"BCP command failed:\nstderr: {e.stderr}\nstdout: {e.stdout}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in bcp_query_out: {e}")
            raise

    @staticmethod
    def bcp_query_out_header(server, database, query, user, password, output_file, delimiter, row_delimiter,
                             port=None, mode='no_header', header_query=None, header_string=None):
        """
        Export SQL Server data using BCP with optional header handling.

        Modes:
            - 'no_header': data only
            - 'query_header': header from separate SQL query
            - 'static_header': provided header string

        Returns:
            int: number of rows exported
        """

        try:
            # Normalize delimiters
            delimiter_map = {"TAB": '\t'}
            delimiter_clean = delimiter_map.get(delimiter.upper(), delimiter)

            row_delimiter_map = {"CRLF": '\r\n', "LF": '\n', "CR": '\r'}
            row_delimiter_clean = row_delimiter_map.get(row_delimiter.upper(), '\n')

            server_with_port = f"{server},{port}" if port else server
            temp_output_file = output_file + ".tmp"

            # Run BCP command
            bcp_command = [
                "bcp", query, "queryout", temp_output_file,
                "-c", "-t", delimiter_clean, "-r", row_delimiter_clean,
                "-S", server_with_port, "-U", user, "-P", password, "-d", database
            ]

            logging.info(f"Running: {' '.join(bcp_command)}")
            result = subprocess.run(bcp_command, capture_output=True, text=True, check=True)
            logging.info(f"BCP stdout:\n{result.stdout.strip()}")

            # Parse row count
            rows_exported = 0
            for line in result.stdout.strip().splitlines():
                if "rows copied" in line.lower():
                    try:
                        rows_exported = int(line.split()[0])
                    except Exception:
                        pass

            # Prepare header string if needed
            header_line = ""
            if mode == "query_header":
                if not header_query:
                    raise ValueError("header_query required for 'query_header' mode")
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server_with_port};DATABASE={database};UID={user};PWD={password}"
                )
                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute(header_query)
                    cols = [desc[0] for desc in cursor.description]
                    header_line = delimiter_clean.join(cols)
            elif mode == "static_header":
                if not header_string:
                    raise ValueError("header_string required for 'static_header' mode")
                header_line = header_string.strip()
            logging.info(f"header_line={header_line}")
            logging.info(f"mode={mode}")
            logging.info(f"header_query={header_query}")
            # Write final file
            if mode == "no_header":
                os.rename(temp_output_file, output_file)
            else:
                with open(output_file, "w", encoding="utf-8", newline="") as f_out:
                    # Write header
                    f_out.write(header_line + row_delimiter_clean)

                    # Append BCP data — log first row
                    with open(temp_output_file, "r", encoding="utf-8") as f_in:
                        for i, line in enumerate(f_in):
                            clean_line = line.rstrip('\r\n')
                            if i == 0:
                                logging.info(f"First data row: {clean_line}")
                            f_out.write(clean_line + row_delimiter_clean)

                os.remove(temp_output_file)

            return rows_exported

        except subprocess.CalledProcessError as e:
            logging.error(f"BCP failed:\nSTDERR: {e.stderr}\nSTDOUT: {e.stdout}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in bcp_query_out: {e}")
            raise
