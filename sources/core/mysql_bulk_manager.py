import subprocess
import logging
import os
import csv
from io import StringIO


class MySqlImportExportManager:

    @staticmethod
    def generate_schema_dump(host, database, user, password, output_schema_file, port=3306):
        """
        MySQL does NOT have a direct equivalent of a BCP 'format file'.
        Instead, we can do a 'schema-only' dump of a specific table or the entire DB,
        which includes CREATE TABLE statements, etc.

        Args:
            host (str): MySQL server hostname/IP.
            database (str): Database name.
            user (str): MySQL username.
            password (str): MySQL password.
            output_schema_file (str): Path to save the generated .sql file.
            port (int, optional): MySQL port (default 3306).

        Returns:
            bool: True if the dump was successful, False otherwise.
        """
        try:
            dump_command = [
                "mysqldump",
                f"-h{host}",
                f"-P{port}",
                f"-u{user}",
                f"-p{password}",
                "--no-data",  # No rows, just schema
                database
            ]
            logging.info("Generating schema dump (similar to BCP format file concept)...")
            with open(output_schema_file, "w", encoding="utf-8") as outfile:
                subprocess.run(dump_command, check=True, stdout=outfile, text=True)

            logging.info(f"Schema dump saved to: {output_schema_file}")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Schema dump command failed: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return False

    @staticmethod
    def import_data_file(
            host,
            database,
            table,
            user,
            password,
            data_file_path,
            delimiter,
            port=3306,
            skip_leading_rows=0,
            row_delimiter="NEW_LINE"
    ):
        """
        Imports data from a local file into a MySQL table using LOAD DATA LOCAL INFILE.

        Args:
            host (str): MySQL server hostname/IP.
            database (str): Database name.
            table (str): Table name to import into.
            user (str): MySQL username.
            password (str): MySQL password.
            data_file_path (str): Path to the source data file.
            delimiter (str): Field delimiter (e.g., ',' or 'TAB').
            port (int, optional): MySQL port.
            skip_leading_rows (int, optional): Number of rows to skip at the top of the file.
            row_delimiter (str, optional): 'NEW_LINE' or 'CR_NEW_LINE'.

        Returns:
            int: Number of rows inserted, or -1 if error.
        """
        if delimiter == "TAB":
            delimiter = "\t"

        if row_delimiter == "NEW_LINE":
            row_delimiter_val = "\n"
        elif row_delimiter == "CR_NEW_LINE":
            row_delimiter_val= "\r\n"

        else:
            # Raise an exception if row_delimiter isn't recognized
            logging.error(f"Unknown row_delimiter: {row_delimiter}")
            raise ValueError(f"Unknown row_delimiter: {row_delimiter}")

        logging.info(f"Importing data file: {data_file_path} into {database}.{table}")
        logging.info(f"Delimiter: {delimiter}, Row Delimiter: {row_delimiter_val}, Skip Rows: {skip_leading_rows}")

        load_sql = f"""
            USE {database};
            LOAD DATA LOCAL INFILE '{data_file_path}'
            INTO TABLE {table}
            FIELDS TERMINATED BY '{delimiter}'
            LINES TERMINATED BY '{row_delimiter_val}'
            {"IGNORE " + str(skip_leading_rows) + " LINES" if skip_leading_rows > 0 else ""};
            SHOW WARNINGS;
        """

        mysql_command = [
            "mysql",
            f"-h{host}",
            f"-P{port}",
            f"-u{user}",
            f"-p{password}",
            "--local-infile=1",
            "-e", load_sql
        ]
        ##logging.info(f"mysql_command={mysql_command}")
        try:
            result = subprocess.run(
                mysql_command,
                capture_output=True,
                text=True,
                check=True
            )
            output = result.stdout
            logging.info(f"mysql import output:{output}")
            error=result.stderr
            logging.info(f"mysql import error output:{error}")
            status=result.returncode
            logging.info(f"statu output:{status}")

            rows_inserted = -1
            for line in output.splitlines():
                if "Query OK" in line and "rows affected" in line:
                    try:
                        row_str = line.split(',')[1].strip().split()[0]
                        rows_inserted = int(row_str)
                    except:
                        pass

            if rows_inserted >= 0:
                logging.info(f"Data imported successfully. Rows inserted: {rows_inserted}")
            else:
                logging.info("Data import completed, but could not parse row count.")

            return rows_inserted

        except subprocess.CalledProcessError as e:
            logging.error(f"mysql import command failed.\nSTDERR:\n{e.stderr}\nSTDOUT:\n{e.stdout}")
            return -1
        except Exception as e:
            logging.error(f"Unexpected error during import: {e}")
            return -1

    import subprocess
    import logging
    import csv
    from io import StringIO

    @staticmethod
    def query_out(host, database, query, user, password, output_file,
                  field_delimiter, port=3306, header="N", row_delimiter="NEW_LINE", expected_col_count=None):
        """
        Exports data from a custom query to a local file using the `mysql` CLI.

        Args:
            host (str): MySQL server hostname.
            database (str): Database name.
            query (str): The SQL query to export.
            user (str): MySQL username.
            password (str): MySQL password.
            output_file (str): Path to write the exported data.
            field_delimiter (str): Field delimiter (e.g., ',' or '\\t' or 'TAB').
            port (int, optional): MySQL port (default=3306).
            header (str, optional): "Y" to include column names, "N" to skip.
            row_delimiter (str, optional): Row delimiter keyword ("NEW_LINE", "CR_NEW_LINE").
            expected_col_count (int, optional): Expected number of columns in each row.

        Returns:
            int: Number of data rows exported (approx).
        """
        if field_delimiter == "TAB":
            field_delimiter = "\t"

        if row_delimiter == "NEW_LINE":
            row_delimiter_val = "\n"
        elif row_delimiter == "CR_NEW_LINE":
            row_delimiter_val = "\r\n"
        else:
            row_delimiter_val = "\n"  # fallback

        if expected_col_count is not None:
            try:
                expected_col_count = int(expected_col_count)
            except ValueError:
                logging.warning(f"Invalid expected_col_count: {expected_col_count}. Ignoring.")
                expected_col_count = None

        sql = f"USE {database}; {query}"

        mysql_command = [
            "mysql",
            f"-h{host}",
            f"-P{port}",
            f"-u{user}",
            f"-p{password}",
            "--batch"
        ]

        if header == "N":
            mysql_command.append("--skip-column-names")

        mysql_command.extend(["-e", sql])

        try:
            logging.info(f"Executing query and exporting to {output_file}:\n{query}")
            result = subprocess.run(
                mysql_command,
                capture_output=True,
                text=True,
                check=True
            )
            logging.info(f"result={result}")
            lines = result.stdout.strip().split("\n")

            with open(output_file, "w", encoding="utf-8") as f:
                for line in lines:
                    if not line.strip():
                        continue

                    reader = csv.reader(StringIO(line), delimiter="\t")
                    columns = next(reader)

                    if expected_col_count is not None:
                        if len(columns) < expected_col_count:
                            columns += [''] * (expected_col_count - len(columns))
                        elif len(columns) > expected_col_count:
                            columns = columns[:expected_col_count]

                    f.write(field_delimiter.join(columns) + row_delimiter_val)

            row_count = len([r for r in lines if r.strip()])
            if header == "Y" and row_count > 0:
                row_count -= 1

            logging.info(f"Export completed. Approx row count: {row_count}")
            return row_count

        except subprocess.CalledProcessError as e:
            logging.error(f"mysql command for query export failed:\n"
                          f"STDERR:\n{e.stderr}\nSTDOUT:\n{e.stdout}")
            return 0
        except Exception as e:
            logging.error(f"Unexpected error in query_out: {e}")
            return 0
