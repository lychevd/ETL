import mysql.connector
import logging

class MySQLClient:
    def __init__(self, host, database, username, password, port=3306):
        """
        Initializes a connection to the MySQL database.

        Args:
            host (str): MySQL server hostname or IP.
            database (str): Target database name.
            username (str): MySQL login username.
            password (str): MySQL login password.
            port (int): TCP port (default 3306 for MySQL).
        """
        try:
            self.connection = mysql.connector.connect(
                host=host,
                database=database,
                user=username,
                password=password,
                port=port
            )
            self.cursor = self.connection.cursor()
            logging.info(f"Successfully connected to MySQL at {host}:{port}!")
        except Exception as e:
            logging.error(f"Error connecting to MySQL at {host}:{port}: {e}")
            raise

    def execute_query_autocommit(self, query, data_return):
        """
        Executes a given SQL query with autocommit enabled.

        Args:
            query (str): SQL query to execute.
            data_return (str): 'Y' if you want to fetch results (SELECT),
                               otherwise 'N' for non-SELECT queries.

        Returns:
            list or None:
                - List of rows (tuples) if data_return == 'Y'.
                - None if data_return == 'N'.
        """
        try:
            # Enable autocommit on the connection
            self.connection.autocommit = True

            # Execute the query
            self.cursor.execute(query)

            if data_return == "Y":
                results = self.cursor.fetchall()
                logging.info(f"Successfully executed query: {query}")
                return results
            else:
                logging.info(f"Successfully executed query: {query}")
                return None
        except Exception as e:
            logging.error(f"Error executing query {query}: {e}")
            raise

    def close_connection(self):
        """
        Closes the database connection.
        """
        try:
            if self.cursor is not None:
                self.cursor.close()
            if self.connection.is_connected():
                self.connection.close()
            logging.info("MySQL connection closed.")
        except Exception as e:
            logging.error(f"Error closing MySQL connection: {e}")

    def export_query_to_file(self, query, output_file, field_delimiter="\t", row_delimiter="\n", header="N"):
        """
        Executes a query and writes the result to a file with specified delimiters.

        Args:
            query (str): The SQL SELECT query to execute.
            output_file (str): The path of the output file.
            field_delimiter (str): Delimiter between fields (default tab).
            row_delimiter (str): Delimiter between rows (default newline).
            header (str): "Y" to include column headers, "N" to omit.

        Returns:
            int: Number of data rows written (excluding header).
        """
        try:
            if field_delimiter == "TAB":
                field_delimiter = "\t"

            if row_delimiter == "NEW_LINE":
                row_delimiter_val = "\n"
            elif row_delimiter == "CR_NEW_LINE":
                row_delimiter_val = "\r\n"
            else:
                row_delimiter_val = "\n"  # fallback
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            col_names = [desc[0] for desc in self.cursor.description]

            with open(output_file, "w", encoding="utf-8") as f:
                if header == "Y":
                    f.write(field_delimiter.join(col_names) + row_delimiter_val)

                for row in rows:
                    cleaned_row = [
                        str(col).replace("\n", " ").replace("\r", " ") if col is not None else "" for col in row
                    ]
                    f.write(field_delimiter.join(cleaned_row) + row_delimiter_val)

            return len(rows)

        except Exception as e:
            logging.error(f"Failed to export query results: {e}")
            raise


