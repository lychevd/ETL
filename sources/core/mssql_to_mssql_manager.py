import pyodbc
import logging

class MSSQLToMSSQLBridge:
    def __init__(self,
                 mssql_host_source, mssql_db_source, mssql_user_source, mssql_password_source, mssql_port_source,
                 mssql_host_destination, mssql_db_destination, mssql_user_destination, mssql_password_destination, mssql_port_destination,
                 mssql_driver="{ODBC Driver 17 for SQL Server}"):
        """
        Initializes connections to both source and destination SQL Server instances.

        Args:
            *_source: Connection details for source SQL Server
            *_destination: Connection details for destination SQL Server
        """
        try:
            self.source_conn = pyodbc.connect(
                f"DRIVER={mssql_driver};SERVER={mssql_host_source},{mssql_port_source};"
                f"DATABASE={mssql_db_source};UID={mssql_user_source};PWD={mssql_password_source}"
            )
            self.source_cursor = self.source_conn.cursor()
            logging.info("Connected to source SQL Server.")
        except Exception as e:
            logging.error(f"Source SQL Server connection failed: {e}")
            raise

        try:
            self.dest_conn = pyodbc.connect(
                f"DRIVER={mssql_driver};SERVER={mssql_host_destination},{mssql_port_destination};"
                f"DATABASE={mssql_db_destination};UID={mssql_user_destination};PWD={mssql_password_destination}"
            )
            self.dest_cursor = self.dest_conn.cursor()
            logging.info("Connected to destination SQL Server.")
        except Exception as e:
            logging.error(f"Destination SQL Server connection failed: {e}")
            raise

    def transfer_query_results(self, source_query, destination_insert_query):
        """
        Executes a SELECT query on source MSSQL and inserts the results into destination MSSQL.

        Args:
            source_query (str): SELECT query to execute on source SQL Server.
            destination_insert_query (str): INSERT INTO query with placeholders (?, ?, ...) for destination SQL Server.
        """
        try:
            self.source_cursor.execute(source_query)
            rows = self.source_cursor.fetchall()
            row_count = len(rows)
            logging.info(f"source_query={source_query}, destination_insert_query={destination_insert_query}")
            ##logging.info(f"rows={rows}")

            if row_count == 0:
                logging.info("No rows to transfer.")
                return 0

            self.dest_cursor.executemany(destination_insert_query, rows)
            self.dest_conn.commit()

            logging.info(f"Transferred {row_count} rows to destination SQL Server.")
            return row_count

        except Exception as e:
            logging.error(f"Error during data transfer: {e}")
            raise


    def close_connections(self):
        """Closes both source and destination database connections."""
        try:
            self.source_cursor.close()
            self.source_conn.close()
            self.dest_cursor.close()
            self.dest_conn.close()
            logging.info("Closed all SQL Server connections.")
        except Exception as e:
            logging.warning(f"Error closing connections: {e}")
