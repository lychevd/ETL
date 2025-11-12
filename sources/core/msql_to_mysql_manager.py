import mysql.connector
import pyodbc
import logging

class MSSQLToMySQLBridge:
    def __init__(self,
                 mysql_host, mysql_db, mysql_user, mysql_password, mysql_port,
                 mssql_host, mssql_db, mssql_user, mssql_password, mssql_port=1433,
                 mssql_driver="{ODBC Driver 17 for SQL Server}"):
        """
        Initializes connections to both SQL Server (source) and MySQL (destination).

        Args:
            mysql_*: MySQL destination connection params
            mssql_*: SQL Server source connection params
        """
        # Connect to MySQL (destination)
        try:
            self.mysql_conn = mysql.connector.connect(
                host=mysql_host,
                database=mysql_db,
                user=mysql_user,
                password=mysql_password,
                port=mysql_port
            )
            self.mysql_cursor = self.mysql_conn.cursor()
            logging.info("Connected to MySQL (destination).")
        except Exception as e:
            logging.error(f"MySQL connection failed: {e}")
            raise

        # Connect to SQL Server (source)
        try:
            self.mssql_conn = pyodbc.connect(
                f"DRIVER={mssql_driver};SERVER={mssql_host},{mssql_port};"
                f"DATABASE={mssql_db};UID={mssql_user};PWD={mssql_password}"
            )
            self.mssql_cursor = self.mssql_conn.cursor()
            logging.info("Connected to SQL Server (source).")
        except Exception as e:
            logging.error(f"SQL Server connection failed: {e}")
            raise

    def transfer_query_results(self, mssql_query, mysql_insert_query):
        """
        Executes a SELECT query on MSSQL and inserts the results into MySQL.

        Args:
            mssql_query (str): SELECT query to execute on MSSQL.
            mysql_insert_query (str): INSERT INTO query with placeholders (%s, %s, ...) for MySQL.
        """
        try:
            self.mssql_cursor.execute(mssql_query)
            rows = self.mssql_cursor.fetchall()
            row_count = len(rows)
            logging.info(f"mssql_query={mssql_query}, mysql_insert_query={mysql_insert_query}")
            ##logging.info(f"rows={rows}")

            if row_count == 0:
                logging.info("No rows to transfer.")
                return 0

            self.mysql_cursor.executemany(mysql_insert_query, rows)
            self.mysql_conn.commit()

            logging.info(f"Transferred {row_count} rows to MySQL.")
            return row_count

        except Exception as e:
            logging.error(f"Error during data transfer: {e}")
            raise

    def close_connections(self):
        """Closes both database connections."""
        try:
            self.mssql_cursor.close()
            self.mssql_conn.close()
            self.mysql_cursor.close()
            self.mysql_conn.close()
            logging.info("Closed all database connections.")
        except Exception as e:
            logging.warning(f"Error closing connections: {e}")
