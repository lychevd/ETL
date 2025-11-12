
import mysql.connector
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.connection.autocommit = True  # Autocommit for simplicity
            logging.info("MySQL connected.")
        except Exception as e:
            logging.error(f"MySQL connection error: {e}")
            raise

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL disconnected.")

    def _execute_procedure(self, procedure_name, params):
        """Executes a stored procedure."""
        try:
            with self.connection.cursor() as cursor:
                cursor.callproc(procedure_name, params)
                results = []  # Initialize results list to handle multiple result sets
                for result in cursor.stored_results():
                    results.extend(result.fetchall())
                return results
        except Exception as e:
            logging.error(f"Error executing procedure {procedure_name}: {e}")
            raise

    def start_workflow_step_log(self, workflow_name, step_name, log_id, additional_param):
        """Starts a workflow step log by calling a stored procedure."""
        try:
            params = (workflow_name, step_name, log_id, additional_param, "SET")
            results = self._execute_procedure("usp_StartWorkflowStepLog",
                                              params)  # Assuming usp_StartWorkflowStepLog exists
            df = pd.DataFrame(results, columns=['key', 'value'])
            logging.info(f"Started workflow step log for {workflow_name} - {step_name}")
            return df
        except Exception as e:
            logging.error(f"Error starting workflow step log: {e}")
            raise

    def create_dataset_instance(self, log_id, step_log_id, step_name, record_type, item):
        """Creates a dataset instance by calling stored procedure."""
        try:
            params = (log_id, step_log_id, step_name, record_type, json.dumps(item))
            logging.info(f"Creating dataset instance: {params}")
            return self._execute_procedure("usp_CreateDataSetsInstance", params)
        except Exception as e:
            logging.error(f"Error creating dataset instance: {e}")
            raise

    def close_step_log(self, workflow_name, step_name, log_id, step_log_id, status, message):
        """Closes the step log by updating its status and message."""
        try:
            params = (workflow_name, step_name, log_id, step_log_id, status, message)
            self._execute_procedure("usp_SetWorkflowStepLog", params)  # Assuming usp_SetWorkflowStepLog exists
            logging.info(f"Closed step log with status: {status}, message: {message}")
        except Exception as e:
            logging.error(f"Error closing step log: {e}")
            raise

    def select_items_to_process_by_list(self, items, step_name, log_id, step_log_id):
        """Selects items to process by passing a list to a stored procedure."""
        try:
            json_items = json.dumps(items)
            params = (json_items, step_name, log_id, step_log_id)
            result = self._execute_procedure("usp_process_items_by_json_array", params)
            if result and result[0] and result[0][0]:  # Check if any result is returned
                return json.loads(result[0][0])  # Return parsed JSON data
            return None  # Explicitly handle the case of no results, as it's a valid scenario
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from stored procedure result: {e}")
            raise
        except Exception as e:
            logging.error(f"Error selecting items to process by list: {e}")
            raise

    def select_items_to_process_by_records(self, log_id, step_log_id, step_name):
        """Selects items to process based on database records."""
        try:
            params = (log_id, step_log_id, step_name)
            result = self._execute_procedure("usp_SelectItemsToProcess", params)
            if result and result[0] and result[0][0]:
                return json.loads(result[0][0])
            return None  # Explicitly handle the case of no results, as it's a valid scenario
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from stored procedure result: {e}")
            raise
        except Exception as e:
            logging.error(f"Error selecting items to process by records: {e}")
            raise
