import json

from google.cloud import secretmanager
from google.auth import default
import logging
import pandas as pd
from collections import namedtuple

logger = logging.getLogger(__name__)


class SecretManager:
    def __init__(self):
        _, project_id = default()  # to ensure program only works on default shared infra projects.
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()

    def fetch_secret(self, secret_name):
        """Fetches a secret from Google Secret Manager."""
        try:
            secret_version_name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
            response = self.client.access_secret_version(name=secret_version_name)
            secret_payload = response.payload.data.decode("UTF-8")
            logging.info(f"Successfully fetched secret: {secret_name}")
            return secret_payload
        except Exception as e:
            logging.error(f"Error fetching secret {secret_name}: {e}")
            raise

    @staticmethod
    def write_secret_to_file(secret_payload, output_file_name):
        """Writes secret payload to a file."""
        try:
            with open(output_file_name, "w") as file:
                file.write(secret_payload)
            logging.info(f"Secret written to file: {output_file_name}")
        except Exception as file_error:
            logging.error(f"Error writing secret to file {output_file_name}: {file_error}")
            raise

    def get_variable_value(self, key, variables, from_secret_list):
        value = variables.loc[variables['key'] == key, 'value'].values[0]

        if key in from_secret_list and value:
            logging.info(f"Fetching secret for key: {key}")
            secret_payload = self.fetch_secret(value)
            ##logging.info(f"secret_payload={secret_payload}")

            try:
                # Try to parse secret as JSON
                secret_data = json.loads(secret_payload)

                if isinstance(secret_data, dict):
                    for k, v in secret_data.items():
                        if not variables[variables['key'] == k].empty:
                            variables.loc[variables['key'] == k, 'value'] = v
                        else:
                            variables.loc[len(variables.index)] = [k, v]
                    return variables.loc[variables['key'] == key, 'value'].values[0]
                else:
                    return secret_payload

            except json.JSONDecodeError:
                # Not JSON, return raw secret
                return secret_payload

        return value

    def get_meta_connection_from_secret(self,secret_name):
        """
        Fetches a secret by name, parses it as JSON, and returns a connection-like object.

        :param secret_name: The name of the secret in Secret Manager
        :param fetch_secret_func: A function that returns the raw secret string
        :return: namedtuple('MetaConnection', ['mysql_host', 'mysql_user', 'mysql_password', 'mysql_database'])
        """
        secret_payload = self.fetch_secret(secret_name)
        ##logging.info(f"Fetched secret payload for '{secret_name}': {secret_payload}")

        try:
            secret_data = json.loads(secret_payload)

            expected_keys = ["mysql_host", "mysql_user", "mysql_password", "mysql_database"]
            missing_keys = [k for k in expected_keys if k not in secret_data]
            if missing_keys:
                raise KeyError(f"Missing expected keys in secret: {missing_keys}")

            MetaConnection = namedtuple("MetaConnection", expected_keys)
            return MetaConnection(
                mysql_host=secret_data["mysql_host"],
                mysql_user=secret_data["mysql_user"],
                mysql_password=secret_data["mysql_password"],
                mysql_database=secret_data["mysql_database"]
            )

        except json.JSONDecodeError:
            raise ValueError("Secret payload is not valid JSON")
