import sys
import requests
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from typing import List
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class RestApiManager:
    @staticmethod
    def get_string_response(
            url: str,
            headers: Optional[Dict[str, str]] = None,
            timeout: Optional[float] = None,
            params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Makes a GET request to the provided URL with optional headers, params,
        and timeout. Returns the raw response text without any modifications.
        If a 404 status code is encountered, returns "no_data_for_report" instead.

        :param url: The target URL for the GET request.
        :param headers: Optional dictionary of HTTP headers to include in the request.
        :param timeout: Time (in seconds) to wait for a response before timing out.
        :param params: Optional dictionary of URL parameters for the request.
        :return: The raw response body as a string, "no_data_for_report" if 404,
                 or re-raises other HTTP errors.
        """
        if headers is None:
            headers = {}  # No custom headers if not provided

        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Check specifically for 404 before calling raise_for_status()
            if response.status_code == 404:
                return "no_data_for_report"

            # For other 4xx/5xx, raise an HTTPError
            response.raise_for_status()
            return response.text

        except requests.exceptions.HTTPError as http_err:
            # This block will catch other 4xx/5xx errors (except 404 above)
            logging.error(f"HTTP error occurred: {http_err} (Status Code: {http_err.response.status_code})")
            raise

        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
            raise

        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Request timed out: {timeout_err}")
            raise

        except requests.exceptions.RequestException as req_err:
            logging.error(f"An error occurred: {req_err}")
            raise

    @staticmethod
    def get_previous_dates(
            num_days: int = 3,
            use_utc: bool = True,
            date_format: str = "%Y-%m-%d"
    ) -> List[str]:
        """
        Returns a list of date strings in the specified 'date_format' for
        the previous `num_days` days. By default, it uses the UTC date.
        If you set `use_utc=False`, it will use local time.

        :param num_days: Number of previous days to include (excluding today).
        :param use_utc: Whether to use UTC date (True) or local date (False).
        :param date_format: The strftime format to use when converting
                            each date to a string. Defaults to "%Y-%m-%d".

        :return: A list of formatted date strings (e.g. ["2025-02-06", "2025-02-05", "2025-02-04"]).
        """
        if use_utc:
            today = datetime.utcnow().date()
        else:
            today = datetime.today().date()

        date_list = []
        for i in range(1, num_days + 1):
            report_date = today - timedelta(days=i)
            date_list.append(report_date.strftime(date_format))

        return date_list

    @staticmethod
    def download_file_using_url(
            download_url: str,
            output_filename: str,
            headers: Optional[Dict[str, str]] = None,
            chunk_size: int = 8192
    ) -> str:
        """
        Downloads a file from the specified URL to a local path.

        :param download_url: The URL to download from.
        :param output_filename: The local filename to which the content will be saved.
        :param headers: (Optional) A dictionary of HTTP headers to send with the request.
        :param chunk_size: (Optional) Number of bytes per chunk in streaming download.
                           Default is 8192.
        :return: The path of the downloaded file (same as output_filename).
        :raises:
            requests.exceptions.HTTPError: For HTTP errors like 4xx/5xx.
            requests.exceptions.ConnectionError: For network-related errors.
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.RequestException: For any other request errors.
        """
        try:
            # Stream the download so we can write it in chunks
            response = requests.get(download_url, headers=headers, stream=True)
            response.raise_for_status()  # Raises HTTPError if status >= 400

            with open(output_filename, "wb") as file_obj:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # Filter out keep-alive chunks
                        file_obj.write(chunk)

            print(f"File downloaded successfully to: {output_filename}")
            return output_filename

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            raise

        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
            raise

        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Request timed out: {timeout_err}")
            raise

        except requests.exceptions.RequestException as req_err:
            logging.error(f"An error occurred while downloading the file: {req_err}")
            raise
