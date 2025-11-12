import os
import logging
import pandas as pd
from pathlib import Path

class LocalUtil:
    def __init__(self):
        """
        Initializes the LocalUtil class.
        This can be used for managing local directory operations.
        """
        logging.info("LocalUtil initialized.")

    def clean_local_file(self, file_path):
        """
        Removes all files from the specified local directory while keeping the directory structure intact.

        Args:
            file_path (str): The path of the directory to delete.

        Returns:
            None
        """
        try:
            if not os.path.exists(file_path):
                logging.warning(f"File not found: {file_path}. Skipping cleanup.")
                return

            try:
                os.remove(file_path)
                logging.info(f"Removed file: {file_path}")
            except Exception as e:
                logging.error(f"Error removing file {file_path}: {e}")

        except Exception as e:
            logging.error(f"Error in clean_local_file: {str(e)}")
            raise

    def clean_local_directory(self, directory_path):
        """
        Removes all files from the specified local directory while keeping the directory structure intact.

        Args:
            directory_path (str): The path of the directory to clean.

        Returns:
            None
        """
        try:
            if not os.path.exists(directory_path):
                logging.warning(f"Directory not found: {directory_path}. Skipping cleanup.")
                return

            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logging.info(f"Removed file: {file_path}")
                    except Exception as e:
                        logging.error(f"Error removing file {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error cleaning directory {directory_path}: {e}")

    def remove_header_from_csv(self, input_file, output_file):
        """
        Removes the header row from a CSV file and writes the result to a new file.

        Args:
            input_file (str): Path to the input file with a header.
            output_file (str): Path to the output file without the header.

        Returns:
            None
        """
        try:
            if not os.path.exists(input_file):
                logging.error(f"Input file does not exist: {input_file}")
                return

            with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
                next(infile)  # Skip the header row
                for line in infile:
                    outfile.write(line)

            logging.info(f"Header removed and content written to {output_file}")
        except FileNotFoundError:
            logging.error(f"File not found: {input_file}")
        except Exception as e:
            logging.error(f"Error processing file {input_file}: {e}")

    def collect_local_file_metadata(self, file_path, row_count):
        """
        Collects metadata for a local file including size, last modified time, and other details.

        Args:
            file_path (str): Path to the local file.
            row_count (int): Number of rows in the file.

        Returns:
            dict: Metadata about the file.
        """
        try:
            if not os.path.exists(file_path):
                logging.error(f"File does not exist: {file_path}")
                return None

            filename_base = os.path.basename(file_path)
            directory = os.path.dirname(file_path)

            file_size = os.path.getsize(file_path)
            file_last_modified = int(os.path.getmtime(file_path))

            file_metadata = {
                'filename_base': filename_base,
                'filename_full': file_path,
                'file_size': str(file_size),
                'file_last_modified': str(file_last_modified),
                'local_directory': directory,
                'file_row_count': row_count
            }

            logging.info(f"File metadata collected: {file_metadata}")
            return file_metadata
        except Exception as e:
            logging.error(f"Error collecting metadata for file {file_path}: {e}")
            return None

    def convert_txt_to_parquet_auto_ext(self,txt_file_path, delimiter=",", header="infer"):
        """
        Reads a delimited text file and saves it as a Parquet file in the same folder,
        replacing the input file's extension with .parquet.

        Args:
            txt_file_path (str): Path to the input text file.
            delimiter (str, optional): Field delimiter (default=',').
            header (int, list of int, or 'infer', optional):
                - 'infer' to use the first line as column names.
                - None if no header (the first line is data).
                - Or an integer specifying the header row.
                Default is 'infer'.

        Returns:
            str: The path to the newly created Parquet file.
        """
        # Convert to Path object
        input_path = Path(txt_file_path)

        # Construct the output path by replacing the extension with ".parquet"
        # If input file is "data.csv", output will be "data.parquet".
        # If there's no extension, ".parquet" is simply appended.
        output_parquet_path = input_path.with_suffix(".parquet")

        # Read the CSV-like text file into a DataFrame
        df = pd.read_csv(input_path, sep=delimiter, header=header)

        # Write DataFrame to Parquet
        df.to_parquet(output_parquet_path, index=False)

        logging.info(f"Successfully converted '{txt_file_path}' to '{output_parquet_path}'")
        return str(output_parquet_path)

