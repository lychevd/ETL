from google.cloud import storage
import logging
import os
import tempfile
import json
import math
import logging
from uuid import uuid4

from pyjsparser.parser import false

logger = logging.getLogger(__name__)


class GCSManager:
    def __init__(self, service_account_path=None, project=None):
        """
        Initializes a Google Cloud Storage client based on provided parameters:

        1. If service_account_path is provided (and optionally project),
           creates a client authenticated with that service account and sets the project if provided.
        2. If only project is provided (no service_account_path),
           uses default credentials but switches to the given project.
        3. If neither is provided, uses default credentials and default project settings.
        """
        from google.cloud import storage

        if service_account_path is not None:
            # Use service account JSON file
            self.client = storage.Client.from_service_account_json(service_account_path, project=project)
            logging.info(f"Storage client created with service account: {service_account_path}")
        elif project is not None:
            # No service account provided, use default credentials with specified project
            self.client = storage.Client(project=project)
            logging.info(f"Storage client created with default credentials for project: {project}")
        else:
            # No service account and no project specified, use default credentials and default project
            self.client = storage.Client()
            logging.info("Storage client created with default credentials and default project.")

    def list_files_with_properties(self, bucket_name, prefix=""):
        """Lists GCS files and their properties."""

        files_properties = []
        try:
            bucket = self.client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)

            for blob in blobs:
                if blob.name.endswith('/') or blob.name.startswith(("Done/", "data/diservices/Done/")):
                    continue  # Skip directories and excluded prefixes

                blob.reload()

                # Split on the rightmost slash
                parts = blob.name.rsplit("/", 1)

                # If there's at least one slash in the blob name
                if len(parts) == 2:
                    directory_part = parts[0] + "/"  # e.g. 'directory/diservices/exp/'
                    filename_base = parts[1]  # e.g. 'someFile.txt'
                else:
                    # If there's no slash, the entire name is the file name
                    directory_part = ""
                    filename_base = blob.name

                files_properties.append({
                    'filename_base': filename_base,
                    'filename_full': blob.name,
                    'file_size': str(blob.size) if blob.size is not None else "0",
                    'file_last_modified': str(int(blob.updated.timestamp())) if blob.updated else "0",
                    'gs_bucket_name': bucket_name,
                    'gs_directory': directory_part  # everything before the filename, including trailing slash
                })

                logging.info(f"Found file: {blob.name}")

        except Exception as e:
            logging.error(f"Error listing files in bucket {bucket_name} with prefix {prefix}: {e}")
            raise
        return files_properties

    def download_file(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)
            logging.info(f"File {source_blob_name} downloaded to {destination_file_name}.")
        except Exception as e:
            logging.error(f"Error downloading file {source_blob_name}: {e}")
            raise

    def move_file(self, source_bucket, source_blob_name, destination_bucket, destination_blob_name):
        """Moves a file between buckets or within a bucket."""

        try:
            source_bucket = self.client.bucket(source_bucket)
            source_blob = source_bucket.blob(source_blob_name)
            destination_bucket = self.client.bucket(destination_bucket)

            new_blob = source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name
            )
            source_blob.delete()  # Delete the original file after successful move

            logging.info(f"File {source_blob_name} moved to {destination_blob_name}")

        except Exception as e:
            logging.error(f"Error moving file {source_blob_name}: {e}")
            raise

    def delete_file(self, bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            logging.info(f"File {blob_name} deleted.")
        except Exception as e:
            logging.error(f"Error deleting file {blob_name}: {e}")
            raise

    def merge_files_in_gcs(
            self,
            bucket_name,
            prefix,
            destination_blob_name,
            header_line,
            print_header,
            row_delimiter,
            pad_need,
            offset: int,
            pad_char: str,
            left_count: int,
            right_count: int,
            drop_temp_dir="N",
            split_count=1


    ):
        """
        Merges all files under a given GCS prefix into either a single file
        or multiple split files, depending on split_count.

        If split_count <= 1:
          - Writes a single merged file named exactly as `destination_blob_name`.
        If split_count > 1:
          - Treats `destination_blob_name` as a Python .format template,
            with placeholders {part_index}, {total_parts}, {prefix} to produce multiple files.

        After uploading each file, constructs a `json_data_successor` dict with these keys:
            'filename_base'
            'filename_full'
            'file_size'
            'file_last_modified'
            'gs_bucket_name'
            'gs_directory'
            'file_row_count'

        Returns:
            list[dict]: One metadata dictionary per uploaded file (single or multiple).
        """
        logging.info("Start merging files...")
        logging.info(f"split_count={split_count}")
        logging.info(f"offset={offset}")
        logging.info(f"pad_char={pad_char}")
        logging.info(f"left_count={left_count}")
        logging.info(f"right_count={right_count}")

        # Initialize storage client
        try:
            client = storage.Client()
        except Exception as e:
            logging.error(f"ERROR initializing client: {e}")
            raise
        if   row_delimiter=="NEW_LINE":
                row_delimiter="\n"
        elif row_delimiter=="CR_NEW_LINE":
            row_delimiter="\r\n"

        else:
            # Raise an exception if row_delimiter isn't recognized
            logging.error(f"Unknown row_delimiter: {row_delimiter}")
            raise ValueError(f"Unknown row_delimiter: {row_delimiter}")


        if print_header==false:
             header_line = None

        try:
            bucket = client.bucket(bucket_name)
        except Exception as e:
            logging.error(f"ERROR getting bucket: {e}")
            raise

        # List all blobs that match the prefix
        try:
            blobs = list(bucket.list_blobs(prefix=prefix))
        except Exception as e:
            logging.error(f"Error listing blobs in gs://{bucket_name}/{prefix}: {e}")
            raise
        logging.info(f"Found {len(blobs)} files under gs://{bucket_name}/{prefix}")

        if not blobs:
            error_message = f"No files found under gs://{bucket_name}/{prefix}"
            logging.error(error_message)
            raise FileNotFoundError(error_message)

        # Collect lines in memory
        lines_to_write = []
        logging.info(f"print_header={print_header}")
        logging.info(f"header_line={header_line}")

        # If we need to print a header, add it first
        if print_header=="true" and header_line:
            lines_to_write.append(header_line.strip())
            logging.info("Added header line to lines_to_write")

        # Merge content from each blob
        for i, blob in enumerate(blobs):
            try:
                content = blob.download_as_text()
            except Exception as e:
                logging.error(f"Failed to download blob {blob.name} from gs://{bucket_name}: {e}")
                raise Exception(f"Failed to download blob {blob.name}") from e

            file_lines = content.splitlines()



            lines_to_write.extend(file_lines)
            logging.info(f"Added {len(file_lines)} lines to lines_to_write")
            logging.info(f"Total  {len(lines_to_write)}  lines_to_write")


        # Decide whether to write a single file or multiple split files
        if split_count <= 1:
            return self._write_and_upload_single_file(
                bucket,
                lines_to_write,
                destination_blob_name,
                drop_temp_dir,
                prefix,
                row_delimiter
            )
        else:
            return self._write_and_upload_multiple_files(
                bucket,
                lines_to_write,
                destination_blob_name,
                header_line if print_header else None,
                split_count,
                drop_temp_dir,
                prefix,
                row_delimiter,
                pad_need,
                offset,
                pad_char,
                left_count,
                right_count
            )

    def _write_and_upload_single_file(
            self, bucket, lines_to_write, destination_blob_name, drop_temp_dir, prefix,row_delimiter
    ):
        """
        Helper method to create a single merged file and optionally delete the original files.
        Returns a list with exactly one dict in `json_data_successor` format.
        """
        logging.info("executing _write_and_upload_single_file ")
        temp_file_path = None
        row_count = len(lines_to_write)  # includes header if present

        try:
            # Write all lines to a temp file
            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
                temp_file_path = temp_file.name
                for line in lines_to_write:
                    temp_file.write(line + row_delimiter)

            # Upload the merged file to GCS
            merged_blob = bucket.blob(destination_blob_name)
            merged_blob.upload_from_filename(temp_file_path)
            logging.info(f"Merged file created at gs://{bucket.name}/{destination_blob_name}")

            # Reload the blob to ensure metadata is up to date
            merged_blob.reload()

        finally:
            # Cleanup local temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError as cleanup_error:
                    logging.warning(f"Failed to remove temp file {temp_file_path}: {cleanup_error}")

            # If drop_temp_dir == "Y", remove all objects under prefix
            if drop_temp_dir == "Y":
                self._delete_temp_dir(bucket, prefix)

        # Build the metadata dict
        # We'll parse the blob name into directory + filename_base
        directory_part, filename_base = self._split_gcs_path(merged_blob.name)
        metadata = {
            'filename_base': filename_base,
            'filename_full': merged_blob.name,
            'file_size': str(merged_blob.size) if merged_blob.size is not None else "0",
            'file_last_modified': str(int(merged_blob.updated.timestamp())) if merged_blob.updated else "0",
            'gs_bucket_name': bucket.name,
            'gs_directory': directory_part,
            'file_row_count': row_count
        }

        return [metadata]

    def _pad_string(self,text: str, left_count: int, right_count: int, pad_char: str) -> str:
        """
        Pads the given string with `pad_char` on the left and right.

        :param text: The string to pad.
        :param left_count: Number of pad characters on the left.
        :param right_count: Number of pad characters on the right.
        :param pad_char: Single character to use for padding.
        :return: The padded string.
        """
        logging.info("_pad_string exectuting")
        if len(pad_char) != 1:
            raise ValueError("pad_char must be a single character.")
        string_len = len(text)
        logging.info(f"string_len={string_len}")
        logging.info(f"left_count={left_count}")
        logging.info(f"right_count={right_count}")
        left_padding_actual = left_count - string_len
        right_padding_actual = right_count - string_len
        logging.info(f"left_padding_actual={left_padding_actual}")
        logging.info(f"right_padding_actual={right_padding_actual}")

        left_padding = pad_char * left_padding_actual
        right_padding = pad_char * right_padding_actual
        return left_padding + text + right_padding

    def _write_and_upload_multiple_files(
            self,
            bucket,
            lines_to_write,
            destination_blob_name_template,
            header_line,
            split_count,
            drop_temp_dir,
            prefix,
            row_delimiter,
            pad_need,
            var_offset:int,
            pad_char: str,
            left_count: int,
            right_count: int

    ):
        """
        Helper method to create multiple merged files (split_count parts).
        Returns a list of metadata dicts, each in `json_data_successor` format.

        If `header_line` is not None, each part will include it as the first line.

        `destination_blob_name_template` can have placeholders:
          - {part_index}
          - {total_parts}
          - {prefix}
        """
        # If the very first line is the header, remove it from the main data so we don't double it
        main_lines = lines_to_write
        logging.info("executing _write_and_upload_multiple_files( ")
        logging.info(f"main_lines={len(main_lines)}")
        if header_line and len(main_lines) > 0 and main_lines[0].strip() == header_line.strip():
            main_lines = main_lines[1:]

        total_lines = len(main_lines)
        if total_lines == 0:
            logging.warning("No data lines to write after merging (only header?). Forcing split_count=1.")
            split_count = 1

        chunk_size = math.ceil(total_lines / split_count) if split_count > 0 else total_lines
        logging.info(f"Splitting {total_lines} lines into {split_count} parts (~{chunk_size} lines each).")

        parts_metadata = []


        for part_index in range(split_count):
            start_idx = part_index * chunk_size
            end_idx = min(start_idx + chunk_size, total_lines)
            part_lines = main_lines[start_idx:end_idx]

            # Build the final list of lines for this part
            lines_for_this_part = []
            if header_line:
                lines_for_this_part.append(header_line.strip())
            lines_for_this_part.extend(part_lines)

            # If there's truly nothing to upload, skip
            if not lines_for_this_part:
                logging.warning(f"No lines for part {part_index + 1}, skipping upload.")
                continue

            row_count_for_this_part = len(lines_for_this_part)

            temp_file_path = None
            try:
                # Write these lines to a local file
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
                    temp_file_path = temp_file.name
                    for line in lines_for_this_part:
                        temp_file.write(line + row_delimiter)

                # Create the part filename from the template
                part_index =part_index +  var_offset
                part_index_text=str(part_index)
                logging.info(f"part_index_text= {part_index_text} ")
                if pad_need=="Y":
                    logging.info(f"Starting getting part_index_text")
                    logging.info(f"pad_char={pad_char}")
                    logging.info(f"left_count={left_count}")
                    logging.info(f"right_count={right_count}")
                    part_index_text = self._pad_string(part_index_text, left_count, right_count, pad_char)

                part_blob_name = destination_blob_name_template.format(
                    part_index=part_index_text,
                    total_parts=split_count,
                    prefix=prefix
                )
                logging.info(f"part_blob_name={part_blob_name}")
                logging.info(f"prefix={prefix}")

                # Upload
                merged_blob = bucket.blob(part_blob_name)
                merged_blob.upload_from_filename(temp_file_path)
                logging.info(f"Part {part_index + 1} created at gs://{bucket.name}/{part_blob_name}")

                # Reload so we can get updated metadata
                merged_blob.reload()

            finally:
                # Cleanup local temp file
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                    except OSError as cleanup_error:
                        logging.warning(f"Failed to remove temp file {temp_file_path}: {cleanup_error}")

            # Build the metadata dict (json_data_successor)
            directory_part, filename_base = self._split_gcs_path(merged_blob.name)
            part_metadata = {
                'filename_base': filename_base,
                'filename_full': merged_blob.name,
                'file_size': str(merged_blob.size) if merged_blob.size is not None else "0",
                'file_last_modified': str(int(merged_blob.updated.timestamp())) if merged_blob.updated else "0",
                'gs_bucket_name': bucket.name,
                'gs_directory': directory_part,
                'file_row_count': row_count_for_this_part
            }
            parts_metadata.append(part_metadata)

        # If drop_temp_dir == "Y", remove all objects under prefix
        if drop_temp_dir == "Y":
            self._delete_temp_dir(bucket, prefix)

        return parts_metadata

    def _split_gcs_path(self, blob_name):
        """
        Utility to split a GCS 'blob_name' into (directory_part, file_part).
        e.g.
          "some/subdir/file.csv" -> ("some/subdir/", "file.csv")
          "justfile.csv"         -> ("", "justfile.csv")
        """
        if "/" in blob_name:
            idx = blob_name.rfind("/")
            directory_part = blob_name[:idx + 1]  # keep trailing slash
            file_part = blob_name[idx + 1:]
            return directory_part, file_part
        else:
            return "", blob_name

    def _delete_temp_dir(self, bucket, prefix):
        """
        Deletes all blobs under the given prefix from the specified bucket.
        """
        try:
            del_blobs = bucket.list_blobs(prefix=prefix)
            count = 0
            for b in del_blobs:
                b.delete()
                count += 1
            logging.info(f"Deleted {count} objects under gs://{bucket.name}/{prefix}")
        except Exception as e:
            logging.error(f"Failed to delete objects under gs://{bucket.name}/{prefix}: {e}")

    def get_gs_file_pro(self, gs_bucket_name, gs_directory, filename_base, row_count):
        bucket = self.client.get_bucket(gs_bucket_name)
        blob = bucket.get_blob(f"{gs_directory}{filename_base}")

        if blob is None:
            raise Exception(
                f"File {gs_directory}{filename_base} not found in bucket {gs_bucket_name}.")

        json_data_successor = {
            'filename_base': filename_base,
            'filename_full': blob.name,
            'file_size': str(blob.size) if blob.size is not None else "0",
            'file_last_modified': str(int(blob.updated.timestamp())) if blob.updated else "0",
            'gs_bucket_name': gs_bucket_name,
            'gs_directory': gs_directory,
            'file_row_count': row_count
        }
        logging.info(f"json_source JSON= {json.dumps(json_data_successor)}")
        return json_data_successor

    # --- paste inside class GCSManager -----------------------------------------

    def _compose_many(
            self,
            bucket,
            components,
            dest_blob,
            content_type="text/csv",
            tmp_prefix=None,
            delete_temps=True,
    ):
        """
        Compose an arbitrary number of component blobs into dest_blob.
        Handles GCS's 32-component-per-compose-call limit by reducing in rounds.
        """
        if not components:
            raise ValueError("No components to compose.")

        # Fast path: <= 32 components
        if len(components) <= 32:
            dest_blob.content_type = content_type
            dest_blob.compose(components)
            return

        # Multi-stage reduction
        current = list(components)
        intermediates = []
        round_idx = 0
        if not tmp_prefix:
            tmp_prefix = f".compose_tmp/{uuid4().hex}"

        while len(current) > 32:
            next_round = []
            for i in range(0, len(current), 32):
                chunk = current[i:i + 32]
                inter = bucket.blob(f"{tmp_prefix}/r{round_idx}_{i // 32}_{uuid4().hex}")
                inter.content_type = content_type
                inter.compose(chunk)
                next_round.append(inter)
            intermediates.extend(next_round)
            current = next_round
            round_idx += 1

        dest_blob.content_type = content_type
        dest_blob.compose(current)

        if delete_temps:
            for b in intermediates:
                try:
                    b.delete()
                except Exception:
                    pass

    def _make_header_blob(self, bucket, tmp_prefix, header_line, content_type="text/csv"):
        """
        Create a tiny blob containing header_line + newline; returns the blob.
        """
        header_blob = bucket.blob(f"{tmp_prefix}/header_{uuid4().hex}.csv")
        # Use one-shot upload of small content
        header_blob.upload_from_string(
            (header_line.rstrip("\n") + "\n"),
            content_type=content_type,
            client=self.client,
        )
        return header_blob

    @staticmethod
    def _render_part_filename(
            template: str,
            part_index_one_based: int,
            offset: int = 0,
            pad_need: str = "N",
            pad_char: str = "0",
            left_count: int | None = None,
            right_count: int | None = None,
    ) -> str:
        num = int(part_index_one_based) + int(offset or 0)
        s = str(num)

        if str(pad_need).upper() == "Y":
            pc = (pad_char or "0")[0]
            if left_count and int(left_count) > 0:
                s = (pc * int(left_count)) + s
            if right_count and int(right_count) > 0:
                s = s + (pc * int(right_count))

        return template.format(part_index=s, PART_INDEX=s)

    def merge_csvs_to_n_outputs_compose(
            self,
            bucket_name: str,  # SOURCE bucket
            src_prefix: str,  # e.g. "input/folder/"
            dest_prefix: str,  # e.g. "output/folder/"
            num_outputs: int,  # how many merged files to create
            add_header: bool = False,  # prepend header to each output?
            header_line: str = None,  # required if add_header=True
            suffix_filter: str = ".csv",
            content_type: str = "text/csv",
            delete_temps: bool = True,
            dest_bucket_name: str = None,  # OPTIONAL: different OUTPUT bucket

            # filename templating & numbering (NO pad_side)
            filename_template: str = "part-{part_index}.csv",  # e.g. "ZAUTOZ...x{part_index}0.txt"
            offset: int = 0,
            pad_need: str = "N",
            pad_char: str = "0",
            left_count: int | None = None,  # add this many pad chars to the LEFT (if pad_need=='Y')
            right_count: int | None = None,  # add this many pad chars to the RIGHT (if pad_need=='Y')
    ):
        """
        Merge many CSV files (no headers) under gs://<bucket_name>/<src_prefix> into N outputs.
        Uses GCS server-side compose (no local download). If dest_bucket_name != bucket_name:
          - compose temp object(s) in source bucket, copy to destination bucket, clean up temps.

        IMPORTANT: Compose does not insert separators. Ensure your source files end with '\n'
                   or pre-process accordingly.

        Returns: list of metadata dicts for the FINAL objects:
          [
            {
              "filename_base": "...",
              "filename_full": "...",
              "file_size": "...",
              "file_last_modified": "...",
              "gs_bucket_name": "...",
              "gs_directory": "...",
              "file_row_count": None
            },
            ...
          ]
        """


        if num_outputs < 1:
            raise ValueError("num_outputs must be >= 1")
        if add_header and not header_line:
            raise ValueError("header_line must be provided when add_header=True")

        # Normalize prefixes
        src_prefix = src_prefix.lstrip("/")
        if src_prefix and not src_prefix.endswith("/"):
            src_prefix += "/"
        dest_prefix = dest_prefix.strip("/")
        if dest_prefix and not dest_prefix.endswith("/"):
            dest_prefix += "/"

        # Buckets
        dest_bucket_name = dest_bucket_name or bucket_name
        src_bucket = self.client.bucket(bucket_name)
        dest_bucket = self.client.bucket(dest_bucket_name)
        compose_bucket = src_bucket  # compose must occur within a single bucket

        # Temporary prefix in compose (source) bucket
        tmp_prefix = f"{dest_prefix}.tmp_{uuid4().hex}"

        # List & filter source CSVs (skip "directory markers")
        src_blobs = [
            b for b in self.client.list_blobs(bucket_name, prefix=src_prefix)
            if not b.name.endswith("/") and b.name.endswith(suffix_filter)
        ]
        src_blobs.sort(key=lambda b: b.name)

        if not src_blobs:
            raise FileNotFoundError(
                f"No CSV files found in gs://{bucket_name}/{src_prefix} with suffix '{suffix_filter}'"
            )

        # Distribute as evenly as possible
        n = len(src_blobs)
        per = math.ceil(n / num_outputs)

        results = []
        idx = 0
        header_blobs_to_cleanup = []

        try:
            for g in range(num_outputs):
                if idx >= n:
                    break
                group = src_blobs[idx: idx + per]
                idx += per

                components = []
                if add_header:
                    hb = self._make_header_blob(
                        compose_bucket, tmp_prefix, header_line, content_type=content_type
                    )
                    header_blobs_to_cleanup.append(hb)
                    components.append(hb)

                # NOTE: compose doesn't insert separators; ensure inputs end with '\n'
                components.extend(group)

                # Build final short name using template + offset + optional left/right pad counts
                file_name_only = self._render_part_filename(
                    template=filename_template,
                    part_index_one_based=g + 1,
                    offset=offset,
                    pad_need=pad_need,
                    pad_char=pad_char,
                    left_count=left_count,
                    right_count=right_count,
                )
                final_name = f"{dest_prefix}{file_name_only}"
                logging.info(f"final_name={final_name }")

                if dest_bucket_name == bucket_name:
                    # Same bucket: compose directly to final object
                    final_blob = dest_bucket.blob(final_name)
                    self._compose_many(
                        bucket=compose_bucket,
                        components=components,
                        dest_blob=final_blob,
                        content_type=content_type,
                        tmp_prefix=tmp_prefix,
                        delete_temps=True,
                    )
                    final_blob.reload()
                else:
                    # Different bucket: compose temp in source, then copy to dest
                    temp_name = f"{tmp_prefix}/{file_name_only}"
                    logging.info(f"temp_name={temp_name}")
                    temp_blob = compose_bucket.blob(temp_name)
                    self._compose_many(
                        bucket=compose_bucket,
                        components=components,
                        dest_blob=temp_blob,
                        content_type=content_type,
                        tmp_prefix=tmp_prefix,
                        delete_temps=True,
                    )
                    final_blob = compose_bucket.copy_blob(temp_blob, dest_bucket, final_name)
                    if delete_temps:
                        try:
                            temp_blob.delete()
                        except Exception:
                            pass
                    final_blob.reload()

                # Build metadata for FINAL object
                directory_part, filename_base = self._split_gcs_path(final_blob.name)
                results.append({
                    "filename_base": filename_base,
                    "filename_full": final_blob.name,
                    "file_size": str(final_blob.size) if final_blob.size is not None else "0",
                    "file_last_modified": str(int(final_blob.updated.timestamp())) if final_blob.updated else "0",
                    "gs_bucket_name": dest_bucket.name,
                    "gs_directory": directory_part,
                    "file_row_count": None,  # unknown without scanning (compose doesnâ€™t count rows)
                })
        finally:
            if delete_temps:
                # Remove header temp blobs (in compose/source bucket)
                for hb in header_blobs_to_cleanup:
                    try:
                        hb.delete()
                    except Exception:
                        pass
                # Remove any remaining temps (compose/source bucket)
                try:
                    for b in self.client.list_blobs(bucket_name, prefix=tmp_prefix):
                        try:
                            b.delete()
                        except Exception:
                            pass
                except Exception:
                    pass

        return results
