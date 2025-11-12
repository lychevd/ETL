import paramiko
import posixpath
import logging
import time
import fnmatch
import stat

logger = logging.getLogger(__name__)


class SFTPManager:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = None


    def _connect(self):
        try:
            self.ssh.connect(self.host, username=self.username, password=self.password, port=self.port)
            self.window_size = 2147483647
            self.ssh.get_transport().set_keepalive(30)
            logging.info("SFTP connection successful!")
            self.sftp = self.ssh.open_sftp()
        except Exception as e:
            logging.error(f"SFTP connection error: {e}")
            raise

    def disconnect(self):
        if self.sftp:
            self.sftp.close()
        self.ssh.close()
        logging.info("SFTP disconnected.")

    def upload_file(self, local_path, remote_path):
        try:
            self._connect()
            self.sftp.put(local_path, remote_path)
            logging.info(f"Uploaded {local_path} to {remote_path}.")
        except Exception as e:
            logging.error(f"Error uploading file: {e}")
            raise

    def download_file(self, local_path, remote_path):
        try:
            self._connect()
            start_time = time.time()
            downloaded_bytes = 0
            chunk_size = 1024 * 1024  # 1 MB
            next_log_threshold = 100 * 1024 * 1024  # Log every 100 MB
            last_log_time = start_time

            with self.sftp.open(remote_path, 'rb') as remote_file, open(local_path, 'wb') as local_file:
                while True:
                    data = remote_file.read(chunk_size)
                    if not data:
                        break
                    local_file.write(data)
                    downloaded_bytes += len(data)

                    # Log progress every 100MB or every 15 seconds
                    now = time.time()
                    if downloaded_bytes >= next_log_threshold or (now - last_log_time > 15):
                        mb_downloaded = downloaded_bytes / (1024 * 1024)
                        elapsed = now - start_time
                        speed_mbps = mb_downloaded / elapsed
                        logging.info(f"Downloaded ~{mb_downloaded:.2f} MB at ~{speed_mbps:.2f} MB/s")
                        next_log_threshold += 100 * 1024 * 1024
                        last_log_time = now

            total_elapsed = time.time() - start_time
            total_mb = downloaded_bytes / (1024 * 1024)
            logging.info(
                f"Download complete: {total_mb:.2f} MB in {total_elapsed:.2f} seconds (~{total_mb / total_elapsed:.2f} MB/s)")

        except Exception as e:
            logging.error(f"Error downloading file: {e}")
            raise

    def remove_file(self,  remote_path):
            try:
                self._connect()
                self.sftp.remove(remote_path)
                logging.info(f"Error deleting  {remote_path}  .")
            except Exception as e:
                logging.error(f"Error deleting  file: {e}")
                raise
    def posix_rename(self,remote_path, destination_path):
            try:
                self._connect()
                self.sftp.posix_rename(remote_path,destination_path)
                logging.info(f"Error moving SFTP file  {remote_path}  to done directory {destination_path} .")
            except Exception as e:
                logging.error(f"Error moving SFTP file  file: {e}")
                raise

    def sftp_post_process(self, post_process_type, filename_full, filename_base, sftp_done_directory):
        try:
            self._connect()
            if post_process_type == "DELETE":
                # Delete file from SFTP
                try:
                    self.sftp.remove(filename_full)
                    logging.info(f"Deleted {filename_full} from SFTP.")
                except Exception as e:
                    logging.error(f"Error deleting file {filename_full}: {e}")
                    raise

            elif post_process_type == "MOVE_TO_DONE":
                # Ensure the 'done' directory exists
                try:

                    self.sftp.mkdir(sftp_done_directory)
                    logging.info(f"Ensured 'done' directory exists: {sftp_done_directory}")
                except IOError:
                    # Ignore if the directory already exists
                    logging.info(f"'Done' directory already exists: {sftp_done_directory}")

                # Construct the destination path
                destination_path = posixpath.join(sftp_done_directory, filename_base)

                # Move the file to the 'done' directory
                try:
                    self.sftp.rename(filename_full, destination_path)
                    logging.info(f"Moved SFTP file {filename_full} to done directory {destination_path}.")
                except Exception as e:
                    logging.error(f"Error moving file {filename_full} to {destination_path}: {e}")
                    raise
            else:
                logging.error(f"Invalid post_process_type: {post_process_type}")
                raise ValueError(f"Invalid post_process_type: {post_process_type}")

        except Exception as e:
            logging.error(f"Fail post process  {filename_full}: {e}")
            raise  # Stop processing on any error

    def ensure_remote_directory(self, remote_directory):
        try:
            self._connect()
            remote_directory = posixpath.normpath(remote_directory)
            current_path = '/'
            for directory in remote_directory.split('/'):
                if directory:
                    current_path = posixpath.join(current_path, directory)
                    if not self._sftp_exists(current_path):
                        self.sftp.mkdir(current_path)
                        logging.info(f"Created directory: {current_path}")
        except Exception as e:
            logging.error(f"Error ensuring remote directory: {e}")
            raise

    def fetch_files(self, sftp_remote_path, pattern=None):
        try:
            self._connect()
            files = []
            sftp_remote_path = sftp_remote_path.rstrip('/')
            self.ssh.get_transport().set_keepalive(30)

            for file_attr in self.sftp.listdir_attr(sftp_remote_path):
                if not stat.S_ISDIR(file_attr.st_mode):
                    filename_base = file_attr.filename
                    if pattern is None or fnmatch.fnmatch(filename_base, pattern):
                        filename_full = f"{sftp_remote_path}/{filename_base}"
                        file_size = str(file_attr.st_size)
                        file_last_modified = str(file_attr.st_mtime)

                        files.append({
                            "filename_base": filename_base,
                            "filename_full": filename_full,
                            "file_size": file_size,
                            "file_last_modified": file_last_modified
                        })
            logging.info(f"Fetched {len(files)} files from SFTP.")
            return files
        except Exception as e:
            logging.error(f"Failed to fetch files from SFTP: {e}")
            raise

