import os
import threading
import paramiko
import uuid
from datetime import datetime
from time import sleep, time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class FastSFTPDownloader:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.sftp = None
        self.transport = None

    def _connect(self):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.window_size = 2147483647
            self.transport.packetizer.REKEY_BYTES = pow(2, 40)
            self.transport.packetizer.REKEY_PACKETS = pow(2, 40)
            self.transport.connect(username=self.username, password=self.password)
            self.transport.set_keepalive(30)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logger.info("SFTP connection established with keepalive and performance tuning.")
        except Exception as e:
            logger.error("Failed to connect: %s", repr(e))
            raise

    def download(self, remote_path, local_path):
        self._connect()
        file_size = self.sftp.stat(remote_path).st_size
        logger.info(f"Starting download: {remote_path} ({file_size / 1024 / 1024:.2f} MB)")

        chunk_size = 64 * 1024 * 1024  # 64 MB
        progress_step = 100 * 1024 * 1024  # Log every 100MB
        downloaded = 0
        next_log_threshold = progress_step

        start = time()

        try:
            with self.sftp.open(remote_path, 'rb') as remote_file, open(local_path, 'wb') as local_file:
                while True:
                    data = remote_file.read(chunk_size)
                    if not data:
                        break
                    local_file.write(data)
                    downloaded += len(data)

                    now = time()
                    if downloaded >= next_log_threshold or now - start > 30:
                        mb = downloaded / (1024 * 1024)
                        speed = mb / (now - start)
                        logger.info(f"Downloaded ~{mb:.2f} MB at ~{speed:.2f} MB/s")
                        next_log_threshold += progress_step

            duration = time() - start
            logger.info(f"Download complete: {local_path} in {duration:.2f} seconds")
        finally:
            self.sftp.close()
            self.transport.close()
