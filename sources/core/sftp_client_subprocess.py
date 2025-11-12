import subprocess
import os
import logging
import tempfile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SFTPClientSubprocess:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def _run_sftp_command(self, commands):
        """Run given SFTP batch commands via subprocess with sshpass."""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as cmd_file:
            for cmd in commands:
                cmd_file.write(f"{cmd}\n")
            cmd_file.write("bye\n")
            cmd_file_path = cmd_file.name

        sftp_cmd = [
	    "sshpass", "-p", self.password,
	    "sftp",
	    "-o", f"Port={self.port}",
	    "-o", "StrictHostKeyChecking=no",
	    "-o", "ServerAliveInterval=30",
	    "-o", "ServerAliveCountMax=10",
	    f"{self.username}@{self.host}"
	    ]

        logger.info(f"Running sftp command: {' '.join(sftp_cmd)}")

        try:
            result = subprocess.run(
                sftp_cmd,
                stdin=open(cmd_file_path, 'r'),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            if result.returncode != 0:
                logger.error("SFTP failed:\nSTDOUT:\n%s\nSTDERR:\n%s",
                             result.stdout, result.stderr)
                raise Exception("SFTP command failed")

            logger.info("SFTP operation completed successfully.")
        finally:
            os.remove(cmd_file_path)

    def download(self, remote_path, local_path):
        """Download file from SFTP server to local path."""
        logger.info(f"Downloading {remote_path} → {local_path}")
        self._run_sftp_command([f"get {remote_path} {local_path}"])

    def upload(self, local_path, remote_path):
        """Upload file from local path to SFTP server."""
        logger.info(f"Uploading {local_path} → {remote_path}")
        self._run_sftp_command([f"put {local_path} {remote_path}"])
