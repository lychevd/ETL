import os
import logging
import subprocess

class GSUtilClient:
    def __init__(self, service_account_path=None, project=None):
        """
        Initializes the GSUtil client by setting up credentials and project environment variables.
        """
        self.env = os.environ.copy()

        if service_account_path:
            self.env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
            logging.info(f"GOOGLE_APPLICATION_CREDENTIALS set to: {service_account_path}")

        if project:
            self.env["CLOUDSDK_CORE_PROJECT"] = project
            logging.info(f"CLOUDSDK_CORE_PROJECT set to: {project}")

        if service_account_path:
            self.activate_service_account()
            self.log_active_gcloud_account()

    def activate_service_account(self):
        """Explicitly activate the service account for gcloud CLI."""
        key_path = self.env.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not key_path or not os.path.isfile(key_path):
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set or invalid. Cannot activate service account.")

        command = [
            "gcloud", "auth", "activate-service-account",
            "--key-file", key_path
        ]

        logging.info(f"Activating service account using: {key_path}")
        try:
            subprocess.run(command, check=True, capture_output=True, text=True, env=self.env)
            logging.info("Service account successfully activated.")
        except subprocess.CalledProcessError as err:
            logging.error(f"Failed to activate service account: {err.stderr}")
            raise

    def log_active_gcloud_account(self):
        """Logs the currently active gcloud account (bonus)."""
        try:
            result = subprocess.run(
                ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
                capture_output=True, text=True, check=True, env=self.env
            )
            active_account = result.stdout.strip()
            logging.info(f"Active gcloud account: {active_account}")
        except subprocess.CalledProcessError as err:
            logging.warning(f"Could not retrieve active gcloud account: {err.stderr}")

    def get_file_from_gs_util(self, gs_bucket_name, filename_full, local_temp_file, filename_base):
        """Download a file from GCS using `gcloud storage cp`."""
        gcloud_command = [
            "gcloud", "storage", "cp",
            f"gs://{gs_bucket_name}/{filename_full}",
            local_temp_file
        ]
        logging.info(f"Running command: {' '.join(gcloud_command)}")

        try:
            subprocess.run(gcloud_command, capture_output=True, text=True, check=True, env=self.env)
            logging.info(f"Downloaded '{filename_base}' from gs://{gs_bucket_name}/{filename_full}")
        except subprocess.CalledProcessError as err:
            logging.error(f"Download failed. Return code: {err.returncode}")
            logging.error(f"STDOUT: {err.stdout}")
            logging.error(f"STDERR: {err.stderr}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during download: {e}")
            raise

    def push_file_to_gs_util(self, gs_bucket_name, gs_directory, local_temp_file):
        """Upload a file to GCS using `gcloud storage cp`."""
        destination = f"gs://{gs_bucket_name}/{gs_directory}"
        gcloud_command = [
            "gcloud", "storage", "cp",
            local_temp_file,
            destination
        ]
        logging.info(f"Running command: {' '.join(gcloud_command)}")

        try:
            subprocess.run(gcloud_command, capture_output=True, text=True, check=True, env=self.env)
            logging.info(f"Uploaded '{local_temp_file}' to '{destination}'.")
        except subprocess.CalledProcessError as err:
            logging.error(f"Upload failed. Return code: {err.returncode}")
            logging.error(f"STDOUT: {err.stdout}")
            logging.error(f"STDERR: {err.stderr}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during upload: {e}")
            raise

    def gs_post_process(self, post_process_type, gs_bucket_name, filename_full,
                        gs_bucket_name_done=None, gs_done_directory=None):
        """Delete or move a file in GCS using `gcloud storage rm/mv`."""
        if post_process_type == "DELETE":
            gcloud_command = [
                "gcloud", "storage", "rm",
                f"gs://{gs_bucket_name}/{filename_full}"
            ]
            logging.info(f"Running command: {' '.join(gcloud_command)}")

            try:
                subprocess.run(gcloud_command, capture_output=True, text=True, check=True, env=self.env)
                logging.info(f"Deleted gs://{gs_bucket_name}/{filename_full}")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to delete file: {e.stderr}")
                raise

        elif post_process_type == "MOVE_TO_DONE":
            if not gs_done_directory:
                done_file_name = os.path.basename(filename_full)
                directory_path = os.path.dirname(filename_full) + "/Done/" + done_file_name
            else:
                if not gs_done_directory.endswith("/"):
                    gs_done_directory += "/"
                directory_path = gs_done_directory + os.path.basename(filename_full)

            if not gs_bucket_name_done:
                gs_bucket_name_done = gs_bucket_name

            gcloud_command = [
                "gcloud", "storage", "mv",
                f"gs://{gs_bucket_name}/{filename_full}",
                f"gs://{gs_bucket_name_done}/{directory_path}"
            ]
            logging.info(f"Running command: {' '.join(gcloud_command)}")

            try:
                subprocess.run(gcloud_command, capture_output=True, text=True, check=True, env=self.env)
                logging.info(f"Moved file to gs://{gs_bucket_name_done}/{directory_path}")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to move file: {e.stderr}")
                raise

    def move_all_files_to_archive(
            self,
            *,
            gs_bucket_name: str,
            gs_directory: str,
            gs_bucket_name_archive: str,
            gs_directory_archive: str
    ) -> int:
        """
        Move all objects under gs://<src_bucket>/<src_prefix>/ to
        gs://<dst_bucket>/<dst_prefix>/ using `gcloud storage mv`.

        - If the source prefix has no objects or doesn't exist, just log and return 0.
        - Still guards against an empty gs_directory (refuses to move entire bucket).

        Returns the number of objects *found* (attempted to move).
        """
        import subprocess
        import logging

        # Safety guard: don't allow moving the whole bucket
        if gs_directory is None or gs_directory.strip() == "":
            raise ValueError("gs_directory must be a non-empty prefix (refusing to move entire bucket).")

        src_prefix = gs_directory.strip("/")
        dst_prefix = (gs_directory_archive or "").strip("/")

        src_pattern = f"gs://{gs_bucket_name}/{src_prefix}/**"
        # Ensure destination ends with a slash so it's treated as a prefix
        dst_url = f"gs://{gs_bucket_name_archive}/{dst_prefix}/" if dst_prefix else f"gs://{gs_bucket_name_archive}/"

        def _is_no_objects_err(msg: str) -> bool:
            m = (msg or "").lower()
            return (
                    "matched no objects" in m or
                    "no urls matched" in m or
                    "notfound" in m or  # some variants include NotFound
                    "not found" in m
            )

        # 1) Pre-list to see what we'd move
        ls_cmd = ["gcloud", "storage", "ls", src_pattern]
        logging.info("Listing objects before move: %s", " ".join(ls_cmd))
        ls = subprocess.run(ls_cmd, capture_output=True, text=True, env=self.env)

        if ls.returncode != 0:
            if _is_no_objects_err(ls.stderr):
                logging.info("No objects found under %s. Nothing to move.", src_pattern)
                return 0
            logging.error("List failed. Return code: %s\nSTDOUT: %s\nSTDERR: %s", ls.returncode, ls.stdout, ls.stderr)
            raise subprocess.CalledProcessError(ls.returncode, ls_cmd, ls.stdout, ls.stderr)

        objects = [line.strip() for line in (ls.stdout or "").splitlines() if line.strip()]
        if not objects:
            logging.info("No objects found under %s. Nothing to move.", src_pattern)
            return 0

        logging.info("Found %d object(s) to move from %s to %s", len(objects), src_pattern, dst_url)

        # 2) Move (no -r; recursion is handled by the ** pattern)
        mv_cmd = ["gcloud", "storage", "mv", src_pattern, dst_url]
        logging.info("Running move: %s", " ".join(mv_cmd))
        mv = subprocess.run(mv_cmd, capture_output=True, text=True, env=self.env)

        if mv.returncode != 0:
            # Treat "no objects" (including race conditions) as non-fatal
            if _is_no_objects_err(mv.stderr):
                logging.info("Nothing to move (possibly a race). Source now empty: %s", src_pattern)
                return 0
            logging.error("Failed to move files. Return code: %s", mv.returncode)
            logging.error("STDOUT: %s", mv.stdout)
            logging.error("STDERR: %s", mv.stderr)
            raise subprocess.CalledProcessError(mv.returncode, mv_cmd, mv.stdout, mv.stderr)

        logging.info("Move completed. STDOUT: %s", (mv.stdout or "").strip())
        return len(objects)
