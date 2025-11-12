import os
import time
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd


class S3Client:
    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_region_name: str = None,
        aws_session_token: str = None,
        aws_role_arn: str = None,
        aws_role_session_name: str = None,
        aws_session_duration: int = 3600,
        aws_external_id: str = None,
    ):
        """
        Initialize an S3 client.

        Two modes:
        1) Direct credentials (env/instance/profile or provided aws_access_key_id/aws_secret_access_key[/aws_session_token])
        2) Assume role via STS if role_arn provided (optionally external_id, custom session name, and duration).

        Args:
            aws_access_key_id (str, optional): Base credentials (optional if using env/instance).
            aws_secret_access_key (str, optional): Base credentials secret.
            region_name (str, optional): AWS region (e.g., 'us-east-1').
            aws_session_token (str, optional): Session token for base creds.
            role_arn (str, optional): ARN of role to assume.
            role_session_name (str, optional): Session name for STS role (defaults to 's3client-<epoch>').
            session_duration (int, optional): Duration seconds (900–43200; defaults 3600).
            external_id (str, optional): ExternalId for third-party role assumption.

        Raises:
            Exception: When initialization fails.
        """
        try:
            # Start with a base session: explicit creds win; otherwise default chain (env, profile, IMDS, etc.)
            base_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region_name,
            )

            if aws_role_arn:
                # Assume role with STS
                sts = base_session.client('sts', region_name=aws_region_name)
                assume_args = {
                    "RoleArn": aws_role_arn,
                    "RoleSessionName": aws_role_session_name or f"s3client-{int(time.time())}",
                    "DurationSeconds": aws_session_duration,
                }
                if aws_external_id:
                    assume_args["ExternalId"] = aws_external_id

                resp = sts.assume_role(**assume_args)
                creds = resp["Credentials"]

                # Build a session with the temporary credentials
                assumed_session = boto3.Session(
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                    region_name=aws_region_name,
                )
                self.s3_client = assumed_session.client('s3')
                self._creds_expiration = creds.get("Expiration")
                logging.info(
                    f"S3 client initialized via AssumeRole: {aws_role_arn} "
                    f"(expires {self._creds_expiration})"
                )
            else:
                # Direct client (from explicit or default credentials)
                self.s3_client = base_session.client('s3')
                self._creds_expiration = None
                logging.info("S3 client initialized successfully (direct credentials/default chain).")

        except Exception as e:
            logging.error(f"Failed to initialize S3 client: {e}")
            raise

    def upload_file_to_s3(self, file_path, bucket_name, object_name=None) -> bool:
        """
        Upload a file to an S3 bucket.

        Args:
            file_path (str): Local file path.
            bucket_name (str): Target S3 bucket.
            object_name (str, optional): Key name in S3. Defaults to basename(file_path).

        Returns:
            bool: True if uploaded, else False.
        """
        object_name = object_name or os.path.basename(file_path)

        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            logging.info(
                f"File {file_path} uploaded to s3://{bucket_name}/{object_name}"
            )
            return True
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            return False
        except NoCredentialsError:
            logging.error("AWS credentials not available.")
            return False
        except ClientError as e:
            # Surface common access/permissions errors
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            logging.error(f"S3 upload failed ({code}): {e}")
            return False
        except Exception as e:
            logging.error(f"Error uploading file to S3: {e}")
            return False
    @staticmethod
    def _parse_s3_uri(s3_uri: str):
        """
        Parse s3://bucket/prefix... into (bucket, prefix).
        """
        if not s3_uri or not s3_uri.startswith("s3://"):
            raise ValueError("s3_uri must start with s3://")
        parts = s3_uri[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    @staticmethod
    def _parse_s3_uri(s3_uri: str):
        """
        Parse s3://bucket/prefix... into (bucket, prefix).
        """
        if not s3_uri or not s3_uri.startswith("s3://"):
            raise ValueError("s3_uri must start with s3://")
        parts = s3_uri[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    def list_files_to_dataframe(
        self,
        bucket_name: str = None,
        prefix: str = "",
        *,
        s3_uri: str = None,
        recursive: bool = True,
        include_folders: bool = False,
        suffix_filter: str = None,
    ) -> pd.DataFrame:
        """
        List objects under bucket/prefix and return a pandas DataFrame.

        Args:
            bucket_name (str): S3 bucket name (ignored if s3_uri is provided).
            prefix (str): Key prefix (AKA "path") to list.
            s3_uri (str): Optional s3://bucket/prefix form. If provided, overrides bucket_name/prefix.
            recursive (bool): If False, uses Delimiter='/' to list only immediate children.
            include_folders (bool): Include keys that end with '/' (S3 "folders").
            suffix_filter (str): If set, only include keys that end with this suffix (e.g., ".csv").

        Returns:
            pandas.DataFrame with columns:
            ['bucket','key','size','last_modified','etag','storage_class','s3_uri']
        """
        # Resolve bucket/prefix (allow s3_uri or separate args)
        if s3_uri:
            bkt, pfx = self._parse_s3_uri(s3_uri)
            bucket_name = bucket_name or bkt
            # if caller also passed prefix, keep it (lets you further narrow the s3_uri)
            prefix = f"{pfx}{prefix}" if pfx and prefix else (pfx or prefix)

        if not bucket_name:
            raise ValueError("bucket_name or s3_uri must be provided.")

        params = {"Bucket": bucket_name, "Prefix": prefix or ""}
        if not recursive:
            params["Delimiter"] = "/"

        rows = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(**params):
                # Objects
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not include_folders and key.endswith("/"):
                        continue
                    if suffix_filter and not key.endswith(suffix_filter):
                        continue

                    rows.append({
                        "bucket": bucket_name,
                        "key": key,
                        "size": obj.get("Size"),
                        "last_modified": obj.get("LastModified"),
                        "etag": (obj.get("ETag") or "").strip('"'),
                        "storage_class": obj.get("StorageClass"),
                        "s3_uri": f"s3://{bucket_name}/{key}",
                    })

                # If non-recursive, you may want the "folders" (CommonPrefixes)
                if not recursive:
                    for cp in page.get("CommonPrefixes", []):
                        folder = cp.get("Prefix")
                        if include_folders and folder:
                            rows.append({
                                "bucket": bucket_name,
                                "key": folder,
                                "size": None,
                                "last_modified": None,
                                "etag": None,
                                "storage_class": "FOLDER",
                                "s3_uri": f"s3://{bucket_name}/{folder}",
                            })

        except ClientError as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            logging.error(f"S3 list failed ({code}): {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error listing S3: {e}")
            raise

        # Return a consistent empty frame if nothing found
        if not rows:
            return pd.DataFrame(columns=[
                "bucket", "key", "size", "last_modified", "etag", "storage_class", "s3_uri"
            ])

        df = pd.DataFrame(rows)
        # Ensure pandas recognizes timestamps properly (boto3 returns tz-aware datetimes already)
        # df["last_modified"] = pd.to_datetime(df["last_modified"], utc=True)  # optional, uncomment if you want
        return df

    def list_files_to_json(
            self,
            *,
            bucket_name: str = None,
            prefix: str = "",
            s3_uri: str = None,
            recursive: bool = True,
            include_folders: bool = False,  # keep False
            suffix_filter: str = None,
    ) -> list:
        """
        List S3 objects and return a list[dict] with fields similar to SFTP fetch_files, plus S3 info:
          [
            {
              "filename_base": "<basename>",
              "filename_full": "<full key>",
              "file_size": "<size_as_string>",
              "file_last_modified": "<iso-ish string>",
              "s3_bucket_name": "<bucket>",
              "s3_uri": "s3://<bucket>/<key>"
            },
            ...
          ]
        """
        # Resolve bucket/prefix (allow s3_uri or separate args)
        if s3_uri:
            bkt, pfx = self._parse_s3_uri(s3_uri)
            bucket_name = bucket_name or bkt
            prefix = f"{pfx}{prefix}" if pfx and prefix else (pfx or prefix)

        if not bucket_name:
            raise ValueError("bucket_name or s3_uri must be provided.")

        params = {"Bucket": bucket_name, "Prefix": prefix or ""}
        if not recursive:
            params["Delimiter"] = "/"

        items = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(**params):
                # Files/objects
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not include_folders and key.endswith("/"):
                        continue
                    if suffix_filter and not key.endswith(suffix_filter):
                        continue

                    filename_base = os.path.basename(key.rstrip("/"))
                    file_size = obj.get("Size")
                    last_modified = obj.get("LastModified")

                    items.append({
                        "filename_base": filename_base,
                        "filename_full": key,  # full S3 key (no bucket)
                        "file_size": "" if file_size is None else str(file_size),
                        "file_last_modified": "" if last_modified is None else str(last_modified),
                        "s3_bucket_name": bucket_name,
                        "s3_uri": f"s3://{bucket_name}/{key}",
                    })

                # Optional "folders" (only when non-recursive)
                if not recursive and include_folders:
                    for cp in page.get("CommonPrefixes", []):
                        folder = cp.get("Prefix")
                        if folder:
                            items.append({
                                "filename_base": os.path.basename(folder.rstrip("/")),
                                "filename_full": folder,  # the prefix (no bucket)
                                "file_size": "",  # folders have no size
                                "file_last_modified": "",  # folders have no timestamp
                                "s3_bucket_name": bucket_name,
                                "s3_uri": f"s3://{bucket_name}/{folder}",
                            })

        except ClientError as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            logging.error(f"S3 list failed ({code}): {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error listing S3: {e}")
            raise

        logging.info(f"Listed {len(items)} S3 item(s) under s3://{bucket_name}/{prefix}")
        return items

    def download_file_from_s3(
            self,
            *,
            bucket_name: str = None,
            key: str = None,
            local_path: str = None,
            s3_uri: str = None,
            make_dirs: bool = True,
            overwrite: bool = True,
            max_attempts: int = 3,
    ) -> str:
        """Download a single S3 object to a local file (all params keyword-only)."""
        # Resolve bucket/key
        if s3_uri:
            bkt, pfx = self._parse_s3_uri(s3_uri)
            bucket_name = bucket_name or bkt
            key = key or pfx
        if not bucket_name or not key:
            raise ValueError("You must provide bucket/key or s3_uri.")

        # Default local path
        if not local_path:
            local_path = os.path.basename(key) or "downloaded_object"

        # Create parent dirs
        if make_dirs:
            parent = os.path.dirname(local_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        # Respect overwrite
        if not overwrite and os.path.exists(local_path):
            logging.info(f"download_file_from_s3: exists, skipping (overwrite=False): {local_path}")
            return local_path

        # Verify with HEAD (to check size)
        try:
            head = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            expected_size = head.get("ContentLength")
        except ClientError as e:
            logging.error(f"HEAD failed for s3://{bucket_name}/{key}: {e}")
            raise

        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except Exception:
                        pass

                self.s3_client.download_file(bucket_name, key, local_path)
                actual_size = os.path.getsize(local_path)

                if expected_size is not None and expected_size != actual_size:
                    raise RuntimeError(
                        f"Size mismatch after download (expected={expected_size}, actual={actual_size})"
                    )

                logging.info(f"Downloaded s3://{bucket_name}/{key} -> {local_path} (size={actual_size})")
                return local_path

            except (ClientError, NoCredentialsError, RuntimeError, OSError) as e:
                last_err = e
                logging.warning(
                    f"Download attempt {attempt}/{max_attempts} failed for s3://{bucket_name}/{key}: {e}"
                )
                if attempt < max_attempts:
                    time.sleep(min(2 * attempt, 10))
                else:
                    break

        raise RuntimeError(
            f"Failed to download s3://{bucket_name}/{key} to {local_path} after {max_attempts} attempts: {last_err}"
        )




