from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
import sys
import argparse
from dataclasses import dataclass, field
from core import gsutil_manager, rest_api_manager, secret_manager, bq_manager
from db import database_manager
import logging
import json
import re
import os
from urllib.parse import urlparse
import time
import posixpath

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class InputParams:
    workflow_name: str
    step_name: str
    work_flow_log_id: str
    meta_db_secret_name: str
    additional_param: str = field(default=None, metadata={"optional": True})


@dataclass
class RestParameters:
    rest_api_conn_secret: str
    api_key: str
    base_url: str
    app_name: str
    app_os: str
    num_days: str


@dataclass
class bqParm:
    work_flow_step_log_id: str
    service_account_path: str
    bigquery_project: str
    get_key_file_from_sec: str
    get_key_file_sec_name: str
    def_bq_cred: str
    def_bq_project: str
    clean_local_dir: str
    query: str


@dataclass
class GsParm:
    local_data_directory: str
    gs_bucket_name: str
    gs_bucket_name_archive: str
    gs_directory_archive: str
    gs_directory: str
    clean_local_dir: str
    def_gs_cred: str
    def_gs_project: str
    google_project_name: str


def build_date_list_for_processing(
    last_report_dt,
    num_days: int,
    *,
    tz_name: str = "America/New_York",
    today_override: date | None = None
) -> Dict[str, Any]:
    """
    Computes StartDate/EndDate and returns a list of date strings YYYY-MM-DD to loop over.

    StartDate = LastReportDt - num_days
    EndDate   = (today in tz) - 1 day
    """
    start_date = last_report_dt - timedelta(days=num_days)

    # EndDate = yesterday in tz
    if today_override is not None:
        today_local = today_override
    else:
        today_local = datetime.now(ZoneInfo(tz_name)).date()
    end_date = today_local - timedelta(days=1)

    if start_date > end_date:
        logging.warning(
            f"StartDate {start_date} > EndDate {end_date}. "
            "Clamping StartDate to EndDate."
        )
        start_date = end_date

    day_count = (end_date - start_date).days
    dates: List[str] = [
        (start_date + timedelta(days=i)).isoformat()
        for i in range(day_count + 1)
    ] if day_count >= 0 else []

    logging.info(
        f"Date range: StartDate={start_date.isoformat()}, "
        f"EndDate={end_date.isoformat()}, Count={len(dates)}"
    )

    return {
        "LastReportDt": last_report_dt.isoformat(),
        "StartDate": start_date.isoformat(),
        "EndDate": end_date.isoformat(),
        "dates": dates,
    }


def fetch_applovin_report_urls_for_dates(
    date_payload: Dict[str, Any],
    *,
    api_key: str,
    base_url: str,
    app_name: str,
    app_os: str,
    timeout: float = 60.0,
    headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    For each date in date_payload["dates"], build the Applovin MAX URL by replacing
    placeholders in base_url, GET it via RestApiManager.get_string_response(),
    and return a list of per-date results mimicking your Java logic.
    """

    def _interpolate_url(tmpl: str, mapping: Dict[str, str]) -> str:
        """
        Replace placeholders marked with |||...||| (and tolerate variants like ||key|||).
        NOTE: No generic fallback replacement is performed to avoid mis-substitution.
        """
        out = tmpl
        for k, v in mapping.items():
            out = out.replace(f"|||{k}|||", v)
            out = out.replace(f"||{k}|||", v)   # tolerate slight typos
            out = out.replace(f"|||{k}||", v)
        return out

    def _extract_report_url(raw_text: str) -> Optional[str]:
        """Try to parse out 'ad_revenue_report_url' from the response."""
        try:
            obj = json.loads(raw_text)
            if isinstance(obj, dict) and "ad_revenue_report_url" in obj:
                return obj.get("ad_revenue_report_url")
        except Exception:
            pass

        m = re.search(r'ad_revenue_report_url["\s:]*[:=]\s*"?([^",}\s]+)"?', raw_text)
        if m:
            return m.group(1)

        return None

    results: List[Dict[str, Any]] = []
    dates = date_payload.get("dates", [])
    if not isinstance(dates, list):
        logging.warning("date_payload['dates'] is not a list; got %r", type(dates))
        return results

    # Support multiple placeholder names in templates (|||api_key||| or |||app_key|||, |||date||| or |||report_date|||)
    mapping_base = {
        "api_key": api_key,
        "app_key": api_key,  # backward compatibility
        "app_name": app_name,
        "app_os": app_os,
    }

    for report_date in dates:
        mapping = dict(mapping_base)
        mapping["report_date"] = report_date
        mapping["date"] = report_date  # explicit date alias to avoid mis-substitution

        url = _interpolate_url(base_url, mapping)

        # Default outputs
        return_code: Optional[int] = None
        report_exist = "E"
        raw = ""
        report_url = None

        try:
            raw = rest_api_manager.RestApiManager.get_string_response(
                url=url,
                headers=headers or {},
                timeout=timeout,
                params=None,
            )
            if raw == "no_data_for_report":
                # 404 (no data)
                return_code = 404
                report_exist = "N"
                report_url = None
            else:
                # Success
                return_code = 0
                report_exist = "Y"
                report_url = _extract_report_url(raw)

        except Exception as e:
            # Try to pull an HTTP status code if it was an HTTPError
            import requests
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                return_code = e.response.status_code
            else:
                return_code = 500  # generic API error

            report_exist = "E"
            report_url = None
            raw = str(e)

        results.append({
            "date": report_date,
            "request_url": url,
            "return_code": return_code,
            "report_exist": report_exist,
            "report_url": report_url,
            "raw": raw,
        })

    return results


def download_applovin_reports_for_dates(
    date_payload: Dict[str, Any],
    *,
    api_key: str,
    base_url: str,
    app_name: str,
    app_os: str,
    local_directory: str,
    timeout: float = 60.0,
    filename_template: str = "applovin_{app}_{os}_{date}{ext}",
    max_retries: int = 2,
    retry_backoff: float = 1.5,
) -> Dict[str, Any]:
    """
    1) Builds per-date URLs (via fetch_applovin_report_urls_for_dates)
    2) Downloads each available report into `local_directory`
    3) Returns per-date results + summary
    """
    t0 = time.time()

    # validate & prep directory
    try:
        os.makedirs(local_directory, exist_ok=True)
        if not os.path.isdir(local_directory):
            raise OSError(f"Not a directory: {local_directory}")
        if not os.access(local_directory, os.W_OK):
            raise PermissionError(f"Directory not writable: {local_directory}")
    except Exception as e:
        logging.error("Failed to prepare local directory '%s': %s", local_directory, e)
        return {
            "results": [],
            "summary": {
                "total_dates": 0,
                "available": 0,
                "downloaded": 0,
                "no_data": 0,
                "errors": 1
            },
            "error": f"local_directory error: {e}",
            "dates": [],
            "request_urls": [],
            "by_date": {}
        }

    # build per-date request URLs
    try:
        results = fetch_applovin_report_urls_for_dates(
            date_payload=date_payload,
            api_key=api_key,
            base_url=base_url,
            app_name=app_name,
            app_os=app_os,
            timeout=timeout,
        )
    except Exception as e:
        logging.error("Failed to build or fetch report URLs: %s", e, exc_info=True)
        return {
            "results": [],
            "summary": {
                "total_dates": len(date_payload.get("dates", [])) if isinstance(date_payload, dict) else 0,
                "available": 0,
                "downloaded": 0,
                "no_data": 0,
                "errors": 1
            },
            "error": f"fetch_applovin_report_urls_for_dates error: {e}",
            "dates": list(date_payload.get("dates", [])) if isinstance(date_payload, dict) else [],
            "request_urls": [],
            "by_date": {}
        }

    def _make_filename(report_url: str, date_str: str) -> str:
        path = urlparse(report_url).path
        ext = os.path.splitext(path)[1] or ".csv"
        safe_app = app_name.replace(" ", "_")
        safe_os = app_os.replace(" ", "_")
        fname = filename_template.format(app=safe_app, os=safe_os, date=date_str, ext=ext)
        return os.path.join(local_directory, fname)

    total_dates = len(results)
    available = sum(1 for r in results if r.get("report_exist") == "Y" and r.get("report_url"))
    downloaded = 0
    no_data = 0
    errors = 0

    for r in results:
        r.setdefault("downloaded", False)
        r.setdefault("download_path", None)
        r.setdefault("error", None)

        if r.get("report_exist") != "Y" or not r.get("report_url"):
            if r.get("return_code") == 404 or r.get("report_exist") == "N":
                no_data += 1
            elif r.get("report_exist") == "E":
                errors += 1
            continue

        out_path = _make_filename(r["report_url"], r["date"])

        attempt = 0
        while True:
            attempt += 1
            try:
                rest_api_manager.RestApiManager.download_file_using_url(
                    download_url=r["report_url"],
                    output_filename=out_path,
                )
                r["downloaded"] = True
                r["download_path"] = out_path
                downloaded += 1
                break

            except Exception as e:
                import requests
                code = None
                if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                    code = e.response.status_code
                    r["return_code"] = code
                    if code == 404:
                        r["report_exist"] = "N"
                        r["error"] = "URL returned 404 during download"
                        no_data += 1
                        break
                    if 400 <= code < 500 and code != 429:
                        r["report_exist"] = "E"
                        r["error"] = f"HTTP {code} during download"
                        errors += 1
                        break

                if attempt <= max_retries:
                    sleep_s = retry_backoff ** (attempt - 1)
                    logging.warning(
                        "Download failed (attempt %d/%d) for %s: %s. Retrying in %.2fs",
                        attempt, max_retries, r.get("date"), e, sleep_s
                    )
                    time.sleep(sleep_s)
                    continue

                r["report_exist"] = "E"
                r["error"] = f"Download failed after {max_retries} retries: {e}"
                errors += 1
                break

    elapsed = time.time() - t0
    logging.info(
        "Applovin download summary: total=%d available=%d downloaded=%d no_data=%d errors=%d in %.2fs",
        total_dates, available, downloaded, no_data, errors, elapsed
    )

    dates = [r.get("date") for r in results]
    request_urls = [r.get("request_url") for r in results]
    by_date = {
        r["date"]: {
            "request_url": r.get("request_url"),
            "report_url": r.get("report_url"),
            "downloaded": r.get("downloaded"),
            "download_path": r.get("download_path"),
            "return_code": r.get("return_code"),
            "report_exist": r.get("report_exist"),
            "error": r.get("error"),
        }
        for r in results
        if "date" in r
    }

    return {
        "results": results,
        "summary": {
            "total_dates": total_dates,
            "available": available,
            "downloaded": downloaded,
            "no_data": no_data,
            "errors": errors,
            "elapsed_sec": round(elapsed, 3),
        },
        "dates": dates,
        "request_urls": request_urls,
        "by_date": by_date,
    }


def get_report_last_date(bq_params: bqParm):
    """
    Initialize BQManager using either default creds/project or explicit ones from bq_params,
    run bq_params.query, and return the first value (normalized to date when possible).
    """
    try:
        def_bq_cred = str(getattr(bq_params, "def_bq_cred", "N")).upper()
        def_bq_project = str(getattr(bq_params, "def_bq_project", "N")).upper()

        service_account_path = None if def_bq_cred == "Y" else getattr(bq_params, "service_account_path", None)
        bq_project_name = None if def_bq_project == "Y" else getattr(bq_params, "bigquery_project", None)

        query = getattr(bq_params, "query", None)
        if not query:
            raise ValueError("bq_params.query is required but missing or empty.")

        try:
            bq = bq_manager.BQManager(service_account_path, bq_project_name)
            logging.info(
                "BigQuery client initialized (project=%s, sa=%s)",
                bq_project_name or "<env default>",
                service_account_path or "<application default>",
            )
        except Exception as err:
            logging.error(f"Error opening bq_manager: {err}")
            raise

        value = bq.run_query_and_return_single_value(query)
        logging.info("Fetched value from BigQuery: %s", value)

        # Normalize to date when possible
        if isinstance(value, datetime):
            value = value.date()
        elif isinstance(value, str):
            try:
                value = datetime.strptime(value[:10], "%Y-%m-%d").date()
            except Exception:
                pass

        return value

    except Exception as e:
        logging.error("get_report_last_date failed: %s", e, exc_info=True)
        raise


def main():
    db_manager = None
    bq_params = None  # so we can reference safely in outer except

    try:
        # Create the db_manager
        logging.info("input_data=%s", 'provided' if 'input_data' in globals() else 'missing')
        secrets = secret_manager.SecretManager()
        meta_connection = secrets.get_meta_connection_from_secret(input_data.meta_db_secret_name)
        db_manager = database_manager.DatabaseManager(
            meta_connection.mysql_host,
            meta_connection.mysql_user,
            meta_connection.mysql_password,
            meta_connection.mysql_database
        )

        variables = db_manager.start_workflow_step_log(
            input_data.workflow_name,
            input_data.step_name,
            input_data.work_flow_log_id,
            input_data.additional_param
        )
        if variables.loc[variables['key'] == 'from_secret_list'].empty:
            variables.loc[len(variables)] = ['from_secret_list', '[]']

        from_secret_list = variables.loc[variables['key'] == 'from_secret_list', 'value'].values[0]
        from_secret_list = json.loads(from_secret_list)

        rest_params = RestParameters(
            rest_api_conn_secret=secrets.get_variable_value('rest_api_conn_secret', variables, from_secret_list),
            api_key=secrets.get_variable_value('api_key', variables, from_secret_list),
            base_url=secrets.get_variable_value('base_url', variables, from_secret_list),
            app_name=secrets.get_variable_value('app_name', variables, from_secret_list),
            app_os=secrets.get_variable_value('app_os', variables, from_secret_list),
            num_days=secrets.get_variable_value('num_days', variables, from_secret_list)
        )

        bq_params = bqParm(
            work_flow_step_log_id=secrets.get_variable_value('WorkFlow_Step_Log_id', variables, from_secret_list),
            service_account_path=secrets.get_variable_value('service_account_path', variables, from_secret_list),
            bigquery_project=secrets.get_variable_value('bigquery_project', variables, from_secret_list),
            get_key_file_from_sec=secrets.get_variable_value('get_key_file_from_sec', variables, from_secret_list),
            get_key_file_sec_name=secrets.get_variable_value('get_key_file_sec_name', variables, from_secret_list),
            def_bq_cred=secrets.get_variable_value('def_bq_cred', variables, from_secret_list),
            def_bq_project=secrets.get_variable_value('def_bq_project', variables, from_secret_list),
            clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list),
            query=secrets.get_variable_value('query', variables, from_secret_list),
        )

        gs_parm = GsParm(
            local_data_directory=secrets.get_variable_value('local_data_directory', variables, from_secret_list),
            clean_local_dir=secrets.get_variable_value('clean_local_dir', variables, from_secret_list),
            gs_bucket_name=secrets.get_variable_value('gs_bucket_name', variables, from_secret_list),
            gs_directory=secrets.get_variable_value('gs_directory', variables, from_secret_list),
            gs_directory_archive=secrets.get_variable_value('gs_directory_archive', variables, from_secret_list),
            gs_bucket_name_archive=secrets.get_variable_value('gs_bucket_name_archive', variables, from_secret_list),
            def_gs_cred=secrets.get_variable_value('def_gs_cred', variables, from_secret_list),
            def_gs_project=secrets.get_variable_value('def_gs_project', variables, from_secret_list),
            google_project_name=secrets.get_variable_value('google_project_name', variables, from_secret_list),
        )

        # Optionally pull SA key from Secret Manager and write to file
        if bq_params.get_key_file_from_sec == "Y":
            try:
                logging.info("Getting key from secret...")
                secret_content = secrets.fetch_secret(bq_params.get_key_file_sec_name)
                secrets.write_secret_to_file(secret_content, bq_params.service_account_path)
                logging.info("Service account key written.")
            except Exception as e:
                logging.error(f"Failed to process secret: {e}")
                db_manager.close_step_log(
                    input_data.workflow_name,
                    input_data.step_name,
                    input_data.work_flow_log_id,
                    bq_params.work_flow_step_log_id,
                    "failed",
                    f"{e}"
                )
                sys.exit(1)

        # 1) Get LastReportDt from BigQuery
        last_report_dt = get_report_last_date(bq_params)
        if not isinstance(last_report_dt, date):
            raise ValueError(f"LastReportDt is not a date: {last_report_dt!r}")

        # 2) Build and use the date list based on num_days
        try:
            num_days_int = int(rest_params.num_days)
        except Exception as e:
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            raise ValueError(f"num_days must be an integer, got: {rest_params.num_days!r}")

        try:
            date_payload = build_date_list_for_processing(
                last_report_dt=last_report_dt,
                num_days=num_days_int,
                tz_name="America/New_York"
            )

            dates_list = date_payload["dates"]
            LastReportDt = date_payload["LastReportDt"]
            StartDate = date_payload["StartDate"]
            EndDate = date_payload["EndDate"]
            logging.info(f"dates_list={dates_list}")
            logging.info(f"LastReportDt={LastReportDt}")
            logging.info(f"StartDate={StartDate}")
            logging.info(f"EndDate ={EndDate}")

        except Exception as e:
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            raise

        # 3) Init gsutil client (using gs_parm flags)
        if gs_parm.def_gs_cred == "Y":
            service_account_path = None
        else:
            service_account_path = bq_params.service_account_path

        if gs_parm.def_gs_project == "Y":
            google_project_name = None
        else:
            google_project_name = gs_parm.google_project_name

        try:
            gsutil = gsutil_manager.GSUtilClient(service_account_path, google_project_name)
            logging.info("gsutil OK!")
        except Exception as err:
            logging.error(f"Error initializing gsutil manager: {err}")
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{err}"
            )
            raise

        # 4) Archive/Clean GCS directory BEFORE loading files
        try:
            num_objects = gsutil.move_all_files_to_archive(
                gs_bucket_name=gs_parm.gs_bucket_name,
                gs_directory=gs_parm.gs_directory,
                gs_bucket_name_archive=gs_parm.gs_bucket_name_archive,
                gs_directory_archive=gs_parm.gs_directory_archive
            )
            logging.info(
                "Number of objects moved from %s/%s to %s/%s is %d",
                gs_parm.gs_bucket_name, gs_parm.gs_directory,
                gs_parm.gs_bucket_name_archive, gs_parm.gs_directory_archive,
                num_objects
            )
        except Exception as err:
            logging.error(f"Error archiving GCS files: {err}")
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{err}"
            )
            raise

        # 5) Download Applovin reports to local directory
        try:
            dl_result = download_applovin_reports_for_dates(
                date_payload=date_payload,
                api_key=rest_params.api_key,
                base_url=rest_params.base_url,
                app_name=rest_params.app_name,
                app_os=rest_params.app_os,
                local_directory=gs_parm.local_data_directory,
                timeout=60.0,
            )
            logging.info("Download summary: %s", dl_result["summary"])
            for r in dl_result["results"]:
                logging.info(
                    "date=%s exist=%s code=%s downloaded=%s path=%s error=%s",
                    r.get("date"), r.get("report_exist"), r.get("return_code"),
                    r.get("downloaded"), r.get("download_path"), r.get("error")
                )
        except Exception as e:
            logging.error("Failed to download Applovin reports: %s", e)
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            raise

        # 6) Upload downloaded files to GCS + create dataset instances (Option A)
        try:
            uploaded = 0
            upload_errors = 0

            # (A) Create Destination entries for EVERY date/url you attempted
            for r in dl_result.get("results", []):
                json_destination = {
                    "date": r.get("date"),
                    "request_url": r.get("request_url"),
                }
                logging.info("Destination JSON = %s", json.dumps(json_destination))
                db_manager.create_dataset_instance(
                    input_data.work_flow_log_id, bq_params.work_flow_step_log_id,
                    input_data.step_name, "Destination", json_destination
                )

            # (B) Upload each successfully-downloaded file and create Successor entry
            for r in dl_result.get("results", []):
                if r.get("downloaded") and r.get("download_path"):
                    local_path = r["download_path"]
                    try:
                        gsutil.push_file_to_gs_util(
                            gs_bucket_name=gs_parm.gs_bucket_name,
                            gs_directory=gs_parm.gs_directory,   # destination prefix
                            local_temp_file=local_path,
                        )
                        r["uploaded"] = True
                        uploaded += 1
                        logging.info(
                            "Uploaded %s to gs://%s/%s",
                            local_path, gs_parm.gs_bucket_name, gs_parm.gs_directory
                        )

                        # Build canonical GCS object path (dir + basename)
                        filename_base = posixpath.basename(local_path)
                        dir_norm = (gs_parm.gs_directory or "").rstrip("/")
                        filename_full = f"{dir_norm}/{filename_base}" if dir_norm else filename_base

                        json_data_successor = {
                            "filename_full": filename_full,               # e.g. reports/2025-09-09.csv
                            "filename_base": filename_base,               # e.g. 2025-09-09.csv
                            "gs_bucket_name": gs_parm.gs_bucket_name      # e.g. my-bucket
                        }
                        logging.info("Successor JSON = %s", json.dumps(json_data_successor))
                        db_manager.create_dataset_instance(
                            input_data.work_flow_log_id, bq_params.work_flow_step_log_id,
                            input_data.step_name, "Successor", json_data_successor
                        )

                    except Exception as up_err:
                        r["uploaded"] = False
                        r["upload_error"] = str(up_err)
                        upload_errors += 1
                        logging.error("Upload failed for %s: %s", local_path, up_err)

            logging.info("Upload summary: uploaded=%d, errors=%d", uploaded, upload_errors)

        except Exception as e:
            logging.error("Unexpected failure during upload to GCS: %s", e)
            db_manager.close_step_log(
                input_data.workflow_name,
                input_data.step_name,
                input_data.work_flow_log_id,
                bq_params.work_flow_step_log_id,
                "failed",
                f"{e}"
            )
            raise

        db_manager.close_step_log(
            input_data.workflow_name, input_data.step_name, input_data.work_flow_log_id,
            bq_params.work_flow_step_log_id, "success", "all_done"
        )

    except Exception as e:
        logging.error("Unexpected failure in main(): %s", e, exc_info=True)
        # Best-effort close step log if we have enough context
        try:
            if db_manager and bq_params:
                db_manager.close_step_log(
                    input_data.workflow_name,
                    input_data.step_name,
                    input_data.work_flow_log_id,
                    getattr(bq_params, "work_flow_step_log_id", None),
                    "failed",
                    f"{e}"
                )
        except Exception as suppress:
            logging.warning("Unable to close step log on failure: %s", suppress)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input JSON and fetch workflow data.")
    parser.add_argument("--result", required=True, help="Input JSON in dictionary format")
    parser.add_argument("--workflow_name", required=True, help="Name of workflow executing")
    parser.add_argument("--step_name", required=True, help="Name of step to execute")

    args = parser.parse_args()

    try:
        input_dict = json.loads(args.result)  # parsed JSON pushed from parent
        input_data = InputParams(
            meta_db_secret_name=input_dict['meta_db_secret_name'],
            workflow_name=args.workflow_name,
            step_name=args.step_name,
            work_flow_log_id=input_dict['WorkFlow_Log_id'],
            additional_param=input_dict.get('additional_param')
        )
        logging.info("Parsed input JSON: %s", input_data)
    except json.JSONDecodeError as json_err:
        logging.error(f"Invalid JSON input: {json_err}")
        sys.exit(1)

    main()
