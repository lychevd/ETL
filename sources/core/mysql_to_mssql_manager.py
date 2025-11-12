import logging
from typing import Iterable, List, Tuple, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
import pyodbc

log = logging.getLogger("transfer")


# ---------- module helpers ----------

def _strip_semicolon(sql: str) -> str:
    return sql.rstrip().rstrip(";")


def _mssql_execmany(
    conn: pyodbc.Connection,
    insert_sql: str,
    rows: Iterable[Tuple],
    batch: int = 1000,
    use_fast_executemany: bool = True,
    verify_rowcount: bool = True,
) -> int:
    """
    Execute batched INSERTs to MSSQL and return the reported total inserted.
    """
    cur = conn.cursor()
    buf: List[Tuple] = []
    total = 0
    try:
        cur.fast_executemany = bool(use_fast_executemany)

        for row in rows:
            buf.append(row)
            if len(buf) >= batch:
                log.info("MSSQL: Executing batch with %d rows", len(buf))
                cur.executemany(insert_sql, buf)
                inserted = (
                    cur.rowcount
                    if cur.rowcount is not None and cur.rowcount >= 0
                    else len(buf)
                )
                if verify_rowcount and inserted < len(buf):
                    log.warning(
                        "MSSQL: Rowcount(%d) < batch(%d) — duplicates/constraints/IGNORE_DUP_KEY?",
                        inserted, len(buf),
                    )
                total += inserted
                buf.clear()

        if buf:
            log.info("MSSQL: Executing final batch with %d rows", len(buf))
            cur.executemany(insert_sql, buf)
            inserted = (
                cur.rowcount
                if cur.rowcount is not None and cur.rowcount >= 0
                else len(buf)
            )
            if verify_rowcount and inserted < len(buf):
                log.warning(
                    "MSSQL: Rowcount(%d) < batch(%d) on final batch.", inserted, len(buf)
                )
            total += inserted

        conn.commit()
        log.info("MSSQL: Total rows reported inserted: %s", total)
        return total
    except pyodbc.Error as e:
        log.error("MSSQL insertion failed: %s", e)
        raise
    finally:
        cur.close()


# ---------- main class ----------

class MySQLToMSSQLSA:
    """
    MySQL -> MSSQL transfer using SQLAlchemy (source) and pyodbc (dest).
    Adds a 'stable count' polling step before reading, to avoid fetching a half-written batch.
    """

    def __init__(
        self,
        mysql_host: str,
        mysql_db: str,
        mysql_user: str,
        mysql_password: str,
        mysql_port: int = 3306,
        *,
        mysql_driver: str = "mysqlconnector",  # "mysqlconnector" | "pymysql" | "mysqldb" | "mariadb" | "pyodbc"
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_pre_ping: bool = True,
        force_writer: bool = True,
        mssql_host: str = "",
        mssql_db: str = "",
        mssql_user: str = "",
        mssql_password: str = "",
        mssql_port: int = 1433,
        mssql_driver: str = "{ODBC Driver 17 for SQL Server}",
        mssql_fast_executemany: bool = True,
        mssql_verify_rowcount: bool = True,
    ) -> None:

        def build_mysql_url(driver: str) -> str:
            u = quote_plus(mysql_user)
            p = quote_plus(mysql_password)
            h = mysql_host
            prt = mysql_port
            db = mysql_db

            if driver == "mysqlconnector":
                return f"mysql+mysqlconnector://{u}:{p}@{h}:{prt}/{db}"
            elif driver == "pymysql":
                return f"mysql+pymysql://{u}:{p}@{h}:{prt}/{db}?charset=utf8mb4"
            elif driver == "mysqldb":
                return f"mysql+mysqldb://{u}:{p}@{h}:{prt}/{db}?charset=utf8mb4"
            elif driver == "mariadb":
                return f"mysql+mariadb://{u}:{p}@{h}:{prt}/{db}?charset=utf8mb4"
            elif driver == "pyodbc":
                drv = quote_plus("MySQL ODBC 8.0 Unicode Driver")
                return f"mysql+pyodbc://{u}:{p}@{h}:{prt}/{db}?driver={drv}"
            else:
                raise ValueError(f"Unsupported mysql_driver: {driver}")

        url = build_mysql_url(mysql_driver)
        log.info("MySQL URL: %s", url)

        self.mysql_engine: Engine = create_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=pool_pre_ping,
            future=True,
            pool_recycle=3200,  # keep under common 3600s wait_timeout
        )

        # MSSQL knobs
        self.mssql_fast_executemany = mssql_fast_executemany
        self.mssql_verify_rowcount = mssql_verify_rowcount

        self._ensure_mysql_fresh_session()
        self._mysql_diag("initial")

        if force_writer and self._mysql_is_read_only():
            raise RuntimeError("Connected MySQL server is read-only (likely a replica).")

        self.mssql_conn: pyodbc.Connection = pyodbc.connect(
            f"DRIVER={mssql_driver};SERVER={mssql_host},{mssql_port};"
            f"DATABASE={mssql_db};UID={mssql_user};PWD={mssql_password}",
            autocommit=False,
        )

    # ---------- MySQL helpers ----------

    def _wait_for_stable_count(
        self,
        conn,
        wrapped_sql: str,
        stable_for: int = 5,
        interval: int = 1,
        max_wait: int = 600,
    ) -> int:
        """
        Poll COUNT(*) until it stays the same for `stable_for` seconds.
        Returns the final stable count (or last observed count if timed out).
        """
        import time
        last: Optional[int] = None
        same_for = 0
        waited = 0

        while waited <= max_wait:
            cnt = int(conn.execute(text(f"SELECT COUNT(*) FROM ({wrapped_sql}) subq")).scalar_one())
            log.info("StableCount: observed=%d last=%s same_for=%ds waited=%ds",
                     cnt, last, same_for, waited)

            if cnt == last:
                same_for += interval
                if same_for >= stable_for:
                    log.info("StableCount: stabilized at %d for %ds", cnt, stable_for)
                    return cnt
            else:
                same_for = 0
                last = cnt

            time.sleep(interval)
            waited += interval

        log.warning("StableCount: timed out at %s after %ds (stable_for=%ds, interval=%ds)",
                    last, max_wait, stable_for, interval)
        return last if last is not None else 0

    def _wrap_unlimited(self, select_sql: str) -> str:
        """
        Neutralize any session SQL_SELECT_LIMIT by wrapping and applying MySQL's 'infinite' LIMIT.
        """
        core = _strip_semicolon(select_sql)
        return f"SELECT * FROM ({core}) AS _x LIMIT 18446744073709551615"

    def _ensure_mysql_fresh_session(self) -> None:
        with self.mysql_engine.connect() as c:
            c.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            c.execute(text("SET SESSION wait_timeout = 3600"))
            c.execute(text("SET autocommit = 1"))

            # Prevent silent caps / large-join aborts
            c.execute(text("SET SESSION SQL_SELECT_LIMIT = DEFAULT"))
            c.execute(text("SET SESSION SQL_BIG_SELECTS = 1"))

            # More resilient I/O on big reads
            c.execute(text("SET SESSION net_read_timeout = 6000"))
            c.execute(text("SET SESSION net_write_timeout = 6000"))

            res: Result = c.execute(text("""
                SELECT @@autocommit, @@transaction_isolation, @@wait_timeout,
                       @@session.sql_select_limit, @@sql_big_selects, @@max_join_size
            """))
            log.info("MySQL session state: %s", res.fetchall())

    def _mysql_diag(self, label: str) -> None:
        try:
            with self.mysql_engine.connect() as c:
                c.execute(text("SET autocommit = 1"))
                res: Result = c.execute(text(
                    "SELECT DATABASE(), USER(), @@hostname, @@server_uuid, @@read_only, @@version"
                ))
                db, user, host, uuid, ro, ver = res.fetchone()
                log.info(
                    "MySQL ctx (%s): db=%s user=%s host=%s uuid=%s read_only=%s version=%s",
                    label, db, user, host, uuid, ro, ver
                )
                try:
                    res2: Result = c.execute(text("SELECT @@global.gtid_executed"))
                    log.info("MySQL ctx (%s): gtid_executed=%s", label, res2.scalar())
                except Exception:
                    log.warning("Could not retrieve gtid_executed (normal in some setups).")
        except Exception as e:
            log.warning("MySQL diag failed (%s): %s", label, e)

    def _mysql_is_read_only(self) -> bool:
        with self.mysql_engine.connect() as c:
            c.execute(text("SET autocommit = 1"))
            ro = c.execute(text("SELECT @@read_only")).scalar_one()
            return int(ro) == 1

    def _count_subquery(self, select_sql: str) -> int:
        """
        Kept for optional use; not used by stabilized paths.
        """
        select_sql = self._wrap_unlimited(select_sql)
        count_sql = f"SELECT COUNT(*) FROM ({select_sql}) AS subq"
        with self.mysql_engine.connect() as c:
            c.execute(text("SET autocommit = 1"))
            log.info("MySQL COUNT(*): Executing %s", count_sql)
            return int(c.execute(text(count_sql)).scalar_one())

    def _preview_rows(self, select_sql: str, n: int = 10) -> list[Tuple]:
        sql = _strip_semicolon(select_sql) + f" LIMIT {int(n)}"
        with self.mysql_engine.connect() as c:
            c.execute(text("SET autocommit = 1"))
            log.info("MySQL Preview: Executing %s", sql)
            rows = c.execute(text(sql)).fetchall()
            return [tuple(r) for r in rows]

    def _stream_rows(self, select_sql: str, arraysize: int = 5000, conn=None) -> Iterable[Tuple]:
        """
        Stream rows from MySQL. If `conn` is provided, it uses that connection (recommended
        so that the stable count and the stream share the same session).
        """
        select_sql = self._wrap_unlimited(select_sql)
        log.info("MySQL Stream: Executing query: %s", select_sql)
        try:
            if conn is None:
                ctx = self.mysql_engine.connect()
                c = ctx.__enter__()  # manual context mgmt
                manage_ctx = True
            else:
                c = conn
                manage_ctx = False

            c.execute(text("SET autocommit = 1"))
            res: Result = c.execution_options(stream_results=True).execute(text(select_sql))
            rows_fetched_in_stream = 0
            while True:
                batch = res.fetchmany(arraysize)
                if not batch:
                    log.info("MySQL Stream: No more batches. Total rows fetched: %s", rows_fetched_in_stream)
                    break
                rows_fetched_in_stream += len(batch)
                log.info("MySQL Stream: Fetched %d rows. Total so far: %d", len(batch), rows_fetched_in_stream)
                for row in batch:
                    yield tuple(row)
        except Exception as e:
            log.error("MySQL streaming failed: %s", e)
            raise
        finally:
            if 'manage_ctx' in locals() and manage_ctx:
                ctx.__exit__(None, None, None)

    # ---------- Public API ----------

    def close(self) -> None:
        try:
            self.mssql_conn.close()
        except Exception:
            pass
        try:
            self.mysql_engine.dispose()
        except Exception:
            pass

    def transfer(
        self,
        select_sql: str,
        insert_sql: str,
        preview_rows: int = 10,
        chunk_size: int = 5000,
        batch_size: int = 1000,
        *,
        stable_for_seconds: int = 60,
        poll_interval_seconds: int = 1,
        max_wait_seconds: int = 600,
    ) -> int:
        """
        Streaming transfer with pre-read stabilization (count must stop changing).
        """
        if "|||" in select_sql:
            raise ValueError(f"Unresolved placeholder in select_sql: {select_sql}")

        self._ensure_mysql_fresh_session()
        self._mysql_diag("pre-select")

        # Peek
        try:
            preview = self._preview_rows(select_sql, n=preview_rows)
            for i, r in enumerate(preview, 1):
                log.info("[Preview %d] %s", i, r)
        except Exception as e:
            log.warning("Preview failed: %s", e)

        wrapped = self._wrap_unlimited(select_sql)

        total = 0
        try:
            with self.mysql_engine.connect() as c:
                c.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                c.execute(text("SET autocommit = 1"))

                expected = self._wait_for_stable_count(
                    c,
                    wrapped,
                    stable_for=stable_for_seconds,
                    interval=poll_interval_seconds,
                    max_wait=max_wait_seconds,
                )
                log.info("Stable count reached before streaming: %s", expected)

                rows_iter = self._stream_rows(select_sql, arraysize=chunk_size, conn=c)
                total = _mssql_execmany(
                    self.mssql_conn,
                    insert_sql,
                    rows_iter,
                    batch=batch_size,
                    use_fast_executemany=self.mssql_fast_executemany,
                    verify_rowcount=self.mssql_verify_rowcount,
                )
        except Exception as e:
            log.error("Streaming transfer failed: %s", e)
            raise

        self._mysql_diag("post-insert")

        if expected >= 0 and expected != total:
            log.warning("Row count drift: stable_count=%s streamed_inserted=%s", expected, total)
        return total

    def transfer_fetchall(
        self,
        select_sql: str,
        insert_sql: str,
        preview_rows: int = 10,
        batch_size: int = 1000,
        *,
        stable_for_seconds: int = 60,
        poll_interval_seconds: int = 1,
        max_wait_seconds: int = 600,
    ) -> int:
        """
        Fetch-all transfer with pre-read stabilization (count must stop changing),
        then batch-insert into MSSQL.
        """
        if "|||" in select_sql:
            raise ValueError(f"Unresolved placeholder in select_sql: {select_sql}")

        self._ensure_mysql_fresh_session()
        self._mysql_diag("pre-select")

        # Peek
        try:
            preview = self._preview_rows(select_sql, n=preview_rows)
            for i, r in enumerate(preview, 1):
                log.info("[Preview %d] %s", i, r)
        except Exception as e:
            log.warning("Preview failed: %s", e)

        rows: list[Tuple] = []
        wrapped = self._wrap_unlimited(select_sql)

        log.info("Waiting for stable count before fetch (stable_for=%ss, interval=%ss, max_wait=%ss)...",
                 stable_for_seconds, poll_interval_seconds, max_wait_seconds)

        try:
            with self.mysql_engine.connect() as c:
                c.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                c.execute(text("SET autocommit = 1"))

                expected = self._wait_for_stable_count(
                    c,
                    wrapped,
                    stable_for=stable_for_seconds,
                    interval=poll_interval_seconds,
                    max_wait=max_wait_seconds,
                )

                log.info("Fetching ALL rows from MySQL into memory (unlimited wrap)...")
                res: Result = c.execute(text(wrapped))
                rows = [tuple(r) for r in res.fetchall()]
                log.info("MySQL fetchall complete: %d rows (stable count was %d)", len(rows), expected)
        except Exception as e:
            log.error("MySQL fetchall failed: %s", e)
            raise

        total = _mssql_execmany(
            self.mssql_conn,
            insert_sql,
            rows,
            batch=batch_size,
            use_fast_executemany=self.mssql_fast_executemany,
            verify_rowcount=self.mssql_verify_rowcount,
        )
        log.info("Inserted rows: %s", total)

        self._mysql_diag("post-insert")

        if expected >= 0 and expected != len(rows):
            log.warning("Row count drift: stable_count=%s fetched=%s", expected, len(rows))
        if len(rows) != total:
            log.warning("Destination difference: fetched=%s inserted=%s (duplicates/constraints/triggers?)",
                        len(rows), total)

        return total

    def transfer_fetchone(
        self,
        select_sql: str,
        insert_sql: str,
        preview_rows: int = 10,
        batch_size: int = 1000,
        *,
        stable_for_seconds: int = 60,
        poll_interval_seconds: int = 1,
        max_wait_seconds: int = 600,
    ) -> int:
        """
        Fetch-one-at-a-time transfer with pre-read stabilization.
        Useful fallback if streaming/fetchall have driver-specific issues.
        """
        if "|||" in select_sql:
            raise ValueError(f"Unresolved placeholder in select_sql: {select_sql}")

        log.info("=== TRANSFER MODE: FETCHONE (with stable count) ===")
        self._ensure_mysql_fresh_session()
        self._mysql_diag("pre-select")

        # Peek
        try:
            preview = self._preview_rows(select_sql, n=preview_rows)
            for i, r in enumerate(preview, 1):
                log.info("[Preview %d] %s", i, r)
        except Exception as e:
            log.warning("Preview failed: %s", e)

        wrapped = self._wrap_unlimited(select_sql)
        buf: list[Tuple] = []
        total_inserted = 0
        fetched = 0

        try:
            with self.mysql_engine.connect() as c:
                c.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                c.execute(text("SET autocommit = 1"))

                expected = self._wait_for_stable_count(
                    c,
                    wrapped,
                    stable_for=stable_for_seconds,
                    interval=poll_interval_seconds,
                    max_wait=max_wait_seconds,
                )
                log.info("Stable count before fetchone: %s", expected)

                res: Result = c.execute(text(wrapped))
                while True:
                    row = res.fetchone()
                    if row is None:
                        break
                    fetched += 1
                    buf.append(tuple(row))
                    if len(buf) >= batch_size:
                        inserted = _mssql_execmany(
                            self.mssql_conn,
                            insert_sql,
                            buf,
                            batch=batch_size,
                            use_fast_executemany=self.mssql_fast_executemany,
                            verify_rowcount=self.mssql_verify_rowcount,
                        )
                        total_inserted += inserted
                        buf.clear()

                if buf:
                    inserted = _mssql_execmany(
                        self.mssql_conn,
                        insert_sql,
                        buf,
                        batch=batch_size,
                        use_fast_executemany=self.mssql_fast_executemany,
                        verify_rowcount=self.mssql_verify_rowcount,
                    )
                    total_inserted += inserted
                    buf.clear()

        except Exception as e:
            log.error("MySQL fetchone failed: %s", e)
            raise

        self._mysql_diag("post-insert")

        if expected >= 0 and expected != fetched:
            log.warning("Row count drift: stable_count=%s fetched=%s", expected, fetched)
        if fetched != total_inserted:
            log.warning("Destination difference: fetched=%s inserted=%s (duplicates/constraints/triggers?)",
                        fetched, total_inserted)

        return total_inserted
