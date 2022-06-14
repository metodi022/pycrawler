from typing import Optional, Tuple, List, Any

import psycopg2

from loader.loader import Loader


class Postgres:
    def __init__(self, database: str, user: str, password: str, host: str, port: str) -> None:
        self._database: str = database
        self._user: str = user
        self._password: str = password
        self._host: str = host
        self._port: str = port

        self._conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                           password=self._password, host=self._host, port=self._port)

    def disconnect(self) -> None:
        self._conn = self._conn.close()

    def register_job(self, job_id: int, loader: Loader) -> None:
        cur: psycopg2.cursor = self._conn.cursor()

        # Create table for URLs if such a table does not exist already
        cur.execute(
            "CREATE TABLE IF NOT EXISTS URLS (rank INT NOT NULL, job INT NOT NULL, crawler INT, "
            "url VARCHAR(255) NOT NULL UNIQUE, finalurl TEXT, code INT);")

        # Check if job already exists
        if Postgres._job_exists(cur, job_id):
            cur.close()
            self.disconnect()
            raise RuntimeError('Job already exists.')

        for entry in loader:
            cur.execute(f"INSERT INTO URLS (rank, job, url) VALUES (%s, %s, %s);",
                        (entry[0], job_id, entry[1].strip(),))

        self._conn.commit()
        cur.close()

    def get_url(self, job_id: int, crawler_id: int) -> Optional[Tuple[str, int, int]]:
        cur: psycopg2.cursor = self._conn.cursor()

        # Check if job exists
        if not Postgres._job_exists(cur, job_id):
            cur.close()
            self.disconnect()
            raise RuntimeError('Job does not exists.')

        # Get a URL with no crawler and lock row to avoid race conditions
        cur.execute(f"SELECT url, rank FROM URLS WHERE job=%s AND crawler IS NULL FOR UPDATE SKIP LOCKED LIMIT 1;",
                    (job_id,))
        url: Optional[Tuple[str, int]] = cur.fetchone()
        url: Optional[Tuple[str, int, int]] = (url[0], 0, url[1]) if url else url

        # Check if there is a URL returned
        if not url:
            cur.close()
            return None

        # Get result from URL and assign crawler to it
        cur.execute(f"UPDATE URLS SET crawler=%s WHERE job=%s AND url=%s;", (crawler_id, job_id, url[0]))

        self._conn.commit()
        cur.close()
        return url

    def update_url(self, job_id: int, crawler_id: int, url: str, final_url: str, code: int) -> None:
        cur: psycopg2.cursor = self._conn.cursor()

        # Check if job exists
        if not Postgres._job_exists(cur, job_id):
            cur.close()
            self.disconnect()
            raise RuntimeError('Job does not exists.')

        cur.execute(f"UPDATE URLS SET code=%s, finalurl=%s, WHERE job=%s AND url=%s AND crawler=%s;",
                    (code, final_url, job_id, url, crawler_id,))

        self._conn.commit()
        cur.close()

    def invoke_transaction(self, transaction: str, values: Any, fetch: bool) -> Optional[List[Tuple[Any, ...]]]:
        cur: psycopg2.cursor = self._conn.cursor()

        cur.execute(transaction, values)
        data: Optional[List[Tuple[Any, ...]]] = cur.fetchall() if fetch else []

        self._conn.commit()
        cur.close()
        return data

    @staticmethod
    def _job_exists(cur, job_id: int) -> bool:
        cur.execute(f"SELECT * FROM URLS WHERE job=%s LIMIT 1;", (job_id,))
        return bool(cur.fetchone())
