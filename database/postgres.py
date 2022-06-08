from typing import Optional, Tuple, List, Any
from urllib.parse import urlparse

import psycopg2


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

    def initialize_job(self, job_id: int, source: str) -> None:
        cur: psycopg2.cursor = self._conn.cursor()

        # Create table for URLs if such a table does not exist already
        cur.execute(
            "CREATE TABLE IF NOT EXISTS URLS (job INT NOT NULL, url VARCHAR(255) NOT NULL UNIQUE, crawler INT, "
            "code INT);")

        # Check if job already exists
        if self._job_exists(cur, job_id):
            self.disconnect()
            raise RuntimeError('Job already exists.')

        # Parse file with path <source>, where each line represents a single URL
        # TODO better
        with open(source, mode='r') as file:
            for line in file:
                urlparse(line)  # Do a sanity check on the URL
                cur.execute(f"INSERT INTO URLS VALUES (%i, %s);", (job_id, line.strip(),))

        self._conn.commit()

    def get_url(self, job_id: int, crawler_id: int) -> Optional[Tuple[str, int]]:
        cur: psycopg2.cursor = self._conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            self.disconnect()
            raise RuntimeError('Job does not exists.')

        # Get a URL with no crawler and lock row to avoid race conditions
        cur.execute(f"SELECT url FROM URLS WHERE job=%i AND crawler IS NULL FOR UPDATE SKIP LOCKED LIMIT 1;", (job_id,))
        url: Optional[Tuple[str, int]] = cur.fetchone()
        url = (url[0], 0) if url is not None else url

        # Check if there is a URL returned
        if not url:
            return None

        # Get result from URL and assign crawler to it
        cur.execute(f"UPDATE URLS SET crawler=%i WHERE job=%i AND url=%s;", (crawler_id, job_id, url[0]))

        self._conn.commit()
        return url

    def add_url(self, job_id: int, url: str, depth: int) -> None:
        cur: psycopg2.cursor = self._conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            self.disconnect()
            raise RuntimeError('Job does not exists.')

        cur.execute(f"INSERT INTO URLS VALUES (%i, %s, %i);", (job_id, url, depth,))

        self._conn.commit()

    def update_url(self, job_id: int, crawler_id: int, url: str, code: int) -> None:
        cur: psycopg2.cursor = self._conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            self.disconnect()
            raise RuntimeError('Job does not exists.')

        cur.execute(f"UPDATE URLS SET code=%i WHERE job=%i AND url=%s AND crawler=%i;",
                    (code, job_id, url, crawler_id,))

        self._conn.commit()

    def invoke_transaction(self, transaction: str) -> Optional[List[Any]]:
        cur: psycopg2.cursor = self._conn.cursor()

        cur.execute(transaction)
        data: List[Tuple[Any]] = cur.fetchall()

        self._conn.commit()
        return data

    def _job_exists(self, cur, job_id: int) -> bool:
        # Check if job already exists
        cur.execute(f"SELECT * FROM URLS WHERE job=%i LIMIT 1;", (job_id,))
        return bool(cur.fetchone())
