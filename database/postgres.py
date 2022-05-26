from typing import Optional, Tuple
from urllib.parse import urlparse

from database.database import Database
import psycopg2


class Postgres(Database):
    def __init__(self, database: str, user: str, password: str, host: str, port: str) -> None:
        self.database: str = database
        self.user: str = user
        self._password: str = password
        self.host: str = host
        self.port: str = port

        # Do a sanity connection check to the database
        conn: psycopg2.connection = psycopg2.connect(database=self.database, user=self.user,
                                                     password=self._password, host=self.host, port=self.port)
        conn.close()

    def initialize_job(self, job_id: int, source: str) -> None:
        conn: psycopg2.connection = psycopg2.connect(database=self.database, user=self.user,
                                                     password=self._password, host=self.host, port=self.port)
        cur: psycopg2.cursor = conn.cursor()

        # Create table for URLs if such a table does not exist already
        cur.execute(
            "CREATE TABLE IF NOT EXISTS URLS (job INT NOT NULL, url VARCHAR(255) NOT NULL UNIQUE, depth INT NOT NULL, crawler INT, code INT);")

        # Check if job already exists
        if self._job_exists(cur, job_id):
            conn.close()
            raise RuntimeError('Job already exists.')

        # Parse file with path <source>, where each line represents a single URL
        with open(source, mode='r') as file:
            for line in file:
                urlparse(line)  # Do a sanity check on the URL
                cur.execute(
                    f"INSERT INTO URLS VALUES ({job_id}, '{line.strip()}', 0);")

        conn.commit()
        conn.close()

    def get_url(self, job_id: int, crawler_id: int) -> Optional[Tuple[str, int]]:
        conn: psycopg2.connection = psycopg2.connect(database=self.database, user=self.user,
                                                     password=self._password, host=self.host, port=self.port)
        cur: psycopg2.cursor = conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            conn.close()
            raise RuntimeError('Job does not exists.')

        # Get a URL with no crawler and lock row to avoid race conditions
        cur.execute(
            f"SELECT url, depth FROM URLS WHERE job={job_id} AND crawler IS NULL FOR UPDATE SKIP LOCKED LIMIT 1;")
        url: Optional[Tuple[str, int]] = cur.fetchone()

        # Check if there is a URL returned
        if not url:
            conn.close()
            return None

        # Get result from URL and assign crawler to it
        cur.execute(
            f"UPDATE URLS SET crawler={crawler_id} WHERE job={job_id} AND url='{url[0]}';")

        conn.commit()
        conn.close()
        return url

    def add_url(self, job_id: int, url: str, depth: int) -> None:
        conn: psycopg2.connection = psycopg2.connect(database=self.database, user=self.user,
                                                     password=self._password, host=self.host, port=self.port)
        cur: psycopg2.cursor = conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            conn.close()
            raise RuntimeError('Job does not exists.')

        cur.execute(f"INSERT INTO URLS VALUES ({job_id}, '{url}', {depth});")

        conn.commit()
        conn.close()

    def update_url(self, job_id: int, crawler_id: int, url: str, code: int) -> None:
        conn: psycopg2.connection = psycopg2.connect(database=self.database, user=self.user,
                                                     password=self._password, host=self.host, port=self.port)
        cur: psycopg2.cursor = conn.cursor()

        # Check if job exists
        if not self._job_exists(cur, job_id):
            conn.close()
            raise RuntimeError('Job does not exists.')

        cur.execute(
            f"UPDATE URLS SET code={code} WHERE job={job_id} AND url='{url}' AND crawler={crawler_id};")

        conn.commit()
        conn.close()

    def _job_exists(self, cur: psycopg2.cursor, job_id: int) -> bool:
        # Check if job already exists
        cur.execute(f"SELECT * FROM URLS WHERE job={job_id} LIMIT 1;")
        return bool(cur.fetchone())
