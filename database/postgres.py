from typing import List, Optional, Tuple
from urllib.parse import urlparse

from database.database import Database
import psycopg2


class Postgres(Database):
    def __init__(self, database: str, user: str, password: str, host: str, port: str) -> None:
        self._database: str = database
        self._user: str = user
        self._password: str = password
        self._host: str = host
        self._port: str = port

        # Do a sanity connection check to the database
        conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                     password=self._password, host=self._host, port=self._port)
        conn.close()

    def initialize_job(self, job_id: int, source: str) -> None:
        conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                     password=self._password, host=self._host, port=self._port)
        cur: psycopg2.cursor = conn.cursor()

        # Create table for URLs if such a table does not exist already
        cur.execute(
            "CREATE TABLE IF NOT EXISTS URLS (job INT NOT NULL, url VARCHAR(255) NOT NULL UNIQUE, crawler INT, code INT);")

        # Check if job already exists
        cur.execute(f"SELECT * FROM URLS WHERE job={job_id} LIMIT 1;")
        if cur.fetchone():
            raise RuntimeError('Job already exists.')

        # Parse file with path <source>, where each line represents a single URL
        with open(source, mode='r') as file:
            for line in file:
                urlparse(line)  # Do a sanity check on the url
                cur.execute(
                    f"INSERT INTO URLS VALUES ({job_id}, '{line.strip()}');")

        conn.commit()
        conn.close()

    def get_url(self, job_id: int, crawler_id: int) -> Optional[str]:
        conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                     password=self._password, host=self._host, port=self._port)
        cur: psycopg2.cursor = conn.cursor()

        # Check if job exists
        cur.execute(f"SELECT * FROM URLS WHERE job={job_id} LIMIT 1;")
        if not cur.fetchone():
            raise RuntimeError('Job doesn\'t exists')

        # Get a URL with no crawler and lock row to avoid race conditions
        cur.execute(
            f"SELECT url FROM URLS WHERE job={job_id} AND crawler IS NULL FOR UPDATE SKIP LOCKED LIMIT 1;")
        url: Tuple = cur.fetchone()

        # Check if there is a URL returned
        if not url:
            return None

        # Get result from URL and assign crawler to it
        url: str = url[0]
        cur.execute(
            f"UPDATE URLS SET crawler={crawler_id} WHERE job={job_id} AND url='{url}';")

        conn.commit()
        conn.close()
        return url

    def update_url(self, job_id: int, crawler_id: int, url: str, code: int) -> None:
        conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                     password=self._password, host=self._host, port=self._port)
        cur: psycopg2.cursor = conn.cursor()

        cur.execute(
            f"UPDATE URLS SET code={code} WHERE job={job_id} AND url='{url}' AND crawler={crawler_id};")

        conn.commit()
        conn.close()

    def invoke_transaction(self, statement: str) -> Optional[List[Tuple]]:
        conn: psycopg2.connection = psycopg2.connect(database=self._database, user=self._user,
                                                     password=self._password, host=self._host, port=self._port)
        cur: psycopg2.cursor = conn.cursor()

        cur.execute(statement)
        data = cur.fetchall()

        conn.close()
        conn.commit()
        return data
