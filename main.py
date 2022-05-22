import sys
from database.database import Database
from database.postgres import Postgres
from log.log import Log
from log.filelog import FileLog
from crawler import Crawler
from config import Config
from modules.testmodule import TestModule


def main() -> int:
    database: Database = Postgres(
        'test', 'postgres', 'postgres', 'localhost', '5432')
    #database.initialize_job(1, 'D:\\Programming\\logincrawler\\urls.txt')
    log: Log = FileLog(1, 1, 'D:\\Programming\\logincrawler\\crawler1.txt')
    crawler = Crawler(1, 1, Config, database, log, TestModule)
    crawler.start_crawl_chromium()
    return 0


if __name__ == '__main__':
    sys.exit(main())
