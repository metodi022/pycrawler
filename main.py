import sys
from logging import Logger, FileHandler, Formatter

from chromiumcrawler import ChromiumCrawler
from config import Config
from database.postgres import Postgres


def main() -> int:
    handler = FileHandler('D:\\Programming\\pycrawler\\job1crawler1.txt')
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger('Job 1 ChromiumCrawler 1')
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    database: Postgres = Postgres('test', 'postgres', 'postgres', 'localhost', '5432')

    """
    loader: Loader = CSVLoader('D:\\Programming\\pycrawler\\urls.csv')
    database.register_job(1, loader)

    if Config.RECURSIVE:
        CollectUrls.register_job(database, log)
    """

    crawler = ChromiumCrawler(1, 1, Config, database, log, [])
    crawler.start_crawl_chromium()

    return 0


if __name__ == '__main__':
    sys.exit(main())
