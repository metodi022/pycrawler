import sys
from logging import Logger, FileHandler, Formatter

from chromiumcrawler import ChromiumCrawler
from config import Config
from database.postgres import Postgres
from modules.empty import Empty


def main() -> int:
    database: Postgres = Postgres('test', 'postgres', 'postgres', 'localhost', '5432')
    # database.initialize_job(1, 'D:\\Programming\\pycrawler\\urls.txt')
    handler = FileHandler('D:\\Programming\\pycrawler\\job1crawler1.txt')
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger('Job 1 ChromiumCrawler 1')
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)
    crawler = ChromiumCrawler(1, 1, Config, database, log, [
        Empty(1, 1, Config, database, log)])
    crawler.start_crawl_chromium()

    return 0


if __name__ == '__main__':
    sys.exit(main())
