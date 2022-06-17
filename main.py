import sys
from logging import Logger, FileHandler, Formatter

from chromiumcrawler import ChromiumCrawler
from config import Config
from database.postgres import Postgres
from loader.csvloader import CSVLoader
from loader.loader import Loader
from modules.collecturls import CollectUrls
from modules.findlogin import FindLogin
from modules.savestats import SaveStats


def main() -> int:
    handler = FileHandler('D:\\Programming\\pycrawler\\job1crawler1.txt')
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger('Job 1 ChromiumCrawler 1')
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    database: Postgres = Postgres('test', 'postgres', 'postgres', 'localhost', '5432')

    # TODO how to deal with cookies?

    """
    loader: Loader = CSVLoader('D:\\Programming\\pycrawler\\urls.csv')
    database.register_job(1, loader)

    CollectUrls.register_job(database, log)
    SaveStats.register_job(database, log)
    FindLogin.register_job(database, log)
    """

    crawler = ChromiumCrawler(1, 1, Config, database, log, [FindLogin(1, 1, Config, database, log)])
    crawler.start_crawl_chromium()

    return 0


if __name__ == '__main__':
    sys.exit(main())
