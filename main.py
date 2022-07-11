import argparse
import importlib
import os
import pathlib
import signal
import sys
import time
from datetime import datetime
from logging import Logger, FileHandler, Formatter
from multiprocessing import Process
from typing import List, Type, Tuple, Optional

from chromiumcrawler import ChromiumCrawler
from config import Config
from database.postgres import Postgres
from loader.csvloader import CSVLoader
from loader.loader import Loader
from modules.acceptcookies import AcceptCookies
from modules.collecturls import CollectUrls
from modules.module import Module
from modules.savestats import SaveStats


def main() -> int:
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-o", "--log", help="path to directory where output log will be saved",
                             type=pathlib.Path)
    args_parser.add_argument("-f", "--urls", help="path to file with urls", type=pathlib.Path,
                             required=True)
    args_parser.add_argument("-m", "--modules", help="which modules the crawler will run",
                             type=str, required=True, nargs='*')
    args_parser.add_argument("-j", "--job", help="unique job id for crawl", type=int, required=True)
    args_parser.add_argument("-c", "--crawlers", help="how many crawlers will run concurrently",
                             type=int, required=True)

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    job_id: int = args.get('job') or 0
    log_path: pathlib.Path = args.get('log') or Config.LOG
    urls_path: pathlib.Path = args.get('urls') or pathlib.Path('.')

    # Verify arguments
    if not log_path.exists() and log_path.is_dir():
        raise RuntimeError('Path to directory for log output is incorrect')

    if not urls_path.exists() and not urls_path.is_dir():
        raise RuntimeError('Path to file with urls is incorrect')

    if (args.get('crawlers') or 0) <= 0:
        raise RuntimeError('Invalid number of crawlers')

    # Main log
    if not (log_path / 'screenshots').exists():
        os.mkdir(log_path / 'screenshots')
    handler: FileHandler = FileHandler(log_path / f"job{job_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger(f"Job {job_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    # Prepare database
    log.info('Load database with URLs')
    loader: Loader = CSVLoader(urls_path)
    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)
    database.register_job(job_id, (args.get('crawlers') or 0), loader)

    # Prepare modules
    log.info('Load modules with URLs')
    CollectUrls.register_job(database, log)
    AcceptCookies.register_job(database, log)
    modules: List[Type[Module]] = _get_modules((args.get('modules') or []))
    for module in modules:
        module.register_job(database, log)
    SaveStats.register_job(database, log)
    database.disconnect()

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(1, (args.get('crawlers') or 0) + 1):
        # _start_crawler2(job_id, i, log_path, modules)
        process = Process(target=_start_crawler1, args=(job_id, i, log_path, modules))
        crawlers.append(process)

    for i, crawler in enumerate(crawlers):
        crawler.start()
        log.info(f"Start crawler {i + 1} with PID {crawler.pid}")

    for crawler in crawlers:
        crawler.join()

    return 0


def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    result: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module('modules.' + module_name.lower())
        result.append(getattr(module, module_name))
    return result


def _start_crawler1(job_id: int, crawler_id: int, log_path: pathlib.Path,
                    modules: List[Type[Module]]) -> None:
    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)
    url: Optional[Tuple[str, int, int, List[Tuple[str, str]]]] = database.get_url(job_id,
                                                                                  crawler_id)

    while url:
        crawler: Process = Process(target=_start_crawler2,
                                   args=(job_id, crawler_id, url, log_path, modules))
        crawler.start()

        while crawler.is_alive():
            crawler.join(timeout=Config.RESTART_TIMEOUT)

            line = _get_line_last(log_path / f"job{job_id}crawler{crawler_id}.log").split()
            date1: datetime = datetime.today()
            date2: datetime = datetime.strptime(line[0] + ' ' + line[1], '%Y-%m-%d %H:%M:%S,%f')

            if (date1 - date2).seconds < Config.RESTART_TIMEOUT:
                continue

            break

        crawler.terminate()
        crawler.join(timeout=30)
        crawler.kill()
        crawler.join(timeout=30)
        time.sleep(1)
        url = database.get_url(job_id, crawler_id)


def _start_crawler2(job_id: int, crawler_id: int, url: Tuple[str, int, int, List[Tuple[str, str]]],
                    log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    handler: FileHandler = FileHandler(log_path / f"job{job_id}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job_id} Crawler {crawler_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)
    signal.signal(signal.SIGTERM, lambda signal_received, frame: (
        log.error('Close stale crawler'), database.disconnect()))
    signal.signal(signal.SIGINT, lambda signal_received, frame: (
        log.error('Close stale crawler'), database.disconnect()))

    log.info('Start crawler')
    ChromiumCrawler(job_id, crawler_id, url, database, log, modules).start_crawl()
    log.info('Stop crawler')


def _get_line_last(path: str | pathlib.Path) -> str:
    with open(path, mode='rb') as file:
        file.seek(-2, 2)
        while file.read(1) != b'\n':
            file.seek(-2, 1)
        line = file.readline()
    return line.decode("utf-8")


if __name__ == '__main__':
    sys.exit(main())
