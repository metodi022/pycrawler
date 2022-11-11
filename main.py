import argparse
import importlib
import os
import pathlib
import re
import signal
import sys
import time
from datetime import datetime
from logging import Logger, FileHandler, Formatter
from multiprocessing import Process
from typing import List, Type, Tuple, Optional, cast

from config import Config
from crawler import Crawler
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
    args_parser.add_argument("-f", "--urls", help="path to file with urls", type=pathlib.Path)
    args_parser.add_argument("-m", "--modules", help="which modules the crawler will run",
                             type=str, required=True, nargs='*')
    args_parser.add_argument("-j", "--job", help="unique job id for crawl", type=int, required=True)
    args_parser.add_argument("-c", "--crawlers", help="how many crawlers will run concurrently",
                             type=int, required=True)
    args_parser.add_argument("-s", "--setup", help="run setup for DB and modules",
                             action='store_true')

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    job_id: int = args.get('job') or 0
    log_path: pathlib.Path = args.get('log') or Config.LOG
    urls_path: Optional[pathlib.Path] = args.get('urls')
    setup: bool = args.get('setup') or False

    # Verify arguments
    if not log_path.exists() and log_path.is_dir():
        raise RuntimeError('Path to directory for log output is incorrect')

    if setup and not urls_path:
        raise RuntimeError('Setup without path to urls file')

    urls_path = cast(pathlib.Path, urls_path)

    if setup and not urls_path.exists() and not urls_path.is_dir():
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

    # Prepare modules
    log.info('Load modules with URLs')
    modules: List[Type[Module]] = _get_modules((args.get('modules') or []))

    # Run setup if needed
    if setup and urls_path:
        loader: Loader = CSVLoader(urls_path)
        database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                      Config.PORT)
        database.register_job(job_id, (args.get('crawlers') or 0), loader)
        CollectUrls.register_job(database, log)
        AcceptCookies.register_job(database, log)
        for module in modules:
            module.register_job(database, log)
        SaveStats.register_job(database, log)
        database.disconnect()

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(1, (args.get('crawlers') or 0) + 1):
        process = Process(target=_start_crawler1, args=(job_id, i, log_path, modules))
        crawlers.append(process)

    for i, crawler in enumerate(crawlers):
        crawler.start()
        log.info(f"Start crawler {i + 1} with PID {crawler.pid}")

    # Wait for crawlers to finish
    for crawler in crawlers:
        crawler.join()

    # Exit code
    return 0


def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    result: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module('modules.' + module_name.lower())
        result.append(getattr(module, module_name))
    return result


def _handler(signum, frame):
    print(f"SIGNUM {signum} FRAME {frame}")


def _start_crawler1(job_id: int, crawler_id: int, log_path: pathlib.Path,
                    modules: List[Type[Module]]) -> None:
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    log = _get_logger(job_id, crawler_id, log_path)
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

            if len(line) > 1:
                date1: datetime = datetime.today()
                date2: datetime = datetime.strptime(line[0] + ' ' + line[1], '%Y-%m-%d %H:%M:%S,%f')

                if (date1 - date2).seconds < Config.RESTART_TIMEOUT:
                    continue

            log.error('Close stale crawler')

            crawler.terminate()
            crawler.join(timeout=30)

            if crawler.is_alive():
                crawler.kill()
                time.sleep(5)

            crawler.close()

            crawler: Process = Process(target=_start_crawler2,
                                       args=(job_id, crawler_id, url, log_path, modules))
            crawler.start()

        url = database.get_url(job_id, crawler_id)


def _start_crawler2(job_id: int, crawler_id: int, url: Tuple[str, int, int, List[Tuple[str, str]]],
                    log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(job_id, crawler_id, log_path)
    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)

    log.info('Start crawler')
    Crawler(job_id, crawler_id, url, database, log, modules).start_crawl()
    log.info('Stop crawler')


def _get_logger(job_id: int, crawler_id: int, log_path: pathlib.Path) -> Logger:
    handler: FileHandler = FileHandler(log_path / f"job{job_id}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job_id} Crawler {crawler_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)
    return log


def _get_line_last(path: str | pathlib.Path) -> str:
    with open(path, mode='rb') as file:
        line: bytes = b''

        try:
            file.seek(-2, 2)
        except OSError:
            return ''

        while re.match('\\d{4}-\\d{2}-\\d{2}', line.decode("utf-8", errors="ignore")) is None:
            try:
                file.seek(-(len(line) + 2) if len(line) > 0 else 0, 1)
            except OSError:
                return ''

            while file.read(1) != b'\n':
                try:
                    file.seek(-2, 1)
                except OSError:
                    try:
                        file.seek(-1, 1)
                    except OSError:
                        return ''
                    break

            line = file.readline()
    return line.decode("utf-8", errors="ignore")


if __name__ == '__main__':
    sys.exit(main())
