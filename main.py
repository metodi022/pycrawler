import argparse
import ast
import importlib
import os
import pathlib
import re
import sys
import time
from datetime import datetime
from logging import Logger, FileHandler, Formatter
from multiprocessing import Process
from typing import List, Type, Tuple, Optional

from peewee import DoesNotExist

from config import Config
from crawler import Crawler
from database import database, URL
from loader.csvloader import CSVLoader
from modules.module import Module


def main() -> int:
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-o", "--log", help="path to directory where output log will be saved",
                             type=pathlib.Path)
    args_parser.add_argument("-f", "--urlspath", help="path to file with urls", type=pathlib.Path)
    args_parser.add_argument("-u", "--urls", help="urls to crawl", nargs='+', type=ast.literal_eval)
    args_parser.add_argument("-m", "--modules", help="which modules the crawler will run",
                             type=str, required=True, nargs='+')
    args_parser.add_argument("-j", "--job", help="unique job id for crawl", type=int, required=True)
    args_parser.add_argument("-c", "--crawlers", help="how many crawlers will run concurrently",
                             type=int, required=True)
    args_parser.add_argument("-i", "--crawlerid", default=1, type=int,
                             help="starting crawler id (default 1); must be > 0")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    job_id: int = args.get('job') or 0
    log_path: pathlib.Path = args.get('log') or Config.LOG
    urls_path: Optional[pathlib.Path] = args.get('urlspath')
    urls: Optional[List[Tuple[int, str]]] = args.get('urls')

    # Verify arguments
    if not (log_path.exists() and log_path.is_dir()):
        raise RuntimeError('Path to directory for log output is incorrect')

    if urls_path is not None and not (urls_path.exists() or urls_path.is_dir()):
        raise RuntimeError('Path to file with urls is incorrect')

    if args.get('crawlers') <= 0 or args.get('crawlerid') <= 0:
        raise RuntimeError('Invalid number of crawlers or starting crawler id.')

    # Main log
    if not (log_path / 'screenshots').exists():
        os.mkdir(log_path / 'screenshots')
    handler: FileHandler = FileHandler(log_path / f"job{job_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger(f"Job {job_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    # Prepare modules
    modules: List[Type[Module]] = _get_modules((args.get('modules') or []))

    # Run setup if needed
    if (urls_path or urls) is not None:
        log.info('Load database with URLs')

        with database:
            database.create_tables([URL])

        # Speedup by using atomic transaction
        with database.atomic():
            # Iterate over URLs and add them to database
            entry: Tuple[int, str]
            for entry in (CSVLoader(urls_path) if urls_path is not None else urls):
                if entry[0] <= 0:
                    raise RuntimeError('Invalid site rank.')
                crawler_id: int = ((entry[0] - 1) % args.get('crawlers')) + args.get('crawlerid')
                url: str = ('https://' if 'http' not in entry[1] else '') + entry[1]
                URL.create(job=job_id, crawler=crawler_id, url=url, rank=entry[0])

        for module in modules:
            module.register_job(log)

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(0, args.get('crawlers')):
        process = Process(target=_start_crawler1, args=(job_id, i + args.get('crawlerid'), log_path, modules))
        crawlers.append(process)

    for i, crawler in enumerate(crawlers):
        log.info(f"Start crawler {i + args.get('crawlerid')} with PID {crawler.pid}")
        crawler.start()

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


def _start_crawler1(job_id: int, crawler_id: int, log_path: pathlib.Path,
                    modules: List[Type[Module]]) -> None:

    log = _get_logger(job_id, crawler_id, log_path)
    url: Optional[URL] = None
    try:
        url = URL.get(job=job_id, crawler=crawler_id, code=None)
    except DoesNotExist:
        # Ignored
        pass

    while url:
        crawler: Process = Process(target=_start_crawler2,
                                   args=(job_id, crawler_id, (url.url, 0, url.rank, []), log_path, modules))
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

        try:
            url = URL.get(job=job_id, crawler=crawler_id, code=None)
        except DoesNotExist:
            url = None


def _start_crawler2(job_id: int, crawler_id: int, url: Tuple[str, int, int, List[Tuple[str, str]]],
                    log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(job_id, crawler_id, log_path)
    log.info('Start crawler')
    Crawler(job_id, crawler_id, url, log, modules).start_crawl()
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
