import argparse
import ast
import importlib
import os
import pathlib
import re
import sys
import time
import traceback
from datetime import datetime
from logging import FileHandler, Formatter, Logger
from multiprocessing import Process
from typing import List, Optional, Tuple, Type

import tld
from tld.exceptions import TldBadUrl, TldDomainNotFound

from crawler import Crawler
from database import URL, database
from loader.csvloader import CSVLoader
from modules.module import Module

try:
    Config = importlib.import_module('config').Config
except ModuleNotFoundError as e:
    traceback.print_exc()
    print(e)
    print("Prepare the config.py file. You can use the config-example.py as a start.")
    sys.exit(1)


def main(job: str, crawlers_count: int, module_names: List[str], urls_path: Optional[pathlib.Path] = None, urls: Optional[List[Tuple[int, str]]] = None, log_path: Optional[pathlib.Path] = None, starting_crawler_id: int = 1, listen: bool = False) -> int:
    # Create log path if needed
    log_path = log_path or Config.LOG
    if not log_path.exists():
        os.mkdir(log_path)

    # Verify arguments
    if not (log_path.exists() and log_path.is_dir()):
        raise RuntimeError('Path to directory for log output is incorrect')

    if urls_path is not None and not (urls_path.exists() or urls_path.is_dir()):
        raise RuntimeError('Path to file with urls is incorrect')

    if crawlers_count <= 0 or starting_crawler_id <= 0:
        raise RuntimeError('Invalid number of crawlers or starting crawler id.')

    # Prepare logger
    if not (log_path / 'screenshots').exists():
        os.mkdir(log_path / 'screenshots')
    handler: FileHandler = FileHandler(log_path / f"job{job}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger(f"Job {job}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    # Fix for multiple modules not correctly parsed
    if module_names and ' ' in module_names[0]:
        module_names = module_names[0].split()

    # Importing modules
    log.info("Import modules %s", str(module_names))
    modules: List[Type[Module]] = _get_modules(module_names)

    # Creating database
    log.info('Load database')
    with database.atomic():
        database.create_tables([URL])

    # Iterate over URLs and add them to database
    if urls_path is not None or urls:
        with database.atomic():  # speedup by using atomic transaction
            entry: Tuple[int, str]
            for entry in (CSVLoader(urls_path) if urls_path is not None else urls):
                url: str = ('https://' if not entry[1].startswith('http') else '') + entry[1]
                try:
                    site: str = tld.get_tld(url, as_object=True).fld
                except (TldBadUrl, TldDomainNotFound):
                    log.warning("Could not parse %s", url)
                    continue
                URL.create(job=job, crawler=None, site=site, url=url, landing_page=url, rank=int(entry[0]))

    # Create modules database
    for module in modules:
        module.register_job(log)

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(0, crawlers_count):
        process = Process(target=_start_crawler1, args=(job, i + starting_crawler_id, log_path, modules, listen))
        crawlers.append(process)

    for i, crawler in enumerate(crawlers):
        log.info(f"Start crawler {i + starting_crawler_id} with JOBID {job} PID {crawler.pid}")
        crawler.start()

    # Wait for crawlers to finish
    log.info('Waiting for crawlers to complete')
    for crawler in crawlers:
        crawler.join()
        crawler.close()

    log.info('Crawl complete')

    # Exit code
    return 0


def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    result: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module('modules.' + module_name.lower())
        result.append(getattr(module, module_name))
    return result


def _get_url(job: str, crawler_id: int, log) -> Optional[URL]:
    url: Optional[URL] = URL.get_or_none(job=job, crawler=crawler_id, state='progress')
    if url is not None:
        log.debug("Loading progress URL")
        return url
    
    with database.atomic():
        url = URL.get_or_none(job=job, crawler=crawler_id, state='free')
        url = url or URL.get_or_none(job=job, crawler=None, state='free')
        url = url or URL.get_or_none(job=job, state='free')
        if url is not None:
            log.debug("Loading free URL")
            url.crawler = crawler_id
            url.state = 'progress'
            url.save()
    
    return url


def _start_crawler1(job: str, crawler_id: int, log_path: pathlib.Path, modules: List[Type[Module]], listen: bool) -> None:
    log = _get_logger(job, crawler_id, log_path)
    url: Optional[URL] = _get_url(job, crawler_id, log)

    while url or listen:
        if not url and listen:
            time.sleep(30)
            url = _get_url(job, crawler_id, log)
            continue

        crawler: Process = Process(target=_start_crawler2, args=(job, crawler_id, url.url, url.rank, log_path, modules))
        crawler.start()

        while crawler.is_alive():
            crawler.join(timeout=Config.RESTART_TIMEOUT)

            line = _get_line_last(log_path / f"job{job}crawler{crawler_id}.log").split()

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

            if Config.RESTART and (Config.LOG / f"job{job}crawler{crawler_id}.cache").exists():
                crawler.close()
                crawler = Process(target=_start_crawler2, args=(job, crawler_id, url.url, url.rank, log_path, modules))
                crawler.start()

        crawler.close()

        url.state = 'complete'
        url.save()
        url = _get_url(job, crawler_id, log)


def _start_crawler2(job: str, crawler_id: int, url: str, rank: int, log_path: pathlib.Path,
                    modules: List[Type[Module]]) -> None:
    log = _get_logger(job, crawler_id, log_path)
    log.info('Start crawler')
    crawler: Crawler = Crawler(job, crawler_id, url, rank, log, modules)
    crawler.start_crawl()
    log.info('Stop crawler')


def _get_logger(job: str, crawler_id: int, log_path: pathlib.Path) -> Logger:
    handler: FileHandler = FileHandler(log_path / f"job{job}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job} Crawler {crawler_id}")
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

            line = file.readline() or b''
    return line.decode("utf-8", errors="ignore")


if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-o", "--log", type=pathlib.Path,
                             help="path to directory where output log will be saved")
    args_parser.add_argument("-f", "--urlspath", type=pathlib.Path,
                             help="path to file with urls",)
    args_parser.add_argument("-u", "--urls", type=ast.literal_eval, nargs='+',
                             help="urls to crawl", )
    args_parser.add_argument("-m", "--modules", type=str, nargs='*',
                             help="which modules the crawler will run")
    args_parser.add_argument("-j", "--job", type=str, required=True,
                             help="unique job id for crawl")
    args_parser.add_argument("-c", "--crawlers", type=int, required=True,
                             help="how many crawlers will run concurrently")
    args_parser.add_argument("-i", "--crawlerid", type=int, default=1,
                             help="starting crawler id (default 1); must be > 0")
    args_parser.add_argument("-l", "--listen", default=False, action='store_true',
                             help="crawler will not stop if there is no job; query and sleep until a job is found")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    sys.exit(main(
        args.get('job'),
        args.get('crawlers'),
        args.get('modules') or [],
        args.get('urlspath'),
        args.get('urls'),
        args.get('log'),
        args.get('crawlerid'),
        args.get('listen')
    ))
