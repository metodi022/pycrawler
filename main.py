import argparse
import importlib
import pathlib
import sys
from logging import Logger, FileHandler, Formatter
from multiprocessing import Process
from typing import List, Type

from chromiumcrawler import ChromiumCrawler
from config import Config
from database.postgres import Postgres
from loader.csvloader import CSVLoader
from loader.loader import Loader
from modules.module import Module


def main() -> int:
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-o", "--log", help="path to directory where output log will be saved",
                             type=pathlib.Path, required=True)
    args_parser.add_argument("-f", "--urls", help="path to file with urls", type=pathlib.Path,
                             required=True)
    args_parser.add_argument("-m", "--modules", help="which modules the crawler will run",
                             type=str, required=True, nargs='+')
    args_parser.add_argument("-j", "--job", help="unique job id for crawl", type=int, required=True)
    args_parser.add_argument("-c", "--crawlers", help="how many crawlers will run concurrently",
                             type=int, required=True)

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    job_id: int = args.get('job', 0)
    log_path: pathlib.Path = args.get('log', pathlib.Path('.'))
    urls_path: pathlib.Path = args.get('urls', pathlib.Path('.'))

    # Verify arguments
    if not log_path.exists() and log_path.is_dir():
        raise RuntimeError('Path to directory for log output is incorrect')

    if not urls_path.exists() and not urls_path.is_dir():
        raise RuntimeError('Path to file with urls is incorrect')

    if args.get('crawlers', 0) <= 0:
        raise RuntimeError('Invalid number of crawlers')

    # Main log
    handler: FileHandler = FileHandler(log_path / f"job{job_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log: Logger = Logger(f"Job {job_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    # Prepare database
    loader: Loader = CSVLoader(urls_path)
    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)
    database.register_job(job_id, loader)
    log.info('Load database with URLs')

    modules: List[Type[Module]] = _get_modules(args.get('modules', []))
    for module in modules:
        module.register_job(database, log)
    database.disconnect()

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(1, args.get('crawlers', 0) + 1):
        handler = FileHandler(log_path / f"job{job_id}crawler{i}.log")
        handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
        log = Logger(f"Job {job_id} Crawler {i}")
        log.setLevel(Config.LOG_LEVEL)
        log.addHandler(handler)
        process = Process(target=_start_crawler(job_id, i, log_path, modules))
        crawlers.append(process)

    for crawler in crawlers:
        crawler.start()

    for crawler in crawlers:
        crawler.join()

    return 0


def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    result: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module('modules.' + module_name.lower())
        result.append(getattr(module, module_name))

    return result


def _initialize_modules(modules: List[Type[Module]], job_id: int, crawler_id: int,
                        database: Postgres, log: Logger) -> List[Module]:
    result: List[Module] = []
    for module in modules:
        result.append(module(job_id, crawler_id, Config, database, log))

    return result


def _start_crawler(job_id: int, crawler_id: int, log_path: pathlib.Path,
                   modules: List[Type[Module]]) -> None:
    handler: FileHandler = FileHandler(log_path / f"job{job_id}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job_id} Crawler {crawler_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    database: Postgres = Postgres(Config.DATABASE, Config.USER, Config.PASSWORD, Config.HOST,
                                  Config.PORT)

    crawler: ChromiumCrawler = ChromiumCrawler(1, 1, Config, database, log, _initialize_modules(modules, job_id, crawler_id, database, log))
    crawler.start_crawl_chromium()
    crawler.stop_crawl_chromium()
    database.disconnect()


if __name__ == '__main__':
    sys.exit(main())
