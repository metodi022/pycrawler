import argparse
import importlib
import logging
import pathlib
import sys
import time
import traceback
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import List, Optional, Type, cast

from crawler import Crawler
from database import Task, database
from modules.Module import Module

#import ecs_logging  # TODO elastic search logs


Config = importlib.import_module('config').Config


class CustomProcess(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as error:
            tb = traceback.format_exc()
            self._cconn.send((type(error), error, tb))
        finally:
            self._cconn.close()

    @property
    def exception(self):
        if (self._exception is None) and self._pconn.poll():
            self._exception = self._pconn.recv()
            self._pconn.close()

        return self._exception


def _validate_arguments(crawlers_count: int, starting_crawler_id: int, log_path: pathlib.Path):
    if crawlers_count <= 0 or starting_crawler_id <= 0:
        raise ValueError('Invalid number of crawlers or starting crawler id.')

    if log_path.exists() and not log_path.is_dir():
        raise ValueError('Path to directory for log output is incorrect')

def _get_logger(log_path: pathlib.Path, name: str) -> logging.Logger:
    handler: logging.FileHandler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    log = logging.Logger(name)
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)

    return log

def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    modules: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module(f"modules.{module_name}")
        modules.append(getattr(module, module_name))
    return modules

def _get_task(job: str, crawler_id: int, log) -> Optional[Task]:
    # Get progress task
    task: Optional[Task] = Task.get_or_none(job=job, crawler=crawler_id, state='progress')
    if task is not None:
        log.info("Loading progress task")
        return task

    # Otherwise get new free task
    with database.atomic():
        if not Config.SQLITE:
            result = database.execute_sql(f"SELECT id FROM task WHERE state='free' AND job={database.param} FOR UPDATE SKIP LOCKED LIMIT 1", (job,)).fetchone()
        else:
            result = database.execute_sql(f"SELECT id FROM task WHERE state='free' AND job={database.param} LIMIT 1", (job,)).fetchone()

        if not result:
            log.info("Found no task")
            task = None
        else:
            log.info("Loading free task")
            database.execute_sql(f"UPDATE task SET updated={database.param}, crawler={database.param}, state='progress' WHERE id={database.param}", (datetime.today(), crawler_id, result[0]))
            task = Task.get_by_id(result[0])

    return task

def _start_crawler(job: str, crawler_id: int, task: int, log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(log_path / f"job{job}crawler{crawler_id}.log", job + str(crawler_id) + __name__)
    log.info('Start crawler')
    crawler: Crawler = Crawler(task, log, modules)
    crawler.start_crawl()
    log.info('Stop crawler')
    log.handlers[-1].close()


def main(job: str, crawlers_count: int, module_names: List[str], log_path: pathlib.Path, starting_crawler_id: int = 1, listen: bool = False) -> int:
    # Prepare logger
    log_path.mkdir(parents=True, exist_ok=True)
    (log_path / 'screenshots').mkdir(parents=True, exist_ok=True)

    if Config.HAR:
        Config.HAR.mkdir(parents=True, exist_ok=True)

    log: logging.Logger = _get_logger(log_path / f"job{job}.log", job + __name__)

    # Importing modules
    log.debug("Import additional modules %s", str(module_names))
    modules: List[Type[Module]] = _get_modules(module_names)

    # Create modules database
    log.info('Load modules database')
    for module in modules:
        module.register_job(log)

    # Prepare crawlers
    log.info('Preparing crawlers')
    crawlers: List[Process] = []
    for i in range(0, crawlers_count):
        process = Process(target=_manage_crawler, args=(job, i + starting_crawler_id, log_path, modules, listen))
        crawlers.append(process)

    # Start crawlers
    log.info('Starting crawlers')
    for i, crawler in enumerate(crawlers):
        crawler.start()
        log.info("Start crawler %s with JOBID %s PID %s", (i + starting_crawler_id), job, crawler.pid)

    # Wait for crawlers to finish
    log.info('Waiting for crawlers to complete')
    for crawler in crawlers:
        crawler.join()
        crawler.close()

    log.info('Crawl complete')

    # Exit code
    return 0

def _manage_crawler(job: str, crawler_id: int, log_path: pathlib.Path, modules: List[Type[Module]], listen: bool) -> None:
    log = _get_logger(log_path / f"job{job}crawler{crawler_id}.log", job + str(crawler_id) + __name__)

    task: Optional[Task] = _get_task(job, crawler_id, log)

    # Main loop
    while task or listen:
        if not task:
            time.sleep(60)
            task = _get_task(job, crawler_id, log)
            continue

        start_time: datetime = datetime.now()

        with database:
            is_cached: bool = not database.execute_sql(f"SELECT crawlerstate IS NULL FROM task WHERE id={database.param}", (task.get_id(),)).fetchone()[0]

        crawler: CustomProcess = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
        crawler.start()
        log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

        while crawler.is_alive() or is_cached:
            if not crawler.is_alive():
                log.error("Crawler %s crashed %s", task.crawler, crawler.exception)
                crawler.close()
                crawler = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
                crawler.start()
                log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

            crawler.join(timeout=Config.RESTART_TIMEOUT)

            with database:
                timelastentry = database.execute_sql(f"SELECT updated FROM task WHERE id={database.param}", (task.get_id(),)).fetchone()[0]
                is_cached = not database.execute_sql(f"SELECT crawlerstate IS NULL FROM task WHERE id={database.param}", (task.get_id(),)).fetchone()[0]

            if not crawler.is_alive():
                continue

            if (datetime.today() - timelastentry).seconds < Config.RESTART_TIMEOUT:
                continue

            log.error("Close stale crawler %s", task.crawler)

            crawler.terminate()
            crawler.join(timeout=30)

            if crawler.is_alive():
                crawler.kill()
                time.sleep(5)

        crawler.close()

        with database.atomic():
            task.updated = datetime.today()
            database.execute_sql(f"UPDATE task SET updated={database.param}, state='complete', crawlerstate=NULL WHERE id={database.param}", (task.updated, task.get_id()))

        if crawler.exception:
            log.error("Crawler %s crashed %s", task.crawler, crawler.exception)

        log.info("Crawler %s finished after %s", task.crawler, (datetime.now() - start_time), extra=())  # TODO

        task = _get_task(job, crawler_id, log)

    log.handlers[-1].close()


if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
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
    args_parser.add_argument("-o", "--log", type=str, required=False, default=Config.LOG,
                             help="path to directory for log output")

    # Parse command line arguments
    args = vars(args_parser.parse_args())

    try:
        _validate_arguments(args['crawlers'], args['crawlerid'], args['log'])
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Fix for multiple modules not correctly parsed
    modules = args['modules'] or []
    if modules and (' ' in modules[0]):
        modules = modules[0].split()

    sys.exit(main(
        cast(str, args['job']),
        cast(int, args['crawlers']),
        modules,
        Config.LOG,
        cast(int, args['crawlerid']),
        cast(bool, args['listen'])
    ))
