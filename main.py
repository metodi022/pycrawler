import argparse
import importlib
import pathlib
import sys
import time
import traceback
from datetime import datetime
from logging import FileHandler, Formatter, Logger
from multiprocessing import Pipe, Process
from typing import List, Optional, Type, cast

from crawler import Crawler
from database import URL, Site, Task, database
from modules.Module import Module
from peewee import ProgrammingError

try:
    Config = importlib.import_module('config').Config
except ModuleNotFoundError as e:
    traceback.print_exc()
    print(e)
    print("Prepare the config.py file. You can use the config-example.py as a start.")
    sys.exit(1)


class CustomProcess(Process):
    def __init__(self, *aargs, **kwargs):
        Process.__init__(self, *aargs, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as error:
            trace = traceback.format_exc()
            self._cconn.send((error, trace))
        finally:
            self._cconn.close()

    @property
    def exception(self):
        if (not self._exception) and self._pconn.poll():
            self._exception = self._pconn.recv()
            self._pconn.close()
        return self._exception


def _validate_arguments(crawlers_count: int, starting_crawler_id: int, log_path: pathlib.Path):
    if crawlers_count <= 0 or starting_crawler_id <= 0:
        raise ValueError('Invalid number of crawlers or starting crawler id.')

    if log_path.exists() and not log_path.is_dir():
        raise ValueError('Path to directory for log output is incorrect')

def _get_logger(log_path: pathlib.Path, name: str) -> Logger:
    handler: FileHandler = FileHandler(log_path)
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(name)
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
        result = database.execute_sql("SELECT id FROM task WHERE state='free' AND job=%s FOR UPDATE SKIP LOCKED LIMIT 1", (job,)).fetchone()

        if not result:
            log.info("Found no task")
            task = None
        else:
            log.info("Loading free task")
            database.execute_sql("UPDATE task SET updated=%s, crawler=%s, state='progress' WHERE id=%s", (datetime.today(), crawler_id, result[0]))
            task = Task.get_by_id(result[0])

    return task

def _start_crawler(job: str, crawler_id: int, task: int, log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(log_path / f"job{job}crawler{crawler_id}.log", job + str(crawler_id))
    log.info('Start crawler')
    crawler: Crawler = Crawler(job, crawler_id, task, log, modules)
    crawler.start_crawl()
    log.info('Stop crawler')
    log.handlers[-1].close()


def main(job: str, crawlers_count: int, module_names: List[str], log_path: pathlib.Path, starting_crawler_id: int = 1, listen: bool = False) -> int:
    # Prepare logger
    log_path.mkdir(parents=True, exist_ok=True)
    (log_path / 'screenshots').mkdir(parents=True, exist_ok=True)

    log: Logger = _get_logger(log_path / f"job{job}.log", job)

    # Importing modules
    log.info("Import additional modules %s", str(module_names))
    modules: List[Type[Module]] = _get_modules(module_names)

    # Creating database
    log.info('Load database')
    with database.atomic():
        database.create_tables([Site])
        database.create_tables([Task])
        database.create_tables([URL])
        
        try:
            Task._schema.create_foreign_key(Task.landing)
        except ProgrammingError:
            pass

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
    log = _get_logger(log_path / f"job{job}crawler{crawler_id}.log", job + str(crawler_id))

    task: Optional[Task] = _get_task(job, crawler_id, log)

    while task or listen:
        if not task:
            time.sleep(60)
            task = _get_task(job, crawler_id, log)
            continue

        crawler: CustomProcess = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
        crawler.start()
        log.info("Start crawler %s PID %s", crawler_id, crawler.pid)
        crawler.join()

        with database:
            is_cached: bool = not database.execute_sql("SELECT crawlerstate IS NULL FROM task WHERE id=%s", (task.get_id(),)).fetchone()[0]

        while crawler.is_alive() or (Config.RESTART and is_cached):
            if not crawler.is_alive():
                log.error("Crawler %s crashed with %s", task.crawler, crawler.exception)
                crawler.close()
                crawler = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
                crawler.start()
                log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

            crawler.join(timeout=Config.RESTART_TIMEOUT)

            with database:
                timelastentry = database.execute_sql("SELECT updated FROM task WHERE id=%s", (task.get_id(),)).fetchone()
                is_cached = not database.execute_sql("SELECT crawlerstate IS NULL FROM task WHERE id=%s", (task.get_id(),)).fetchone()[0]

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
            database.execute_sql("UPDATE task SET updated=%s, state='complete', crawlerstate=NULL WHERE id=%s", (task.updated, task.get_id()))

        if crawler.exception:
            # TODO save error in db?
            log.error("Crawler %s crashed with %s", task.crawler, crawler.exception)

        task = _get_task(job, crawler_id, log)

    log.handlers[-1].close()


if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-o", "--log", type=pathlib.Path,
                             help="path to directory where output log will be saved")
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

    try:
        _validate_arguments(args['crawlers'], args['crawlerid'], args['log'] or Config.LOG)
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
        args['log'] or Config.LOG,
        cast(int, args['crawlerid']),
        cast(bool, args['listen'])
    ))
