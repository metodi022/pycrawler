import argparse
import importlib
import os
import pathlib
import sys
import time
import traceback
from datetime import datetime
from logging import FileHandler, Formatter, Logger
from multiprocessing import Pipe, Process
from typing import List, Optional, Type, cast

from crawler import Crawler
from database import URL, Task, database
from modules.module import Module

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

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def _get_logger(job: str, crawler_id: int, log_path: pathlib.Path) -> Logger:
    handler: FileHandler = FileHandler(log_path / f"job{job}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job} Crawler {crawler_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)
    return log

def _get_modules(module_names: List[str]) -> List[Type[Module]]:
    result: List[Type[Module]] = []
    for module_name in module_names:
        module = importlib.import_module('modules.' + module_name.lower())
        result.append(getattr(module, module_name))
    return result

def _get_task(job: str, crawler_id: int, log) -> Optional[Task]:
    # Get progress task
    task: Optional[Task] = Task.get_or_none(job=job, crawler=crawler_id, state='progress')
    if task is not None:
        log.info("Loading progress task")
        return task

    # Otherwise get new free task
    with database.atomic():
        result = database.execute_sql("SELECT id FROM task WHERE state='free' AND job=%s FOR UPDATE SKIP LOCKED LIMIT 1", (job,)).fetchall()

        if len(result) == 0:
            task = None
        else:
            log.info("Loading free task")
            with database.atomic():
                database.execute_sql("UPDATE task SET updated=%s, crawler=%s, state='progress' WHERE id=%s", (datetime.today(), crawler_id, result[0]))
            task = Task.get_by_id(result[0])

    return task

def _start_crawler(job: str, crawler_id: int, task: int, log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(job, crawler_id, log_path)
    log.info('Start crawler')
    crawler: Crawler = Crawler(job, crawler_id, task, log, modules)
    crawler.start_crawl()
    log.info('Stop crawler')
    log.handlers[-1].close()


def main(job: str, crawlers_count: int, module_names: List[str], log_path: Optional[pathlib.Path] = None, starting_crawler_id: int = 1, listen: bool = False) -> int:
    # Create log path if needed
    log_path = (log_path or Config.LOG).resolve()
    log_path = cast(pathlib.Path, log_path)
    if not log_path.exists():
        os.mkdir(log_path)

    # TODO better verify of arguments

    # Verify arguments
    if not (log_path.exists() and log_path.is_dir()):
        raise RuntimeError('Path to directory for log output is incorrect')

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
    if module_names and (' ' in module_names[0]):
        module_names = module_names[0].split()

    # Importing modules
    log.info("Import modules %s", str(module_names))
    modules: List[Type[Module]] = _get_modules(module_names)

    # Creating database
    log.info('Load database')
    with database.atomic():
        database.create_tables([Task])
        database.create_tables([URL])

    # Create modules database
    log.info('Load modules database')
    for module in modules:
        module.register_job(log)

    # Prepare crawlers
    crawlers: List[Process] = []
    for i in range(0, crawlers_count):
        process = Process(target=_manage_crawler, args=(job, i + starting_crawler_id, log_path, modules, listen))
        crawlers.append(process)

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
    log = _get_logger(job, crawler_id, log_path)

    task: Optional[Task] = _get_task(job, crawler_id, log)

    while task or listen:
        if not task:
            time.sleep(60)
            task = _get_task(job, crawler_id, log)
            continue

        crawler: CustomProcess = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
        crawler.start()
        log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

        while crawler.is_alive() or (Config.RESTART and (Config.LOG / f"job{job}crawler{crawler_id}.cache").exists()):
            if not crawler.is_alive():
                log.error("Crawler %s crashed with %s", task.crawler, crawler.exception)
                crawler.close()
                crawler = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.get_id(), log_path, modules))
                crawler.start()
                log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

            crawler.join(timeout=Config.RESTART_TIMEOUT)
            timelastentry = database.execute_sql("SELECT updated FROM task WHERE id=%s", (task.get_id(),)).fetchone()

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
            database.execute_sql("UPDATE task SET updated=%s, state='complete' WHERE id=%s", (task.updated, task.get_id()))

        if crawler.exception:
            # TODO save error in db?
            log.error("Crawler %s crashed with %s", task.crawler, crawler.exception)

        if Config.RESTART and (Config.LOG / f"job{job}crawler{crawler_id}.cache").exists():
            os.remove(Config.LOG / f"job{job}crawler{crawler_id}.cache")

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
    sys.exit(main(
        cast(str, args.get('job')),
        cast(int, args.get('crawlers')),
        cast(List[str], args.get('modules', [])),
        args.get('log'),
        cast(int, args.get('crawlerid')),
        cast(bool, args.get('listen'))
    ))
