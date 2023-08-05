import argparse
import importlib
import os
import pathlib
import re
import sys
import time
import traceback
from datetime import datetime, timedelta
from logging import FileHandler, Formatter, Logger
from multiprocessing import Pipe, Process
from typing import List, Optional, Type

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


def main(job: str, crawlers_count: int, module_names: List[str], log_path: Optional[pathlib.Path] = None, starting_crawler_id: int = 1, listen: bool = False) -> int:
    # Create log path if needed
    log_path = (log_path or Config.LOG).resolve()
    if not log_path.exists():
        os.mkdir(log_path)

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
    if module_names and ' ' in module_names[0]:
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
            database.execute_sql("UPDATE task SET crawler=%s, state='progress' WHERE id=%s", (crawler_id, result[0]))
            task = Task.get_by_id(result[0])

    return task


def _manage_crawler(job: str, crawler_id: int, log_path: pathlib.Path, modules: List[Type[Module]], listen: bool) -> None:
    log = _get_logger(job, crawler_id, log_path)

    task: Optional[Task] = _get_task(job, crawler_id, log)

    while task or listen:
        if not task:
            time.sleep(60)
            task = _get_task(job, crawler_id, log)
            continue

        crawler: CustomProcess = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.id, log_path, modules))
        crawler.start()
        log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

        while crawler.is_alive() or (Config.RESTART_TIMEOUT and (Config.LOG / f"job{job}crawler{crawler_id}.cache").exists()):
            if not crawler.is_alive():
                log.error("Crawler %s crashed with %s", task.crawler, crawler.exception)
                crawler.close()
                # TODO update task in db with error?
                crawler = CustomProcess(target=_start_crawler, args=(job, crawler_id, task.id, log_path, modules))
                crawler.start()
                log.info("Start crawler %s PID %s", crawler_id, crawler.pid)

            crawler.join(timeout=Config.RESTART_TIMEOUT)
            timelastentry = _get_last_log_entry(log_path / f"job{job}crawler{crawler_id}.log")

            if crawler.is_alive():
                if (datetime.today() - timelastentry).seconds < Config.RESTART_TIMEOUT:
                    continue
            else:
                continue

            log.error("Close stale crawler %s", task.crawler)

            crawler.terminate()
            crawler.join(timeout=30)

            if crawler.is_alive():
                crawler.kill()
                time.sleep(5)

        crawler.close()

        task = Task.get_by_id(task.get_id())
        task.state = 'complete'

        if crawler.exception:
            # TODO update task in db with error?
            task.error = 'Crash' if not task.error else 'Crash, ' + task.error
            log.warning("Crawler %s crashed with %s", task.crawler, crawler.exception)

        task.save()

        if Config.RESTART and (Config.LOG / f"job{job}crawler{crawler_id}.cache").exists():
            os.remove(Config.LOG / f"job{job}crawler{crawler_id}.cache")

        task = _get_task(job, crawler_id, log)

    log.handlers[-1].close()


def _start_crawler(job: str, crawler_id: int, task: int, log_path: pathlib.Path, modules: List[Type[Module]]) -> None:
    log = _get_logger(job, crawler_id, log_path)
    log.info('Start crawler')
    crawler: Crawler = Crawler(job, crawler_id, task, log, modules)
    crawler.start_crawl()
    log.info('Stop crawler')
    log.handlers[-1].close()


def _get_logger(job: str, crawler_id: int, log_path: pathlib.Path) -> Logger:
    handler: FileHandler = FileHandler(log_path / f"job{job}crawler{crawler_id}.log")
    handler.setFormatter(Formatter('%(asctime)s %(levelname)s %(message)s'))
    log = Logger(f"Job {job} Crawler {crawler_id}")
    log.setLevel(Config.LOG_LEVEL)
    log.addHandler(handler)
    return log


def _get_last_log_entry(path: str | pathlib.Path) -> datetime:
    error_time = datetime.today() - timedelta(seconds=Config.RESTART_TIMEOUT)
    last_line: bytes = b''

    try:
        with open(path, mode='rb') as file:
            file.seek(-2, 2)

            while re.match('\\d{4}-\\d{2}-\\d{2}', last_line.decode("utf-8", errors="ignore")) is None:
                file.seek(-(len(last_line) + 2) if len(last_line) > 0 else 0, 1)

                while file.read(1) != b'\n':
                    try:
                        file.seek(-2, 1)
                    except OSError:
                        file.seek(-1, 1)
                        break

                last_line = file.readline() or b''
    except Exception:
        return error_time
    
    try:
        last_line = last_line.decode("utf-8", errors="ignore").split()
    except Exception:
        return error_time
    
    if not last_line:
        return error_time
    
    try:
        return datetime.strptime(last_line[0] + ' ' + last_line[1], '%Y-%m-%d %H:%M:%S,%f')
    except Exception:
        return error_time


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
        args.get('job'),
        args.get('crawlers'),
        args.get('modules') or [],
        args.get('log'),
        args.get('crawlerid'),
        args.get('listen')
    ))
