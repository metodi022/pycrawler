import argparse
import pathlib
import sys
from typing import Optional, cast

import tld
from peewee import ProgrammingError

import utils
from config import Config
from database import URL, Site, Task, database


def main(job: str, file: Optional[pathlib.Path]) -> int:
    # Prepare database
    with database.atomic():
        database.create_tables([Site])
        database.create_tables([Task])
        database.create_tables([URL])

        try:
            Task._schema.create_foreign_key(Task.landing)
        except ProgrammingError:
            pass

    # Iterate over URLs and add them to database
    with database.atomic(), open(file, encoding="utf-8") as _file:
        for entry in _file:
            rank, url = entry.split(',')
            url = ('https://' if not url.strip().startswith('http') else '') + url.strip()

            url_parsed: Optional[tld.Result] = utils.get_tld_object(url)
            if url_parsed is None:
                # TODO log bad URL?
                continue

            site: Site = Site.get_or_create(
                scheme=utils.get_url_scheme(url_parsed),
                site=utils.get_url_site(url_parsed)
            )[0]
            site.rank = int(rank)
            site.save()

            task: Task = Task.create(job=job, site=site)

            _url: URL = [
                URL.create(
                    task=task,
                    site=site,
                    url=url,
                    depth=0,
                    repetition=repetition
                )
                for repetition in range(1, Config.REPETITIONS + 1)
            ][0]

            task.landing = _url
            task.save()

    return 0

if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-j", "--job", type=str, required=True, help="unique job id for crawl")
    args_parser.add_argument("-f", "--file", type=str, required=True, help="path to tranco CSV file")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    sys.exit(main(cast(str, args.get('job')), args.get('file')))
