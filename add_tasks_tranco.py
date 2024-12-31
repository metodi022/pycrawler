import argparse
import pathlib
import re
import sys
from typing import Optional, Set, cast

import tld
from peewee import ProgrammingError

import utils
from config import Config
from database import URL, Site, Task, database


def main(job: str, file: Optional[pathlib.Path]) -> int:
    # Get easylist adult sites
    if Config.ADULT_FILTER:
        adult_filter: Set[str] = set()

        # Create easylist_adult.txt
        if not pathlib.Path('easylist/easylist_adult/easylist_adult.txt').exists():
            for easylist_adult_file in pathlib.Path('easylist/easylist_adult').glob('*.txt'):
                with open(easylist_adult_file, 'r', encoding='utf-8') as easylist_adult:
                    for line in easylist_adult:
                        if line.startswith('!'):
                            continue
                        adult_filter.update(
                            utils.get_url_site(utils.get_tld_object(entry) or utils.get_tld_object('https://' + entry))
                            for entry in re.split(r'[^a-zA-Z0-9\\:\\/\\@\\-\\_\\.]', line)
                            if (utils.get_tld_object(entry) or utils.get_tld_object('https://' + entry)) is not None
                        )
            with open('easylist/easylist_adult/easylist_adult.txt', 'w', encoding='utf-8') as easylist_adult:
                easylist_adult.writelines(line + '\n' for line in adult_filter if '.' in line)
        # Read easylist_adult.txt
        else:
            with open('easylist/easylist_adult/easylist_adult.txt', 'r', encoding='utf-8') as easylist_adult:
                adult_filter.update(line.strip() for line in easylist_adult.readlines())

    # Prepare database
    with database.atomic():
        database.create_tables([Site])
        database.create_tables([Task])
        database.create_tables([URL])

        if not Config.SQLITE:
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
                continue        # TODO log bad URL?

            # Filter out adult urls
            if Config.ADULT_FILTER and (utils.get_url_site(url_parsed) in adult_filter):
                continue        # TODO log adult URL?

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
