import argparse
import pathlib
import sys
from typing import Optional, cast

import tld

import utils
from config import Config
from database import URL, Site, Task, load_database


def main(job: str, file: pathlib.Path) -> int:
    # Iterate over URLs and add them to database
    database = load_database()
    with database.atomic(), open(file, encoding="utf-8") as _file:
        for entry in _file:
            rank, url = entry.split(',')
            scheme: Optional[str] = 'https' if url.startswith('https') else ('http' if url.startswith('http') else 'https')
            url = ('https://' if not url.strip().startswith('http') else '') + url.strip()

            url_parsed: Optional[tld.Result] = utils.get_tld_object(url)
            if url_parsed is None:
                continue  # TODO log bad URL?

            site: Site = Site.get_or_create(
                scheme=scheme,
                tld=url_parsed.tld,
                site=utils.get_url_site(url_parsed)
            )[0]
            site.rank = int(rank)
            site.save()

            # Filter out tasks with adult sites
            if Config.ADULT_FILTER and site.adult:
                continue

            task: Task = Task.create(job=job, site=site)

            _url: URL = cast(URL, [
                URL.create(
                    task=task,
                    site=site,
                    url=url,
                    depth=0,
                    repetition=repetition
                )
                for repetition in range(1, Config.REPETITIONS + 1)
            ][0])

            task.landing = _url.id
            task.save()

    database.close()

    return 0

if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-j", "--job", type=str, required=True, help="unique job id for crawl")
    args_parser.add_argument("-f", "--file", type=str, required=True, help="path to tranco CSV file")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    sys.exit(main(cast(str, args.get('job')), args.get('file')))
