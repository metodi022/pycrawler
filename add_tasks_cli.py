import argparse
import ast
import pathlib
import sys
from typing import List, Optional, cast

import tld
from tld.exceptions import TldBadUrl, TldDomainNotFound

import utils
from config import Config
from database import URL, Site, Task, database


def main(job: str, urlspath: Optional[pathlib.Path], urls: List[str]) -> int:
    # Prepare database
    with database.atomic():
        database.create_tables([Site])
        database.create_tables([Task])

    # Check for urls
    if (urlspath is None) and (not urls):
        raise ValueError('URLs not specified.')

    # Iterate over URLs and add them to database
    with database.atomic():
        for entry in urls:
            url: str = ('https://' if not entry.startswith('http') else '') + entry
            try:
                url_parsed: tld.Result = tld.get_tld(url, as_object=True)
                _site: str = url_parsed.fld
                site: Site = Site.get_or_create(site=_site)[0]
                task: Task = Task.create(job=job, site=site)
                _url: URL = [
                    URL.create(task=task, site=site, url=url, scheme=url[:url.find(':')], origin=utils.get_url_origin(url_parsed), depth=0, repetition=repetition)
                    for repetition in range(1, Config.REPETITIONS + 1)
                    ][0]
                task.landing = _url
                task.save()
            except (TldBadUrl, TldDomainNotFound):
                # TODO log bad URL?
                pass

    return 0

if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-j", "--job", type=str, required=True, help="unique job id for crawl")
    args_parser.add_argument("-u", "--urls", type=ast.literal_eval, nargs='+', required=True, help="urls to crawl")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    sys.exit(main(cast(str, args.get('job')), args.get('urlspath'), args.get('urls')))
