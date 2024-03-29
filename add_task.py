import argparse
import ast
import pathlib
import sys
from typing import List, Optional, Tuple, cast

import tld
from tld.exceptions import TldBadUrl, TldDomainNotFound

from database import Task, database
from loader.csvloader import CSVLoader


def main(job: str, urlspath: Optional[pathlib.Path], urls: Optional[List[Tuple[int, str]]]) -> int:
    # Prepare database
    with database.atomic():
        database.create_tables([Task])

    # Check for urls
    if urlspath is None and (not urls):
        return 0

    # Iterate over URLs and add them to database
    with database.atomic():  # speedup by using atomic transaction

        entry: Tuple[int, str]
        for entry in (CSVLoader(urlspath) if (urlspath is not None) else cast(List[Tuple[int, str]], urls)):
            url: str = ('https://' if not entry[1].startswith('http') else '') + entry[1]
            try:
                site: str = cast(tld.Result, tld.get_tld(url, as_object=True)).fld
            except (TldBadUrl, TldDomainNotFound):
                continue
            Task.create(job=job, crawler=None, site=site, url=url, landing_page=url, rank=int(entry[0]))

    return 0

if __name__ == '__main__':
    # Preparing command line argument parser
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-j", "--job", type=str, required=True, help="unique job id for crawl")
    args_parser.add_argument("-f", "--urlspath", type=pathlib.Path, help="path to file with urls")
    args_parser.add_argument("-u", "--urls", type=ast.literal_eval, nargs='+', help="urls to crawl")

    # Parse command line arguments
    args = vars(args_parser.parse_args())
    sys.exit(main(cast(str, args.get('job')), args.get('urlspath'),args.get('urls')))
