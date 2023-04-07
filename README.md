# PyCrawler

PyCrawler is a Python-based extendible and modular crawling framework that uses the [Playwright](https://playwright.dev/) browser automation tool.

## Requirements
1. Python 3.10
2. PostgreSQL
3. Playwright

## Installation Instructions
1. Clone the project
2. Create a Python virtual environment and activate it
3. Install the requirements from the `requirements.txt` text file
4. Install additionally browser binaries using `playwright install`. For more information, check [this article](https://playwright.dev/python/docs/intro) and [this article](https://playwright.dev/python/docs/browsers)

## Starting the Crawl
You can edit the `config.py` file to specify the PostgreSQL database and additional crawling parameters.

Running `main.py -h` shows additional options:
```
usage: main.py [-h] [-o LOG] [-f URLSPATH] [-u URLS [URLS ...]] [-m [MODULES ...]] -j JOB -c CRAWLERS [-i CRAWLERID] [-l]

options:
  -h, --help            show this help message and exit
  -o LOG, --log LOG     path to directory where output log will be saved
  -f URLSPATH, --urlspath URLSPATH
                        path to file with urls
  -u URLS [URLS ...], --urls URLS [URLS ...]
                        urls to crawl
  -m [MODULES ...], --modules [MODULES ...]
                        which modules the crawler will run
  -j JOB, --job JOB     unique job id for crawl
  -c CRAWLERS, --crawlers CRAWLERS
                        how many crawlers will run concurrently
  -i CRAWLERID, --crawlerid CRAWLERID
                        starting crawler id (default 1); must be > 0
  -l, --listen          crawler will not stop if there is no job; query and sleep until a job is found
```

For example, if we want to start a single crawler to find login forms, we use the following command:
`main.py -m FindLoginForms -j <your-job-id> -c 1`

The `-f` and `-u` options allows you to specify a list of sites which the crawler will be visiting. The sites follow the [Tranco List](https://tranco-list.eu/) CSV format (`rank, domain`).

## Modules
You can find existing modules in the `./modules` directory. Additionally, you can create your own modules to do something specific. To do that:
1. Implement the interface from `./modules/module.py`
2. The `register_job` method is called whenever the database is setup
3. The `add_handlers` method is called every time before visiting a page; here you can register listeners for the browser and its pages
4. The `receive_response` is run whenever the crawler visits a page
5. The `add_url_filter_out` allows you to specify functions which will ignore certain URLs during the crawling process

Check existing modules as a guideline to how to construct your own module.
