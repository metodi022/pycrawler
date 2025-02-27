# PyCrawler

PyCrawler is a Python-based extendible and modular crawling framework that uses the [Playwright](https://playwright.dev/) browser automation tool.

## Requirements
1. Python 3.10+
2. PostgreSQL or SQLite
3. Playwright

## Installation Instructions
1. Clone the project and its submodules
2. Create a Python virtual environment and activate it
3. Install the requirements from the `requirements.txt` text file
4. Install additionally browser binaries using `playwright install`. For more information, check [this article](https://playwright.dev/python/docs/intro) and [this article](https://playwright.dev/python/docs/browsers)
5. Copy `config-example.py` to `config.py` and edit it accordingly
6. Run `python prepare_database.py` to populate and prepare the database

## Starting the Crawl
You can edit the `config.py` file to specify the PostgreSQL database and additional crawling parameters before the crawl.

The `add_tasks_tranco.py` scripts allows you to specify a list of sites (Tranco format) which the crawler will be visiting.

```
usage: add_tasks_tranco.py [-h] -j JOB -f FILE

options:
  -h, --help            show this help message and exit
  -j JOB, --job JOB     unique job id for crawl
  -f FILE, --file FILE  path to tranco CSV file
```

```
usage: main.py [-h] [-m [MODULES ...]] -j JOB -c CRAWLERS [-i CRAWLERID] [-l] [-o LOG]

options:
  -h, --help            show this help message and exit
  -m [MODULES ...], --modules [MODULES ...]
                        which modules the crawler will run
  -j JOB, --job JOB     unique job id for crawl
  -c CRAWLERS, --crawlers CRAWLERS
                        how many crawlers will run concurrently
  -i CRAWLERID, --crawlerid CRAWLERID
                        starting crawler id (default 1); must be > 0
  -l, --listen          crawler will not stop if there is no job; query and sleep until a job is found
  -o LOG, --log LOG     path to directory for log output
```

For example, if we want to start a single crawler to find login forms, we use the following command:
`main.py -m FindLoginForms -j <your-job-id> -c 1`

## Modules
You can find existing modules in the `./modules` directory. Additionally, you can create your own modules to do something specific. To do that:
1. Implement the interface from `./modules/module.py`
2. The `register_job` method is called whenever the database is setup
3. The `add_handlers` method is called every time before visiting a page; here you can register listeners for the browser and its pages
4. The `receive_response` is run whenever the crawler visits a page
5. The `add_url_filter_out` allows you to specify functions which will filter out and ignore certain URLs during the crawling process

Check existing modules as a guideline to understand how to construct your own modules.
