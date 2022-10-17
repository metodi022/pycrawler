# PyCrawler

PyCrawler is a Python-based extendible and modular crawling framework that uses the [Playwright](https://playwright.dev/) browser automation tool.

## Requirements
1. Python 3
2. PostgreSQL

## Installation Instructions
1. Clone the project
2. Create a Python virtual environment and activate it
3. Install the requirements from the `requirements.txt` text file
4. Install additionally browser binaries using `playwright install`. For more information, check [this article](https://playwright.dev/python/docs/intro)

## Starting the Crawl
You can edit the `config.py` file to specify the PostgreSQL database and additional crawling parameters.

Running `main.py -h` shows additional options:
```
  -h, --help            show this help message and exit
  -o LOG, --log LOG     path to directory where output log will be saved
  -f URLS, --urls URLS  path to file with urls
  -m [MODULES ...], --modules [MODULES ...]
                        which modules the crawler will run
  -j JOB, --job JOB     unique job id for crawl
  -c CRAWLERS, --crawlers CRAWLERS
                        how many crawlers will run concurrently
  -s, --setup           run setup for DB and modules
  ```

For example, if we want to start a single crawler to find login forms, we use the following command:
`main.py -o ./tmp/ -f sites.csv -m FindLoginForms -j 1 -c 1 -s`

The `-f` option requires you to specify a list of sites which the crawler will be visiting. The sites follow the [Tranco List](https://tranco-list.eu/) CSV format (`rank, domain`).

## Modules
You can find existing modules in the `./modules` directory. Additionally, you can create your own modules to do something specific. To do that, implement the interface from `./modules/module.py`. The `register_job` method is called whenever the database is setup. The `add_handlers` method is called every time before starting the crawl for a site. Here you can register listeners for the browser and its pages. The `receive_response` is run whenever the crawler visits a page. Finally, the `add_url_filter_out` allows you to specify functions which will ignore certain URLs during the crawling process.

Check existing modules as a guideline to how to construct your own module.
