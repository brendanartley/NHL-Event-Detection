import sys

from .scraper import crawl_all
crawl_all(sys.argv[1], sys.argv[2])