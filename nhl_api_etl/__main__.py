import sys
from .scraper import crawl_stats_in_year, crawl_events_in_year

output_dir = None
year = int(sys.argv[2])
if len(sys.argv) >= 4:
    output_dir = sys.argv[3]

if sys.argv[1] == "linescore":
    crawl_stats_in_year(year, output_dir)
elif sys.argv[1] == "livefeed":
    crawl_events_in_year(year, output_dir)