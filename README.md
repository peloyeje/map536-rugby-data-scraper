# Rugby data scraper

This repository provides [Scrapy](scrapy.org) spiders to crawl rugby match/team/players data from the [espnscrum.com](espnscrum.com)

### Installation and usage

```shell
$ pip install -r requirements
$ cd scraper
$ scrapy crawl espn
```

The scraper stores scraped data into a SQLite database in /tmp/

### Available data

- Matches
    - Infos (teams, ground, date)
    - Basic statistics (tries, conversions, penalties, drops)
    - Extended statistics (kicks, mauls, etc.)
- Players
    - Basic info (name, birth date, height, weight)
    - Basic statistics per match (tries, conversions, penalties, drops)
    - Extended statistics (meters run, tackles, etc.)
- Game events (match, team, player, time, type)

### Authors

- Jean-Eudes Peloye
- Antoine Redier
