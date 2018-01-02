# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field

class Match(Item):
    """Data structure to store basic match info"""

    match_id = Field()
    home_team_id = Field()
    away_team_id = Field()
    ground_id = Field()
    won = Field()
    date = Field()

class MatchStats(Item):
    """Data structure to store basic match stats"""

    match_id = Field()
    team_id = Field()
    scored = Field()
    conceded = Field()
    tries = Field()
    conversions = Field()
    penalties = Field()
    drops = Field()
