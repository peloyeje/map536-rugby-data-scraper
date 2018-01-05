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

class MatchExtraStats(Item):
    """Data structure to store extended match stats"""

    match_id = Field()
    team_id = Field()
    pens_attempt = Field()
    pens_conceeded = Field()
    drops_attempt = Field()
    kicks = Field()
    passes = Field()
    runs = Field()
    meters = Field()
    breaks = Field()
    def_beaten = Field()
    offloads = Field()
    rucks_init = Field()
    rucks_won = Field()
    mall_init = Field()
    mall_won = Field()
    turnovers = Field()
    tackles_made = Field()
    tackles_missed = Field()
    scrums_won_on_feed = Field()
    scrums_lost_on_feed = Field()
    lineouts_won_on_throw = Field()
    lineouts_lost_on_throw = Field()
    yellow_cards = Field()
    red_cards = Field()

class Team(Item):
    """Data structure to store basic team info"""

    team_id = Field()
    name = Field()

class Player(Item):
    """Data structure to store basic player info"""

    player_id = Field()
    name = Field()
    height = Field()
    weight = Field()

class PlayerStats(Item):
    """Data structure to store player stats per match"""

    player_id = Field()
    team_id = Field()
    match_id = Field()
    position = Field()
    number = Field()
    first_team = Field()
    tries = Field()
    cons = Field()
    pens = Field()
    drops = Field()

class PlayerExtraStats(Item):
    """Data structure to store extended match stats"""

    player_id = Field()
    team_id = Field()
    match_id = Field()
    tries = Field()
    assists = Field()
    points = Field()
    kicks = Field()
    passes = Field()
    runs = Field()
    meters = Field()
    breaks = Field()
    def_beaten = Field()
    offloads = Field()
    turnovers = Field()
    tackles_made = Field()
    tackles_missed = Field()
    lineouts_won_on_throw = Field()
    lineouts_stolen_from_opp = Field()
    pens_conceeded = Field()
    yellow_cards = Field()
    red_cards = Field()

class GameEvent(Item):
    """Data structure to store game events (tries, penalties, etc.)"""

    player_id = Field()
    team_id = Field()
    match_id = Field()
    time = Field()
    action_type = Field()
    extra_info = Field()
