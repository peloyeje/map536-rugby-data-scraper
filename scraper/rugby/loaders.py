# -*- coding: utf-8 -*-
import arrow
import regex

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Compose

def missing_values(entry):
    tokens = str(entry).split(" ")
    banned = ["-", "unknown", "circa", "none"]

    if any(token in banned for token in tokens):
        return None
    return entry

def parse_date(date, loader_context):
    try:
        return arrow.get(date, loader_context.get("template", "D MMM YYYY"), locale = "en_us")
    except Exception:
        return None

def parse_won(won, loader_context):
    codes = loader_context.get("codes")
    if not codes or not type(codes) is dict:
        raise Exception("You must provide a codes dict")
    if not won in codes.keys():
        return None
    return codes.get(won)

def parse_weight(string):
    factor = 0.453592
    weight = str(string).replace("lb", "").strip()
    return int(int(weight) * factor)

def parse_height(string):
    factors = [0.3048, 0.0254]
    matches = regex.findall("(\d+)", str(string).strip())
    if matches and len(matches) <= 2:
        components = [int(value) * factors[i] for i, value in enumerate(matches)]
        return round(sum(components), 2)
    return None

def parse_stats(entry):
    return int(regex.sub("\D", "", str(entry)))

def parse_id(id):
    id = int(id)
    return id if id != 0 else None

class MatchLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, parse_id)
    default_output_processor = TakeFirst()

    won_in = MapCompose(missing_values, parse_won, codes={"won": 1, "lost": 2, "draw": 0})
    type_in = MapCompose(parse_id, lambda x: 2 if x == 3 else 1)
    date_in = MapCompose(missing_values, parse_date, template="D MMM YYYY")
    date_out = Compose(lambda x: x[0].datetime)

class MatchStatsLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, parse_stats)
    default_output_processor = TakeFirst()

class MatchExtraStatsLoader(ItemLoader):
    default_input_processor = MapCompose(int)
    default_output_processor = TakeFirst()

class TeamLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
    default_output_processor = TakeFirst()

    name_in = MapCompose(missing_values, lambda x: x[2:] if x[0:2] == "v " else x)

class PlayerLoader(ItemLoader):
    default_output_processor = TakeFirst()

    player_id_in = MapCompose(int)
    birthday_in = MapCompose(missing_values, parse_date, template="MMMM D, YYYY")
    birthday_out = Compose(lambda x: x[0].datetime)
    weight_in = MapCompose(missing_values, parse_weight)
    height_in = MapCompose(missing_values, parse_height)

class PlayerStatsLoader(ItemLoader):
    default_output_processor = TakeFirst()

    player_id_in = MapCompose(int)
    number_in = MapCompose(int)
    position_in = MapCompose(lambda x: x.upper())

class PlayerExtraStatsLoader(ItemLoader):
    default_input_processor = MapCompose(int)
    default_output_processor = TakeFirst()

class GameEventLoader(ItemLoader):
    default_input_processor = MapCompose(int)
    default_output_processor = TakeFirst()

    action_type_in = MapCompose(lambda x: x if x in ["tries", "pens", "cons", "drops"] else None)
