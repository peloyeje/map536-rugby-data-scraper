# -*- coding: utf-8 -*-
import arrow
import regex

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Compose

def missing_values(entry):
    tokens = str(entry).split(" ")
    banned = ["-", "unknown", "circa"]

    if any((token in banned for token in tokens)):
        return None
    return entry

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

class MatchLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
    default_output_processor = TakeFirst()

    won_in = MapCompose(missing_values, lambda x: x == "won")
    date_in = MapCompose(missing_values, lambda x: arrow.get(x, "D MMM YYYY", locale = "en_us"))
    date_out = Compose(lambda x: x[0].isoformat())

class MatchStatsLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
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
    birthday_in = MapCompose(missing_values, lambda x: arrow.get(x, "MMMM D, YYYY", locale = "en_us"))
    birthday_out = Compose(lambda x: x[0].isoformat())
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
