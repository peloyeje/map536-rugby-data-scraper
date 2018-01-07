# -*- coding: utf-8 -*-
import arrow

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Compose

def missing_values(entry):
    tokens = str(entry).split(" ")
    banned = ["-", "unknown", "circa"]

    if any((token in banned for token in tokens)):
        return None
    return entry

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
