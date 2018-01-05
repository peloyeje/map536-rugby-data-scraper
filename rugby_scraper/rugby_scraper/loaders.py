# -*- coding: utf-8 -*-
import arrow

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose

def missing_values(entry):
    return entry if entry != "-" else None

class MatchLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
    default_output_processor = TakeFirst()

    won_in = MapCompose(missing_values, lambda x: x == "won")
    date_in = MapCompose(missing_values, lambda x: arrow.get(x, "D MMM YYYY", locale = "en_us"))

class MatchStatsLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
    default_output_processor = TakeFirst()

class TeamLoader(ItemLoader):
    default_input_processor = MapCompose(missing_values, int)
    default_output_processor = TakeFirst()

    name_in = MapCompose(missing_values, lambda x: x[2:] if x[0:2] == "v " else x)

class PlayerLoader(ItemLoader):
    default_output_processor = TakeFirst()

    player_id_in = MapCompose(int)

class PlayerStatsLoader(ItemLoader):
    default_output_processor = TakeFirst()

    player_id_in = MapCompose(int)
    number_in = MapCompose(int)
    position_in = MapCompose(lambda x: x.upper())

class GameEventLoader(ItemLoader):
    default_input_processor = MapCompose(int)
    default_output_processor = TakeFirst()

    action_type_in = MapCompose(lambda x: x if x in ["tries", "pens", "cons", "drops"] else None)
