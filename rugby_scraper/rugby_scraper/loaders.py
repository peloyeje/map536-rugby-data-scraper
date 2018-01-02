# -*- coding: utf-8 -*-
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose


class MatchLoader(ItemLoader):
    default_output_processor = TakeFirst()

    won_in = MapCompose(lambda x: x == "won")

class MatchStatsLoader(ItemLoader):
    default_input_processor = MapCompose(lambda x: int(x) if x != "-" else None)
    default_output_processor = TakeFirst()
