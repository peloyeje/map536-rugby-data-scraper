import re
import regex

from scrapy import Request
from scrapy.spider import BaseSpider
from rugby_scraper.items import Match, MatchStats
from rugby_scraper.loaders import MatchLoader, MatchStatsLoader

class MainSpider(BaseSpider):
    """main spider of the scraper that will get all the statistics from the different pages of the website http://stats.espnscrum.com"""

    # Scrapy params
    name = "main_spider"
    allowed_domains = ["stats.espnscrum.com"]

    # Custom params
    start_endpoint = "http://stats.espnscrum.com/statsguru/rugby/stats/index.html"
    default_endpoint_params = {
        "class": 1, # ?,
        "home_or_away": 1, # Only returns home team entries
        "orderby": "date",
        "page": 1,
        "size": 200, # Results per page
        "spanmin1": "24+Jul+1992", # Lower bound date
        "spanval1": "span", # ?
        "template": "results",
        "type": "team",
        "view": "match",
    }

    def _generate_search_url(self, **custom_params):
        query_params = { **self.default_endpoint_params, **custom_params }
        query_string = ";".join(["{}={}".format(k, v) for k, v in query_params.items()])

        return "{}?{}".format(self.start_endpoint, query_string)

    def start_requests(self):
        """ Method that initializes the spider by getting the first page of the following queries :
        - all matches from all teams
        - from the 24 of july 1992 (date of the change in the way to count points in rugby)
        - ordered by date
        - grouped by home or away
        """
        #for i in [1, 2]: # Get home matches then away matches
        yield Request(
            url = self._generate_search_url(page = 1, home_or_away = 1),
            callback = self.match_list_parse,
            meta = { "home_or_away": 1 }
        )

    def match_list_parse(self, response):
        """
        """

        id_fields = {
            'match_id': 'li:nth-child(6) > a::attr(href)',
            'home_team_id': 'li:nth-child(3) > a::attr(href)',
            'away_team_id': 'li:nth-child(4) > a::attr(href)',
            'ground_id': 'li:nth-child(5) > a::attr(href)',
        }

        bool_fields = {
            "won": "td:nth-child(2)::text"
        }

        stat_fields = {
            "scored": "td:nth-child(3)::text",
            "conceded": "td:nth-child(4)::text",
            "tries": "td:nth-child(6)::text",
            "conversions": "td:nth-child(7)::text",
            "penalties": "td:nth-child(8)::text",
            "drops": "td:nth-child(9)::text",
        }

        offset = None

        for index, links in enumerate(response.css(".engine-dd")):
            if links.css("[id^=\"engine-dd-\"]"):
                # Skip these useless divs
                continue

            if not offset:
                offset = index - 1

            # Extract the basic match info
            loader = MatchLoader(item = Match(), response = response)
            for field, selector in id_fields.items():
                loader.add_css(field,
                    "#engine-dd{} {}".format(index - offset, selector),
                    re = "\/([0-9]+)\.")
            for field, selector in bool_fields.items():
                loader.add_css(field,
                    "tr.data1:nth-child({}) {}".format(index - offset, selector))

            match = loader.load_item()
            # Scrapy will follow this link only for the first occurence of the match id so we avoid duplicates
            yield response.follow(
                url = "/statsguru/rugby/match/{}.html".format(match["match_id"]),
                callback = self.match_page_parse,
                meta = { "match" : match }
            )

            # Extract match stats for the given team
            loader = MatchStatsLoader(item = MatchStats(), response = response)
            loader.add_value("match_id", match["match_id"])
            loader.add_value("team_id", match["home_team_id"] if response.meta["home_or_away"] == 1 else match["away_team_id"])
            for field, selector in stat_fields.items():
                loader.add_css(field,
                    "tr.data1:nth-child({}) {}".format(index - offset, selector))

            # Yiels stats object
            yield loader.load_item()

        # Get next page link and follow it
        #NEXT_PAGE_SELECTOR = "#scrumArticlesBoxContent table:nth-child(3) tr td:nth-child(2) span:last-child a::attr(href)"
        #next_page_link = response.css(NEXT_PAGE_SELECTOR).extract_first()
        #yield {"page" : response.url}
        #yield response.follow(next_page_link, callback = self.match_list_parse)


    def player_page_parse(self, response):
        """player page parser that gets the info on a specific player
        it returns the information in the format : {...}
        """

        pass


    def match_page_parse(self, response):
        """match page parser that gets all the info on the match itself, each teams statistics, each players statistcs.
        - scoring data in the format {"match" : match_id, "team": team_id, "tries" : [player_1_id, player_2_id, ...], "cons" : [player_1_id, ...], "pen" : [palyer_1_id, ...], "drops" : [player_1_id, ...]}
        - match scoring data format : {...}
        - player statistics format : {"match_id" : int, "team_id" : int, "pens_attempt" : int, "drops_attempt" : int, "kicks" : int, "passes" : int, "runs" : int, "meters" : int, "def_beaten" : int, "offloads" : int, "rucks_init" : init , "rucks_won" : int , "mall_init" : int, "mall_won" : int, "turnovers" : int, "tackles_made" : int, "tackles_missed" : int, "scrums_won_on_feed" : int, "scrums_lost_on_feed" : int, "lineouts_won_on_throw" : "int, "lineouts_lost_on_throw" : int }
        - team statistics format : {...}
        - match events in format : {"event_type" = "event_type", "match_id" = match_id, "team_id" = home_team_id, "player_id" : player_id, "event_time" : time} with time as int in minutes
        this parser calls multiple other parser to deal with each situation
        """
        url = response.css("#win_old::attr(src)").extract_first()

        if url:
            yield response.follow(
                url = url,
                callback = self._match_iframe_parse,
                meta = response.meta
            )


    def _get_player_id_from_name(self, name, team_dic) :
        """method that allows to get the id of a player from his name and the dic of his team
        should accept names as : name, initials name"""
        potential = []
        final = []
        name = name.upper().strip()
        for player_id , player_info in team_dic.items() :
            player_name = player_info[0].upper().strip()
            player_name_list = player_name.split(" ")
            researched_name_list = name.split(" ")
            if researched_name_list[-1] in player_name_list[-1] :
                potential.append(player_id)
        if len(potential) == 0 :
            raise RuntimeError ("no name was detected")
        elif len(potential) == 1 :
            return potential[0]
        else:
            #if all the potentials are equals :
            if all(potential[0] == rest for rest in potential) :
                return potential[0]
            if len(name.split(" ")) == 1 :
                raise RuntimeError("two many names containing the exact researched name")
            else:
                researched_first_leter = name.split(" ")[0][0]
                researched_last_name = name.split(" ")[-1]
                for potential_id in potential :
                    potential_name = team_dic[potential_id][0].upper().strip()
                    potential_first_letter = potential_name.split(" ")[0][0]
                    potential_last_name = potential_name.split(" ")[-1]
                    if potential_first_letter == researched_first_leter and potential_last_name == researched_last_name:
                        final.append(potential_id)
                if len(final) == 1:
                    return final[0]
                else:
                    #if all final are equals we return it :
                    if final and all(final[0] == rest for rest in final) :
                        return final[0]
                    raise RuntimeError ("could not find name")



    def _get_team_dics_from_info(self, info):
        """method that get the teams dicts from the info"""
        #home team
        HOME_PLAYER_ROW = "table tr td:nth-child(1) div table tr"
        #if the team tables are present we proceed
        if not info.css(HOME_PLAYER_ROW):
            #if the home team tables are not present we skip to the next information source in the page
            return None

        players_row = info.css(HOME_PLAYER_ROW)
        PLAYER_NUMBER_SELECTOR = "td:nth-child(1)::text"
        PLAYER_POS_SELECTOR = "td:nth-child(2)::text"
        PLAYER_NAME_SELECTOR = "td:nth-child(3) table a::text"
        PLAYER_URL_SELECTOR = "td:nth-child(3) table a::attr(href)"
        home_team_player_dic = {}
        for player_row in players_row:
            player_number = player_row.css(PLAYER_NUMBER_SELECTOR).extract_first()
            player_position = player_row.css(PLAYER_POS_SELECTOR).extract_first()
            player_name = player_row.css(PLAYER_NAME_SELECTOR).extract_first()
            if not player_row.css(PLAYER_URL_SELECTOR).extract_first() :
                #if player url not found, we skip to next player
                continue
            player_url = player_row.css(PLAYER_URL_SELECTOR).extract_first()
            #INSERT PLAYER PAGE SCRAPING LINK
            player_id_re = re.search("^[\D\-_.:]+/(\d+).html$", player_url)

            #checking that we are selecting a valid player row
            if not player_id_re :
                #if player has no id in url, we skip to next player
                continue
            assert len(player_id_re.groups()) == 1 , "found more than one player id in url"
            player_id = int(player_id_re.group(1))
            home_team_player_dic[player_id] = (player_name, player_position, player_number)

        #away team
        AWAY_PLAYER_ROW = "table tr td:nth-child(2) div table tr"
        #if the team tables are present we proceed
        if not info.css(AWAY_PLAYER_ROW):
            #if the away team tables are not present we skip to the next information source in the page
            return None
        players_row = info.css(AWAY_PLAYER_ROW)
        away_team_player_dic = {}
        for player_row in players_row:
            player_number = player_row.css(PLAYER_NUMBER_SELECTOR).extract_first()
            player_position = player_row.css(PLAYER_POS_SELECTOR).extract_first()
            player_name = player_row.css(PLAYER_NAME_SELECTOR).extract_first()
            if not player_row.css(PLAYER_URL_SELECTOR).extract_first() :
                #if player url not found, we skip to next player
                continue
            player_url = player_row.css(PLAYER_URL_SELECTOR).extract_first()
            #INSERT PLAYER PAGE SCRAPING LINK
            player_id_re = re.search("^[\D\-_.:]+/(\d+).html$", player_url)

            #checking that we are selecting a valid player row
            if not player_id_re :
                #if player has no id in url, we skip to next player
                continue
            assert len(player_id_re.groups()) == 1 , "found more than one player id in url"
            player_id = int(player_id_re.group(1))
            away_team_player_dic[player_id] = (player_name, player_position, player_number)

        return (home_team_player_dic, away_team_player_dic)

    def _parse_match_stats(self, info, team) :
        """method that parses the Match stats tab of the match data
        format : {"match_id" : "placeholder", "team_id" : "placeholder", "pens_attempt" : int, "drops_attempt" : int, "kicks" : int, "passes" : int, "runs" : int, "meters" : int, "def_beaten" : int, "offloads" : int, "rucks_init" : init , "rucks_won" : int , "mall_init" : int, "mall_won" : int, "turnovers" : int, "tackles_made" : int, "tackles_missed" : int, "scrums_won_on_feed" : int, "scrums_lost_on_feed" : int, "lineouts_won_on_throw" : "int, "lineouts_lost_on_throw" : int, }"""

        match_stats = {"match_id" : "placeholder", "team_id" : "placeholder"}
        assert team in ["home", "away"] , "team to analyse must be either home or away"
        DATA_LINE_SELECTOR = "table tr"
        if not info.css(DATA_LINE_SELECTOR) :
            return None
        for data_line in info.css(DATA_LINE_SELECTOR) :
            #selecting data_line title
            title = data_line.css("td:nth-child(2)::text")
            if not title:
                continue
            title = title.extract_first()
            #selecting data_line value
            if team == "home" :
                VALUE_SELECTOR = "td:nth-child(1)::text"
            else:
                VALUE_SELECTOR = "td:nth-child(3)::text"
            value = data_line.css(VALUE_SELECTOR)
            if not value:
                continue
            value = value.extract_first()

            #analysing the data itself
            #attempted penalties
            if title == "Penalty goals":
                cons_attempt_re = regex.match("[0-9]+ from ([0-9]+)", value)
                if not cons_attempt_re :
                    continue
                cons_attempt = int(cons_attempt_re.captures(1)[0])
                match_stats["pens_attempt"] = cons_attempt
            #attempted drops
            if title == "Dropped goals":
                drops_re = regex.match("([0-9]+)( \(([0-9]+) missed\))?", value)
                if not drops_re:
                    continue
                drops_scored = int(drops_re.captures(1)[0])
                drops_missed = drops_re.captures(3)
                if not drops_missed:
                    drops_missed = 0
                else:
                    drops_missed = int(drops_missed[0])
                drops_attempt = drops_scored + drops_missed
                match_stats["drops_attempt"] = drops_attempt
            #kicks from hand
            if title == "Kicks from hand":
                kicks = int(value)
                match_stats["kicks"] = kicks
            #passes
            if title == "Passes":
                passes = int(value)
                match_stats["passes"] = passes
            #runs
            if title == "Runs":
                runs = int(value)
                match_stats["runs"] = runs
            #meters run with ball
            if title == "Metres run with ball":
                meters = int(value)
                match_stats["meters"] = meters
            #clean breacks
            if title == "Clean breaks":
                breaks = int(value)
                match_stats["breaks"] = breaks
            #defenders beaten
            if title == "Defenders beaten":
                def_beaten = int(value)
                match_stats["def_beaten"] = def_beaten
            #Offloads
            if title == "Offloads":
                offloads = int(value)
                match_stats["offloads"] = offloads
            #rucks both initiated and won
            if title == "Rucks won":
                rucks_re = regex.match("^\\n([0-9]+) from ([0-9]+)", value)
                if not rucks_re :
                    continue
                rucks_init = int(rucks_re.captures(2)[0])
                match_stats["rucks_init"] = rucks_init
                rucks_won = int(rucks_re.captures(1)[0])
                match_stats["rucks_won"] = rucks_won
            #mauls both initiated and won
            if title == "Mauls won":
                mall_re = regex.match("^\\n([0-9]+) from ([0-9]+)", value)
                if not mall_re :
                    continue
                mall_init = int(mall_re.captures(2)[0])
                match_stats["mall_init"] = mall_init
                mall_won = int(mall_re.captures(1)[0])
                match_stats["mall_won"] = mall_won
            #turnovers
            if title == "Turnovers conceded":
                turnovers = int(value)
                match_stats["turnovers"] = turnovers
            #tackles
            if title == "Tackles made/missed":
                tackles_re = regex.match("^([0-9]+)/([0-9]+)$", value)
                if not tackles_re :
                    continue
                tackles_made = int(tackles_re.captures(1)[0])
                match_stats["tackles_made"] = tackles_made
                tackles_missed = int(tackles_re.captures(2)[0])
                match_stats["tackles_missed"] = tackles_missed
            #scrums
            if title == "Scrums on own feed":
                scrums_re = regex.match("^\\n\\t  ([0-9]+) won, ([0-9]+) lost", value)
                if not scrums_re:
                    continue
                scrums_won_on_feed = int(scrums_re.captures(1)[0])
                match_stats["scrums_won_on_feed"] = scrums_won_on_feed
                scrums_lost_on_feed = int(scrums_re.captures(2)[0])
                match_stats["scrums_lost_on_feed"] = scrums_lost_on_feed
            #lineouts
            if title == "Lineouts on own throw":
                lineout_re = regex.match("^\\n\\t  ([0-9]+) won, ([0-9]+) lost", value)
                if not lineout_re:
                    continue
                lineouts_won_on_throw = int(lineout_re.captures(1)[0])
                match_stats["lineouts_won_on_throw"] = lineouts_won_on_throw
                lineouts_lost_on_throw = int(lineout_re.captures(2)[0])
                match_stats["lineouts_lost_on_throw"] = lineouts_lost_on_throw
            #penalties
            if title == "Penalties conceded":
                penalties = int(value)
                match_stats["penalties"] = penalties
            #cards
            if title == "Yellow/red cards":
                cards_re = regex.match("^([0-9]+)/([0-9]+)$", value)
                if not cards_re:
                    continue
                yellow_cards = int(cards_re.captures(1)[0])
                match_stats["yellow_cards"] = yellow_cards
                red_cards = int(cards_re.captures(2)[0])
                match_stats["red_cards"] = red_cards


        return match_stats

    def _parse_player_stats(self, row, potential_team, potential_team_id ):
        """method that parses players match stats from row,
        format : {"match_id" : "placeholder", "player_id" : int, }
        """

        assert type(potential_team) is list, "potential teams must be in a list"
        assert type(potential_team_id) is list, "potential teams id must be in a list"
        assert len(potential_team) == len(potential_team_id) and len(potential_team) == 2, "potential teams and team ids must be of same length 2"

        player_stats = {"match_id" : "placeholder"}

        #getting the player name and deducing his id and his team id
        player_name = row.css("td:nth-child(2)::text")
        if not player_name :
            return None
        player_name = player_name.extract_first()
        try :
            home_player_id = self._get_player_id_from_name(player_name, potential_team[0])
        except RuntimeError:
            home_player_id = None
        try:
            away_player_id = self._get_player_id_from_name(player_name, potential_team[1])
        except RuntimeError:
            away_player_id = None

        if away_player_id and home_player_id:
            return None
        elif not(away_player_id or home_player_id) :
            return None
        elif home_player_id :
            player_id = home_player_id
            team_id = potential_team_id[0]
        elif away_player_id:
            player_id = away_player_id
            team_id = potential_team_id[1]
        else:
            return None
        player_stats["player_id"] = player_id
        player_stats["team_id"] = team_id

        #getting the statistics
        #tries and assists
        tries_assists = row.css("td:nth-child(3)::text")
        if tries_assists:
            tries_assists = tries_assists.extract_first()
            tries_assists_re = regex.match("^([0-9]+)/([0-9])+$", tries_assists)
            if tries_assists_re:
                tries = int(tries_assists_re.captures(1)[0])
                player_stats["tries"] = tries
                assists = int(tries_assists_re.captures(2)[0])
                player_stats["assists"] = assists
        #points
        points = row.css("td:nth-child(4)::text")
        if points:
            points = int(points.extract_first())
            player_stats["points"] = points
        #kicks runs passes
        k_r_p = row.css("td:nth-child(5)::text")
        if k_r_p:
            k_r_p = k_r_p.extract_first()
            k_r_p_re = regex.match("^([0-9]+)/([0-9]+)/([0-9]+)$", k_r_p)
            if k_r_p_re:
                kicks = int(k_r_p_re.captures(1)[0])
                player_stats["kicks"] = kicks
                passes = int(k_r_p_re.captures(2)[0])
                player_stats["passes"] = passes
                runs = int(k_r_p_re.captures(3)[0])
                player_stats["runs"] = runs
        #meters ran
        meters_ran = row.css("td:nth-child(6)::text")
        if meters_ran:
            meters_ran = int(meters_ran.extract_first())
            player_stats["meters_ran"] = meters_ran
        #clean breacks
        breaks = row.css("td:nth-child(7)::text")
        if breaks:
            breaks = int(breaks.extract_first())
            player_stats["breaks"] = breaks
        #defenders beaten
        defenders_beaten = row.css("td:nth-child(8)::text")
        if defenders_beaten:
            defenders_beaten = int(defenders_beaten.extract_first())
            player_stats["defenders_beaten"] = defenders_beaten
        #offloads
        offloads = row.css("td:nth-child(9)::text")
        if offloads:
            offloads = int(offloads.extract_first())
            player_stats["offloads"] = offloads
        #turnovers
        turnovers = row.css("td:nth-child(10)::text")
        if turnovers:
            turnovers = int(turnovers.extract_first())
            player_stats["turnovers"] = turnovers
        #tackles made and missed
        tackles = row.css("td:nth-child(11)::text")
        if tackles:
            tackles = tackles.extract_first()
            tackles_re = regex.match("^([0-9]+)/([0-9]+)", tackles)
            if tackles_re:
                tackles_made = int(tackles_re.captures(1)[0])
                player_stats["tackles_made"] = tackles_made
                tackles_missed = int(tackles_re.captures(2)[0])
                player_stats["tackles_missed"] = tackles_missed
        #lineouts
        lineouts = row.css("td:nth-child(12)::text")
        if lineouts:
            lineouts = lineouts.extract_first()
            lineouts_re = regex.match("^([0-9]+)/([0-9]+)$", lineouts)
            if lineouts_re:
                lineouts_won_on_throw = int(lineouts_re.captures(1)[0])
                player_stats["lineouts_won_on_throw"] = lineouts_won_on_throw
                lineouts_stolen_from_opp = int(lineouts_re.captures(2)[0])
                player_stats["lineouts_stolen_from_opp"] = lineouts_stolen_from_opp
        #penalties conceeded
        pens_conceeded = row.css("td:nth-child(13)::text")
        if pens_conceeded:
            pens_conceeded = int(pens_conceeded.extract_first())
            player_stats["pens_conceeded"] = pens_conceeded
        #cards
        cards = row.css("td:nth-child(14)::text")
        if cards:
            cards = cards.extract_first()
            cards_re = regex.match("^([0-9]+)/([0-9]+)", cards)
            if cards_re:
                yellow_cards = int(cards_re.captures(1)[0])
                red_cards = int(cards_re.captures(2)[0])

        return player_stats


    def _parse_teams_score_data(self, info, match_id,  home_team_player_dic, home_team_id, away_team_player_dic, away_team_id) :
        """generator that parses the hole scoring data section of the Teams tab
        format :
        - event data : {"event_type" : str, "team_id" : int, "player_id" : int, "event_time" : int}
        - score_data : {"team_id" : int, "match_id" : int, "tries" : [player_id_1, player_id_2, ...], "cons" : [player_id_1, ...], "pens" : [player_id_1, ...], "drops" : [player_id_1, ...]}
        """

        home_team_score_data = {"team_id" : home_team_id, "match_id" : match_id, "tries" : [], "cons" : [], "pens" : [], "drops" : []}
        away_team_score_data = {"team_id" : away_team_id, "match_id" : match_id, "tries" : [], "cons" : [], "pens" : [], "drops" : []}
        #home team
        HOME_EVENT_ROW_SELECTOR = ".liveTblScorers:nth-child(1)"
        for row in info.css(HOME_EVENT_ROW_SELECTOR):
            try:
                event_type = row.css("span::text").extract()
                assert len(event_type) == 1, "did not find exacty one event type while parsing the teams info"
                event_type = event_type[0]
            except AssertionError:
                continue

            try :
                info_str = row.css("td::text").extract()
                assert len(info_str) == 1 , "did not find exactly one info string for a scoring data"
                info_str = info_str[0]
            except AssertionError:
                continue
            if info_str == "\nnone     ":
                continue

            #tries events
            tries_results = self._parse_team_score_data (event_type, "Tries", info_str, home_team_player_dic)
            for result in tries_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "try", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    try_player_id = result["score"]
                    home_team_score_data["tries"].append(try_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #cons events
            cons_results = self._parse_team_score_data(event_type, "Cons", info_str, home_team_player_dic)
            for result in cons_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "con", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    cons_player_id = result["score"]
                    home_team_score_data["cons"].append(cons_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #pens events
            pens_results = self._parse_team_score_data(event_type, "Pens", info_str, home_team_player_dic)
            for result in pens_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "pen", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    pens_player_id = result["score"]
                    home_team_score_data["pens"].append(pens_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #drops events
            drops_results = self._parse_team_score_data(event_type, "Drops", info_str, home_team_player_dic)
            for result in drops_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "drop", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    drops_player_id = result["score"]
                    home_team_score_data["drops"].append(drops_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass

        #away team
        AWAY_EVENT_ROW_SELECTOR = ".liveTblScorers:nth-child(2)"
        for row in info.css(AWAY_EVENT_ROW_SELECTOR):
            try:
                event_type = row.css("span::text").extract()
                assert len(event_type) == 1, "did not find exacty one event type while parsing the teams info"
                event_type = event_type[0]
            except AssertionError:
                continue

            try :
                info_str = row.css("td::text").extract()
                assert len(info_str) == 1 , "did not find exactly one info string for a scoring data"
                info_str = info_str[0]
            except AssertionError:
                continue
            if info_str == "\nnone     ":
                continue

            #tries events
            tries_results = self._parse_team_score_data (event_type, "Tries", info_str, away_team_player_dic)
            for result in tries_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "try", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    try_player_id = result["score"]
                    away_team_score_data["tries"].append(try_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #cons events
            cons_results = self._parse_team_score_data(event_type, "Cons", info_str, away_team_player_dic)
            for result in cons_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "con", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    con_player_id = result["score"]
                    away_team_score_data["cons"].append(con_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #pens events
            pens_results = self._parse_team_score_data(event_type, "Pens", info_str, away_team_player_dic)
            for result in pens_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "pen", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    pens_player_id = result["score"]
                    away_team_score_data["pens"].append(pens_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass
            #drops events
            drops_results = self._parse_team_score_data(event_type, "Drops", info_str, away_team_player_dic)
            for result in drops_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "drop", "match_id" : match_id, "team_id" : home_team_id, "player_id" : got_event[1], "event_time" : got_event[0]}
                    yield {"event_data" : event}
                except KeyError :
                    pass
                try:
                    drops_player_id = result["score"]
                    away_team_score_data["drops"].append(drops_player_id)
                except KeyError:
                    pass
                try:
                    debug = result["debug"]
                    yield {"debug" : debug}
                except KeyError:
                    pass

        yield{"score_data" : home_team_score_data}
        yield{"score_data" : away_team_score_data}


    def _parse_team_score_data (self, event_type, wanted_event_type, info_str, team_dic):
        """generator that returns the event_data and the player_ids from a single line of event data in the Teams tab
        format :
        - event_data : {"event" : (time, player_id)}
        - player_id : {"score" : player_id}
        """

        if event_type == wanted_event_type:
            #get all the events player and times for the home team
            events_for_players_re = regex.match("^\\n(([a-zA-Z'éàè^éäëüï ]+[\d ]*(\([\d, ]+\))*),?)+\s$", info_str)
            if not events_for_players_re :
                return None
            events_for_players = events_for_players_re.captures(2)
            #analyse each individual player
            for events_per_player in events_for_players :
                name_number_time_re = regex.match("^([a-zA-Z'éàè^éäëüï ]+)([\d ]*)(\(([\d]+)[, ]*\))?", events_per_player)
                if not name_number_time_re :
                    continue
                player_name = name_number_time_re.captures(1)[0]
                number_events = name_number_time_re.captures(2)
                time_events = name_number_time_re.captures(4)
                    #try to get the player id from info string
                try :
                    player_id = self._get_player_id_from_name(player_name, team_dic)
                except RuntimeError :
                    if not regex.match("^[\s]+$", player_name):
                        player_id = "unknown"
                #get the proper info and pass it in pipeline
                if time_events and time_events[0]:
                    for time in time_events:
                        time = int(time)
                        yield {"score" : player_id}
                        pre_event = (time ,player_id)
                        yield {"event" : pre_event}
                elif number_events and number_events[0]:
                    for i in range(0, int(number_events[0])) :
                        yield {"score" : player_id}
                else :
                    yield {"score" : player_id}

    def _match_iframe_parse(self, response):
        """parser for the internal iframe of each match page"""

        # Get the forwarded match data
        match = response.meta.get('match')
        match_id, home_team_id, away_team_id = [match["match_id"], match["home_team_id"], match["away_team_id"]]

        home_team_score_data = {"match" : match_id, "team_id" : home_team_id, "tries" : [], "cons" : [], "pens" : [], "drops" : []}
        away_team_score_data = {"match" : match_id, "team_id" : away_team_id, "tries" : [], "cons" : [], "pens" : [], "drops" : []}

        #getting the info on the match first to have the player_dics
        INFO_SELECTOR = "#scrumContent .tabbertab"
        for info in response.css(INFO_SELECTOR):
            title = info.css("h2::text").extract_first()
            if title == "Teams":
                #getting the players lists in format {player_id :(player_name, player_position, player_number), ...}
                get_teams_result = self._get_team_dics_from_info(info)
                if not get_teams_result:
                    continue
                home_team_player_dic , away_team_player_dic = get_teams_result
                #getting the score and event data
                event_score_results = self._parse_teams_score_data(info, match_id, home_team_player_dic, home_team_id, away_team_player_dic, away_team_id)
                for event_score in event_score_results:
                    if not event_score:
                        continue
                    yield event_score

        #analysing the rest of the tabs in the match page
        for info in response.css(INFO_SELECTOR):
            title = info.css("h2::text").extract_first()
            if title == "Match stats":
                home_match_stats = self._parse_match_stats(info, team = "home")
                if not home_match_stats :
                    continue
                home_match_stats["match_id"] = match_id
                home_match_stats["team_id"] = home_team_id
                yield {"match_stat_data" : home_match_stats}

                away_match_stats = self._parse_match_stats(info, team = "away")
                if not away_match_stats:
                    continue
                away_match_stats["match_id"] = match_id
                away_match_stats["team_id"] = away_team_id
                yield {"match_stat_data" : away_match_stats}

            elif title == "Timeline":
                pass
            elif re.search("^[a-zA-Z ]+ stats$", title) :
                PLAYER_ROW_SELCTOR = "table tr"
                if not home_team_player_dic and away_team_player_dic :
                    continue
                for player_row in info.css(PLAYER_ROW_SELCTOR) :
                    player_stats = self._parse_player_stats(player_row, potential_team = [home_team_player_dic, away_team_player_dic], potential_team_id = [home_team_id, away_team_id])
                    if not player_stats :
                        continue
                    player_stats["match_id"] = match_id
                    yield {"player_stats" : player_stats}
