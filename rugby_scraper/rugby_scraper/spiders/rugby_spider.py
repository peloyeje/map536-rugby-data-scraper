import re
import regex
from urllib.parse import urljoin

from scrapy import Request
from scrapy.spider import BaseSpider
from rugby_scraper.items import Match, MatchStats, Team, Player, PlayerStats
from rugby_scraper.loaders import MatchLoader, MatchStatsLoader, TeamLoader, PlayerLoader, PlayerStatsLoader

class MainSpider(BaseSpider):
    """main spider of the scraper that will get all the statistics from the different pages of the website http://stats.espnscrum.com"""

    # Scrapy params
    name = "main_spider"
    allowed_domains = ["stats.espnscrum.com"]

    # Custom params
    follow_pages = False
    start_domain = "http://stats.espnscrum.com/"
    search_path = "/statsguru/rugby/stats/index.html"
    search_params = {
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

    player_params = {
        "class": 1, # ?,
        "template": "results",
        "type": "player",
        "view": "match",
    }

    def _generate_query_string(self, query_params):
        sep = ";"
        key_values = ["{}={}".format(k, v) for k, v in query_params.items()]
        return sep.join(key_values)

    def _generate_url(self, domain, path, query_params):
        query_string = self._generate_query_string(query_params)
        return urljoin(domain, "{}?{}".format(path, query_string))

    def _generate_search_url(self, **params):
        query_params = { **self.search_params, **params }
        return self._generate_url(domain = self.start_domain, path = self.search_path, query_params = query_params)

    def start_requests(self):
        """ Method that initializes the spider by getting the first page of the following queries :
        - all matches from all teams
        - from the 24 of july 1992 (date of the change in the way to count points in rugby)
        - ordered by date
        - grouped by home or away
        """
        for i in [1, 2]: # Get home matches then away matches
            yield Request(
                url = self._generate_search_url(page = 1, home_or_away = i),
                callback = self.match_list_parse,
                meta = { "home_or_away": i, "page": 1 })

    def match_list_parse(self, response):
        """
        """

        id_fields = {
            'match_id': 'li:nth-child(6) > a::attr(href)',
            'home_team_id': 'li:nth-child(3) > a::attr(href)',
            'away_team_id': 'li:nth-child(4) > a::attr(href)',
            'ground_id': 'li:nth-child(5) > a::attr(href)',
        }

        meta_fields = {
            "won": "td:nth-child(2)::text",
            "date": "td:nth-child(13) b::text"
        }

        stat_fields = {
            "scored": "td:nth-child(3)::text",
            "conceded": "td:nth-child(4)::text",
            "tries": "td:nth-child(6)::text",
            "conversions": "td:nth-child(7)::text",
            "penalties": "td:nth-child(8)::text",
            "drops": "td:nth-child(9)::text",
        }

        team_name_fields = {
            "home_team": "td:nth-child(1) a::text",
            "away_team": "td:nth-child(11)::text"
        }

        # Variable storing the index offset between the side menu divs and the rows
        offset = None

        for index, links in enumerate(response.css(".engine-dd")):
            if links.css("[id^=\"engine-dd-\"]"):
                # Skip these useless divs containing the UI
                continue

            if not offset:
                offset = index - 1

            ###
            # 1) Extract the basic match info into the Match structure
            ###
            loader = MatchLoader(item = Match(), response = response)
            # Subloader that handles the links in the side menu divs
            link_block_loader = loader.nested_css("#engine-dd{}".format(index - offset))
            for field, selector in id_fields.items():
                link_block_loader.add_css(field, selector, re = "\/([0-9]+)\.")
            # Subloader that handles the match info in the table rows (won, date)
            # We only have to get this info for the home team iteration, as they are mirrored for the away team
            if response.meta["home_or_away"] == 1:
                table_row_loader = loader.nested_css("tr.data1:nth-child({})".format(index - offset))
                for field, selector in meta_fields.items():
                    table_row_loader.add_css(field, selector)
            # Fetch the data
            match = loader.load_item()

            # Follow each match link to get additional info (player stats, etc.)
            # Scrapy will follow this link only for the first occurence of the match id so we avoid duplicates
            yield response.follow(
                url = "/statsguru/rugby/match/{}.html".format(match["match_id"]),
                callback = self.match_page_parse,
                meta = { "match" : match }
            )

            ###
            # 2) Extract basic match stats for each team into the MatchStats structure
            ###
            # The match stats are associated to the left-side team
            loader = MatchStatsLoader(item = MatchStats(), response = response)
            loader.add_value("match_id", match["match_id"])
            loader.add_value("team_id", match["home_team_id"] if response.meta["home_or_away"] == 1 else match["away_team_id"])
            for field, selector in stat_fields.items():
                loader.add_css(field, "tr.data1:nth-child({}) {}".format(index - offset, selector))
            # Fetch the data
            match_stats = loader.load_item()

            # Return it directly
            #yield match_stats

            ###
            # 3) Extract basic team profiles and create Team structures
            ###
            # Duplicates will be handled during pipeline processing
            for team, selector in team_name_fields.items():
                if match.get("{}_id".format(team)):
                    loader = TeamLoader(item = Team(), response = response)
                    loader.add_value("team_id", match.get("{}_id".format(team)))
                    loader.add_css("name", "tr.data1:nth-child({}) {}".format(index - offset, selector))
                    #yield loader.load_item()

        # Get next page link and follow it if there is still data to process
        if follow_pages:
            if links:
                page = response.meta["page"] + 1
                for i in [1, 2]: # Get home matches then away matches
                    yield Request(
                        url = self._generate_search_url(page = page, home_or_away = i),
                        callback = self.match_list_parse,
                        meta = { "home_or_away": i, "page": page })


    def player_info_parse(self, response):
        """player page parser that gets the info on a specific player
        it returns the information in the format : {...}
        """
        yield response.meta["player_info"]

    def player_matches_parse(self, response):
        """"""
        yield response.meta["player_stats"]


    def match_page_parse(self, response):
        """match page parser that gets all the info on the match itself, each teams statistics, each players statistcs.
        - scoring data in the format {"match" : match_id, "team": team_id, "tries" : [player_1_id, player_2_id, ...], "cons" : [player_1_id, ...], "pen" : [palyer_1_id, ...], "drops" : [player_1_id, ...]}
        - match scoring data format : {...}
        - player statistics format : {"match_id" : int, "team_id" : int, "pens_attempt" : int, "drops_attempt" : int, "kicks" : int, "passes" : int, "runs" : int, "meters" : int, "def_beaten" : int, "offloads" : int, "rucks_init" : init , "rucks_won" : int , "mall_init" : int, "mall_won" : int, "turnovers" : int, "tackles_made" : int, "tackles_missed" : int, "scrums_won_on_feed" : int, "scrums_lost_on_feed" : int, "lineouts_won_on_throw" : "int, "lineouts_lost_on_throw" : int }
        - team statistics format : {...}
        - match events in format : {"event_type" = "event_type", "match_id" = match_id, "team_id" = home_team_id, "player_id" : player_id, "event_time" : time} with time as int in minutes
        this parser calls multiple other parser to deal with each situation
        """
        # Extract iframe url with match data
        iframe = response.css("#win_old::attr(src)").extract_first()

        if iframe:
            yield response.follow(
                url = iframe,
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


    def _parse_teams_score_data(self, info, player_dict, match) :
        """generator that parses the hole scoring data section of the Teams tab
        format :
        - event data : {"event_type" : str, "team_id" : int, "player_id" : int, "event_time" : int}
        - score_data : {"team_id" : int, "match_id" : int, "tries" : [player_id_1, player_id_2, ...], "cons" : [player_id_1, ...], "pens" : [player_id_1, ...], "drops" : [player_id_1, ...]}
        """

        home_team_score_data = {"team_id" : match["home_team_id"], "match_id" : match["match_id"], "tries" : [], "cons" : [], "pens" : [], "drops" : []}
        away_team_score_data = {"team_id" : match["away_team_id"], "match_id" : match["match_id"], "tries" : [], "cons" : [], "pens" : [], "drops" : []}
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
            tries_results = self._parse_team_score_data (event_type, "Tries", info_str, player_dict["home"])
            for result in tries_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "try", "match_id" : match["match_id"], "team_id" : match["home_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            cons_results = self._parse_team_score_data(event_type, "Cons", info_str, player_dict["home"])
            for result in cons_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "con", "match_id" : match["match_id"], "team_id" : match["home_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            pens_results = self._parse_team_score_data(event_type, "Pens", info_str, player_dict["home"])
            for result in pens_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "pen", "match_id" : match["match_id"], "team_id" : match["home_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            drops_results = self._parse_team_score_data(event_type, "Drops", info_str, player_dict["home"])
            for result in drops_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "drop", "match_id" : match["match_id"], "team_id" : match["home_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            tries_results = self._parse_team_score_data (event_type, "Tries", info_str, player_dict["away"])
            for result in tries_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "try", "match_id" : match["match_id"], "team_id" : match["away_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            cons_results = self._parse_team_score_data(event_type, "Cons", info_str, player_dict["away"])
            for result in cons_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "con", "match_id" : match["match_id"], "team_id" : match["away_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            pens_results = self._parse_team_score_data(event_type, "Pens", info_str, player_dict["away"])
            for result in pens_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "pen", "match_id" : match["match_id"], "team_id" : match["away_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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
            drops_results = self._parse_team_score_data(event_type, "Drops", info_str, player_dict["away"])
            for result in drops_results :
                try:
                    got_event = result["event"]
                    event = {"event_type" : "drop", "match_id" : match["match_id"], "team_id" : match["away_team_id"], "player_id" : got_event[1], "event_time" : got_event[0]}
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

        yield {"score_data" : home_team_score_data}
        yield {"score_data" : away_team_score_data}


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

        # Start the actual parsing
        # 1) Get an array of the tabs indexed by title
        tabs = response.css("#scrumContent .tabbertab")
        if not tabs:
            return # If no tabs, we have no match info, so drop this request

        tabs = [(tab.css("h2::text").extract_first(), tab) for tab in tabs]
        tabs = { tab[0]: tab[1] for tab in tabs if tab[0]}

        # 2) Get all players in the match from the "Teams" tab. For each team line-up,
        #    - extract player ids from list and creates requests to player page
        #    - extract match specific info and creates requests to player match page

        if "Teams" not in tabs:
            return # We ain't gonna do nothin' bru

        # LEGACY : create dict to match _parse_teams_score_data inputs
        player_dict = { "home": {}, "away": {}}
        # For each team
        teams = tabs["Teams"].css("table tr:last-child .divTeams")
        for index, team in enumerate(teams):
            # For each team group (first team or replacements)
            for position, group in enumerate(team.xpath("table")):
                # For each player (discard first rows - subtitles)
                players = team.css("tr.liveTblRowWht")[1:]
                for player in players:
                    # Get basic info
                    player_loader = PlayerLoader(item = Player(), response = response, selector = player)
                    player_loader.add_css("player_id", "a.liveLineupTextblk::attr(href)", re = "\/([0-9]+)\.")
                    player_loader.add_css("name", "a.liveLineupTextblk::text")
                    player_info = player_loader.load_item()
                    # Discard players without id
                    if not player_info:
                        continue
                    # Go to the player page to scrape it
                    #yield player_info
                    # yield response.follow(
                    #     url = "/statsguru/rugby/player/{}.html".format(player_info["player_id"]),
                    #     callback = self.player_info_parse,
                    #     meta = { "player_info" : player_info }
                    # )

                    player_stats_fields = {
                        "number" : "td.liveTblTextGrn::text",
                        "position" : "td.liveTblColCtr::text",
                    }
                    # Get match-specific info for each player
                    player_stats_loader = PlayerStatsLoader(item = PlayerStats(), response = response, selector = player)
                    player_stats_loader.add_value("player_id", player_info["player_id"])
                    player_stats_loader.add_value("team_id", match["home_team_id"] if index == 0 else match["away_team_id"])
                    player_stats_loader.add_value("match_id", match["match_id"])
                    player_stats_loader.add_value("first_team", position == 0)
                    for field, selector in player_stats_fields.items():
                        player_stats_loader.add_css(field, selector)
                    player_stats = player_stats_loader.load_item()

                    #yield player_stats
                    # Experimental : go to the match list of the player to retrieve match stats (pens/cons/tries/drops)
                    # yield response.follow(
                    #     url = "/statsguru/rugby/player/{}.html?{}".format(player_info["player_id"], self._generate_query_string(self.player_params)),
                    #     callback = self.player_matches_parse,
                    #     meta = { "player_stats": player_stats }
                    # )

                    # LEGACY : populate dict
                    player_dict["home" if index == 0 else "away"][player_info["player_id"]] = (player_info["name"], player_stats.get("position"), player_stats.get("number"))

        # Abort parsing if we don't have info on players
        if not player_dict["home"] or not player_dict["away"]:
            return

        # 3) Parse top summary to retrieve the names of the players who scored
        event_score_results = self._parse_teams_score_data(tabs["Teams"], player_dict, match)
        for event_score in event_score_results:
            if not event_score:
                continue
            yield event_score

        # Analysing the rest of the tabs in the match page
        if "Match stats" in tabs:
            home_match_stats = self._parse_match_stats(tabs["Match stats"], team = "home")
            if home_match_stats:
                home_match_stats["match_id"] = match["match_id"]
                home_match_stats["team_id"] = match["home_team_id"]
                yield {"match_stat_data" : home_match_stats}

            away_match_stats = self._parse_match_stats(tabs["Match stats"], team = "away")
            if away_match_stats:
                away_match_stats["match_id"] = match["match_id"]
                away_match_stats["team_id"] = match["away_team_id"]
                yield {"match_stat_data" : away_match_stats}

        # if "Timeline" in tabs:
        #     pass
        for tab in { title: tabs[title] for title in tabs.keys() if re.search("^[a-zA-Z ]+ stats$", title) }:
            for player_row in tab.css("table tr") :
                player_stats = self._parse_player_stats(player_row, potential_team = [player_dict["home"], player_dict["away"]], potential_team_id = [match["home_team_id"], match["away_team_id"]])
                if player_stats :
                    player_stats["match_id"] = match["match_id"]
                    yield {"player_stats" : player_stats}
