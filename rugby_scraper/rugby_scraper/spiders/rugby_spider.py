import re
import regex
import scrapy

class rugby_spider (scrapy.Spider) :
    """main spider of the scraper that will get all the statistics from the different pages of the website http://stats.espnscrum.com"""

    name = "main_spider"


    def start_requests(self):
        """ method  that initializes the spider by getting the first page of the following query :
        - all matches from all teams
        - from the 24 of july 1992 (date of the change in the way to count points in rugby)
        - ordered by date
        """

        urls = ["http://stats.espnscrum.com/statsguru/rugby/stats/index.html?class=1;filter=advanced;orderby=date;page=1;spanmin1=15+aug+1992;spanval1=span;template=results;type=team;view=match"]
        for url in urls :
            yield scrapy.Request(url = url, callback = self.match_list_parse)


    def match_list_parse(self, response):
        """parser that reads the pages with the lists of all the match.
        it aims at getting the match info page
        it also find the next list page to find
        """

        #selecting the matches menu to parse match info page
        MENU_SELECTOR = ".engine-dd"
        for menu in response.css(MENU_SELECTOR):
            menu_id = menu.css("div::attr(id)").extract_first()
            if re.search("^engine-dd[0-9]+$", menu_id) :
                #selecting the match description link in the page
                MATCH_LINK_SELECTOR = "ul li:nth-child(6) a::attr(href)"
                match_link = menu.css(MATCH_LINK_SELECTOR).extract_first()
                yield response.follow(match_link, callback = self.match_page_parse)

        ##selecting next list page to parse
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
        - scoring data in the format {"match" : match_id, "team": team_id, "tries" : [player_1_id, player_2_id, ...], "cons" : [player_1_id, ...], "pen" : [palyer_1_id, ...]}
        - match data format : {...}
        - player statistics format : {...}
        - team statistics format : {...}
        - match events in format : {"event_type" = "event_type", "match_id" = match_id, "team_id" = home_team_id, "player_id" : player_id, "event_time" : time} with time as int in minutes
        this parser calls multiple other parser to deal with each situation
        """

        IFRAME_LINK_SELECTOR = "#win_old::attr(src)"
        iframe_link = response.css(IFRAME_LINK_SELECTOR).extract_first()
        yield response.follow(iframe_link, callback = self._match_iframe_parse)


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
            player_id = player_id_re.group(1)
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
            player_id = player_id_re.group(1)
            away_team_player_dic[player_id] = (player_name, player_position, player_number)

            return (home_team_player_dic, away_team_player_dic)

    def _match_iframe_parse(self, response):
        """parser for the internal iframe of each match page"""
        #getting match id
        match_id_re = re.search("^http://stats.espnscrum.com/statsguru/rugby/current/match/(\d+).html", response.url)
        assert len(match_id_re.groups()) == 1, 'match id detection failed'
        match_id = int(match_id_re.group(1))
        assert type(match_id) is int , "match id is not an integer"

        home_team_id = "FAKE HOME TEAM ID"
        away_team_id = "FAKE AWAY TEAM ID"

        home_team_score_data = {"match" : match_id, "team_id" : home_team_id, "tries" : [], "cons" : [], "pen" : []}

        #getting the info on the match
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
                    if event_type == "Tries":
                        #get all the tries player and times for the home team
                        tries_for_players_re = regex.match("^\\n(([a-zA-Z ]+[\d ]*(\([\d, ]+\))*),?)+\s$", info_str)
                        if not tries_for_players_re :
                            continue
                        tries_for_players = tries_for_players_re.captures(2)
                        #analyse each individual player
                        for tries_per_player in tries_for_players :
                            name_number_time_re = regex.match("^([a-zA-Z ]+)([\d ]*)(\(([\d]+)[, ]*\))?", tries_per_player)
                            if not name_number_time_re :
                                continue
                            player_name = name_number_time_re.captures(1)[0]
                            number_tries = name_number_time_re.captures(2)
                            time_tries = name_number_time_re.captures(4)
                                #try to get the player id from info string
                            try :
                                player_id = self._get_player_id_from_name(player_name, home_team_player_dic)
                            except RuntimeError :
                                player_id = "unkwon"
                            #get the proper info and pass it in pipeline
                            if time_tries and time_tries[0]:
                                for time in time_tries:
                                    time = int(time)
                                    home_team_score_data["tries"].append(player_id)
                                    yield {"event_type" : "try", "match_id" : match_id, "team_id" : home_team_id, "player_id" : player_id, "event_time" : time}
                            elif number_tries and number_tries[0]:
                                for i in range(0, int(number_tries[0])) :
                                    home_team_score_data["tries"].append(player_id)
                            else :
                                home_team_score_data["tries"].append(player_id)

                    yield{"home tries" : home_team_score_data}





            elif title == "Match stats":
                pass
            elif title == "Timeline":
                pass
            elif re.search("^[a-zA-Z]+ stats$", title) :
                pass
