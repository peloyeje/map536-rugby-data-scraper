# -*- coding: utf-8 -*-
import re
import regex

def get_player_id_from_name(name, team_dic) :
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

def get_team_dics_from_info(info):
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

def parse_match_stats(info, team) :
    """method that parses the Match stats tab of the match data
    format : {"match_id" : "placeholder", "team_id" : "placeholder", "pens_attempt" : int, "drops_attempt" : int, "kicks" : int, "passes" : int, "runs" : int, "meters" : int, "def_beaten" : int, "offloads" : int, "rucks_init" : init , "rucks_won" : int , "mall_init" : int, "mall_won" : int, "turnovers" : int, "tackles_made" : int, "tackles_missed" : int, "scrums_won_on_feed" : int, "scrums_lost_on_feed" : int, "lineouts_won_on_throw" : int, "lineouts_lost_on_throw" : int, }"""

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
