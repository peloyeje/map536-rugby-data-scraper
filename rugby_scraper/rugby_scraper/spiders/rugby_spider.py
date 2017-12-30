import re
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
        - match events in format : {...}
        this parser calls multiple other parser to deal with each situation
        """

        IFRAME_LINK_SELECTOR = "#win_old::attr(src)"
        iframe_link = response.css(IFRAME_LINK_SELECTOR).extract_first()
        yield response.follow(iframe_link, callback = self._match_iframe_parse)


    def _match_iframe_parse(self, response):
        """parser for the internal iframe of each match page"""
        #getting match id
        match_id_re = re.search("^http://stats.espnscrum.com/statsguru/rugby/current/match/(\d+).html", response.url)
        assert len(match_id_re.groups()) == 1, 'match id detection failed'
        match_id = int(match_id_re.group(1))
        assert type(match_id) is IntType , "match id is not an integer"
        yield {"match id" : match_id}
        #getting the info on the match
        INFO_SELECTOR = "#scrumContent .tabbertab"
        for info in response.css(INFO_SELECTOR):
            title = info.css("h2::text").extract_first()
            if title == "Teams" :
                pass
            elif title == "Match stats":
                pass
            elif title == "Timeline":
                pass
            elif re.search("^[a-zA-Z]+ stats$", title) :
                pass
