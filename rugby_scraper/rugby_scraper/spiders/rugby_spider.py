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






    def player_page_parse(self, response):
        """player page parser that gets the info on a specific player
        it returns the information in the format : {...}
        """

        pass


    def match_page_parse(self, response):
        """match page parser that gets all the info on the match itself, each teams statistics, each players statistcs.
        - match data format : {...}
        - player statistics format : {...}
        - team statistics format : {...}
        """

        pass 
