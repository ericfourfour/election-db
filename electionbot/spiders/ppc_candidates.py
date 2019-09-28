import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class PPCCandidateSpider(scrapy.Spider):
    name = "ppc-candidate-spider"
    start_urls = ["https://www.peoplespartyofcanada.ca/our_candidates"]

    def parse(self, response):
        for sel in response.xpath(
            "//table[@class='candidatetable']/tbody/tr[td[2][string-length(normalize-space(text())) > 5]]"
        ):
            l = ItemLoader(item=ei.PPCCandidate(), selector=sel)

            l.add_xpath("name", "./td[2]/text()")
            l.add_xpath("riding", "./td[1]/text()")
            l.add_xpath("website", "./td[5]/a/@href")
            l.add_xpath("facebook", "./td[4]/a/@href")
            l.add_xpath("twitter", "./td[3]/a/@href")

            yield l.load_item()

