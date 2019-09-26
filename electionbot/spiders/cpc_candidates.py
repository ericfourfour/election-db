import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class CPCCandidateSpider(scrapy.Spider):
    name = "cpc-candidate-spider"
    start_urls = ["https://www.conservative.ca/team/2019-candidates/"]

    def parse(self, response):
        for sel in response.xpath("//div[@class='cabinet-member']"):
            l = ItemLoader(item=ei.LPCCandidate(), selector=sel)

            riding = sel.xpath(".//p[@class='riding-title']//text()").extract_first()
            more_link = sel.xpath(".//a[text()='Learn More']//@href").get()

            req = scrapy.Request(more_link, callback=self.parse_learn_more)
            req.meta["riding"] = riding

            yield req

    def parse_learn_more(self, response):
        l = ItemLoader(item=ei.CPCCandidate(), selector=response)

        l.add_value("riding", response.meta.get("riding"))

        title = l.nested_xpath("//div[@class='cell text-center']")
        title.add_xpath("name", "./h1/text()")
        title.add_xpath("nomination_dt", ".//p[@class='nomination_date']/text()")
        title.add_xpath("cabinet_position", "./p[not(@class)]/text()")

        social = l.nested_xpath("//section[@class='section section--social-share']")
        social.add_xpath("donate", ".//a[@data-type='donate']/@href")
        social.add_xpath("website", ".//a[@data-type='website']/@href")
        social.add_xpath("facebook", ".//a[@data-type='facebook']/@href")
        social.add_xpath("twitter", ".//a[@data-type='twitter']/@href")
        social.add_xpath("instagram", ".//a[@data-type='instagram']/@href")

        l.add_xpath("photo", "//img[@class='team-bio-image']/@src")
        l.add_xpath("bio", "//section[@class='section section--text-block']//text()")

        yield l.load_item()
