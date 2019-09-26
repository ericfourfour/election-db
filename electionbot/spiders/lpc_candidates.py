import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class LPCCandidateSpider(scrapy.Spider):
    name = "lpc-candidate-spider"
    start_urls = ["https://www.liberal.ca/team-trudeau-2019-candidates/"]

    def parse(self, response):
        for sel in response.xpath("//li[contains(@class, 'candidate-card')]"):
            l = ItemLoader(item=ei.LPCCandidate(), selector=sel)
            l.add_xpath("name", "./@data-full-candidate-name")
            l.add_xpath("ed_code", "./@data-riding-riding_id")
            l.add_xpath("photo", ".//@data-photo-url")
            l.add_xpath("donate", ".//a[text()='Donate']/@href")
            l.add_xpath("website", ".//a[contains(@class, 'website')]/@href[1]")
            l.add_xpath("facebook", ".//a[contains(@class, 'facebook')]/@href[1]")
            l.add_xpath("twitter", ".//a[contains(@class, 'twitter')]/@href[1]")
            l.add_xpath("instagram", ".//a[contains(@class, 'instagram')]/@href[1]")
            l.add_xpath("bio", ".//div[@class='candidate-modal-bio']//text()")
            yield l.load_item()

