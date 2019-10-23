import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class NDPCandidateSpider(scrapy.Spider):
    name = "ndp-candidate-spider"
    start_urls = ["https://www.ndp.ca/candidates"]

    def parse(self, response):
        for sel in response.xpath(
            "//div[@class='campaign-civics-list-items']/div[contains(@class, 'campaign-civics-list-item')]"
        ):
            l = ItemLoader(item=ei.NDPCandidate(), selector=sel)

            civic_data = l.nested_xpath(".//div[@class='civic-data']")

            civic_data.add_xpath("name", "./@data-fullname")
            civic_data.add_xpath("ed_code", "./@data-riding-code")
            civic_data.add_xpath("cabinet_position", "./@data-cabinet-position")
            civic_data.add_xpath("donate", "./@data-donate-link")
            civic_data.add_xpath("volunteer", "./@data-volunteer-link")
            civic_data.add_xpath("lawnsign", "./@data-lawnsign-link")
            civic_data.add_xpath("website", "./@data-website-link")
            civic_data.add_xpath("facebook", "./@data-facebook-link")
            civic_data.add_xpath("instagram", "./@data-instagram-link")
            civic_data.add_xpath("twitter", "./@data-twitter-link")
            civic_data.add_xpath("bio", "./@data-bio")

            l.add_xpath(
                "photo",
                ".//img[contains(@class, 'campaign-civics-list-image')]/@data-img-src",
            )

            yield l.load_item()

