import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class PartySpider(scrapy.Spider):
    name = "party-spider"
    start_urls = [
        "https://www.elections.ca/content.aspx?section=pol&dir=par&document=index&lang=e"
    ]

    def parse(self, response):
        for sel in response.xpath('//div[@class="borderbox1"]'):
            l = ItemLoader(item=ei.Party(), selector=sel)
            l.add_xpath("title", ".//h3//text()")
            l.add_xpath("leader", ".//p[contains(span[1]/text(), 'Leader')]/text()")
            l.add_xpath("short_name", ".//p[contains(span[1]/text(), 'Short')]/text()")
            l.add_xpath(
                "eligible_dt", ".//p[contains(span[1]/text(), 'Eligible')]//text()"
            )
            l.add_xpath(
                "registered_dt", ".//p[contains(span[1]/text(), 'Register')]//text()"
            )
            l.add_xpath(
                "deregistered_dt",
                ".//p[contains(span[1]/text(), 'Deregister')]//text()",
            )
            l.add_xpath("website", "./h3/a/@href")
            l.add_xpath("logo", './/div[@class="logopp"]/img/@src')
            l.add_xpath(
                "national_headquarters",
                ".//p[contains(span[1]/text(), 'Headquarter')]//text()",
            )
            l.add_xpath(
                "chief_agent", ".//p[contains(span[1]/text(), 'Chief')]//text()"
            )
            l.add_xpath("auditor", ".//p[contains(span[1]/text(), 'Audit')]//text()")
            yield l.load_item()

