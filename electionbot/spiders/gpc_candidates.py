import scrapy
from scrapy.loader import ItemLoader
import electionbot.items as ei


class GPCCandidateSpider(scrapy.Spider):
    name = "gpc-candidate-spider"
    start_urls = ["https://www.greenparty.ca/en/candidates"]

    def parse(self, response):
        for sel in response.xpath("//div[contains(@class, 'candidate-card')]"):

            ed_code = sel.xpath(".//div[@class='well']/@data-target").extract_first()

            more_link = sel.xpath(
                ".//a[contains(text(), 'Read') and contains(text(), 'more')]/@href"
            ).get()

            req = scrapy.Request(
                f"https://www.greenparty.ca{more_link}", callback=self.parse_read_more
            )
            req.meta["ed_code"] = ed_code

            yield req

    def parse_read_more(self, response):
        l = ItemLoader(item=ei.GPCCandidate(), selector=response)
        l.add_value("ed_code", response.meta.get("ed_code"))

        main = l.nested_xpath("//div[@id='block-system-main']")
        main.add_xpath("name", ".//h2/text()")
        main.add_xpath("photo", ".//img/@src")
        main.add_xpath(
            "donate", ".//a[normalize-space(text()[1])='Donate locally']/@href"
        )
        main.add_xpath(
            "volunteer", ".//a[normalize-space(text()[1])='Volunteer locally']/@href"
        )
        main.add_xpath("facebook", ".//a[@title='Facebook']/@href")
        main.add_xpath("instagram", ".//a[@title='Instagram']/@href")
        main.add_xpath("twitter", ".//a[@title='Twitter']/@href")
        main.add_xpath(
            "email",
            ".//i[contains(@class, 'fa-envelope-square')]/following-sibling::a/@href",
        )
        main.add_xpath(
            "phone", ".//i[contains(@class, 'fa-phone')]/following-sibling::text()"
        )
        main.add_xpath("bio", ".//div[contains(@class, 'field-candidate-bio')]//text()")

        yield l.load_item()

