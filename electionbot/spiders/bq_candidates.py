import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from scrapy.spiders import Rule
import electionbot.items as ei


class BQCandidateSpider(scrapy.Spider):
    name = "bq-candidate-spider"
    start_urls = ["http://www.blocquebecois.org/candidats/"]

    Rules = (
        Rule(
            LinkExtractor(
                allow=(), restrict_xpaths=("//div[@class='nav-next']//a/@href",)
            ),
            callback="parse",
            follow=True,
        ),
    )

    def parse(self, response):
        for sel in response.xpath("//div[contains(@class, 'candidates')]//article"):
            l = ItemLoader(item=ei.BQCandidate(), selector=sel)

            name = l.add_xpath("name", ".//div[@class='infos']//a[1]/@title")
            riding = l.add_xpath("riding", ".//div[@class='infos']//a[1]/@title")
            photo = l.add_xpath("photo", ".//img/@src")

            links = l.nested_xpath(".//span[@class='links']")
            links.add_xpath("website", ".//a[contains(text(), 'Site Web')]/@href")
            links.add_xpath("facebook", ".//a[contains(i/@class, 'facebook')]/@href")
            links.add_xpath("instagram", ".//a[contains(i/@class, 'instagram')]/@href")
            links.add_xpath("twitter", ".//a[contains(i/@class, 'twitter')]/@href")

            contact = l.nested_xpath(".//div[@class='contact']")
            contact.add_xpath("phone", "./text()")
            contact.add_xpath("email", ".//a[@class='email-coded']/@data-mail")

            l.add_xpath("bio", ".//div[@class='desc']//text()")

            yield l.load_item()

        next_page = response.xpath("//div[@class='nav-next']//a/@href").get()
        if next_page:
            request = scrapy.Request(url=next_page)
            yield request
