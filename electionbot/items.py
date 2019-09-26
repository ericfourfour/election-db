import dateparser as dp
import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from w3lib.html import remove_tags


def clean_field(value: str) -> str:
    return value.replace(":", "")


def resolve_relative_url(value: str) -> str:
    return f"https://www.elections.ca{value}"


def strip_remove_blanks(value: str) -> str:
    value = value.strip()
    if value == "":
        return None
    return value


def cleanse_address(value: str) -> str:
    if (
        value.strip().endswith(":")
        or value.strip() == ""
        or value.strip() == "Privacy Policy"
    ):
        return None
    return value.replace("–", "")


def cleanse_date(value: str) -> str:
    try:
        parsed = dp.parse(value)
        if parsed is None:
            return None
        return str(parsed.date())
    except Exception as e:
        return None


class Party(scrapy.Item):
    title = scrapy.Field(input_processor=MapCompose(str.strip))
    short_name = scrapy.Field(input_processor=MapCompose(clean_field, str.strip))
    eligible_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip)
    )
    registered_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip)
    )
    deregistered_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip)
    )
    leader = scrapy.Field(input_processor=MapCompose(clean_field, str.strip))
    logo = scrapy.Field(input_processor=MapCompose(resolve_relative_url))
    website = scrapy.Field()
    national_headquarters = scrapy.Field(
        input_processor=MapCompose(cleanse_address, clean_field, str.strip),
        output_processor=Join("|"),
    )
    chief_agent = scrapy.Field(
        input_processor=MapCompose(cleanse_address, clean_field, str.strip),
        output_processor=Join("|"),
    )
    auditor = scrapy.Field(
        input_processor=MapCompose(cleanse_address, clean_field, str.strip),
        output_processor=Join("|"),
    )


def clean_photo(photo: str) -> str:
    return photo[4:-1]


class LPCCandidate(scrapy.Item):
    name = scrapy.Field()
    ed_code = scrapy.Field()
    donate = scrapy.Field(input_processor=TakeFirst())
    twitter = scrapy.Field(input_processor=TakeFirst())
    facebook = scrapy.Field(input_processor=TakeFirst())
    instagram = scrapy.Field(input_processor=TakeFirst())
    website = scrapy.Field(input_processor=TakeFirst())
    photo = scrapy.Field(input_processor=MapCompose(clean_photo))
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
