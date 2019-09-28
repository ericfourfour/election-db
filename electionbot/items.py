import dateparser as dp
import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from w3lib.html import remove_tags


def clean_phone(value: str) -> str:
    return (
        value.replace(".", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "")
        .replace("x", "X")
        .strip()
    )


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


def clean_lpc_photo(photo: str) -> str:
    return photo[4:-1]


class LPCCandidate(scrapy.Item):
    name = scrapy.Field()
    ed_code = scrapy.Field()
    donate = scrapy.Field(input_processor=TakeFirst())
    twitter = scrapy.Field(input_processor=TakeFirst())
    facebook = scrapy.Field(input_processor=TakeFirst())
    instagram = scrapy.Field(input_processor=TakeFirst())
    website = scrapy.Field(input_processor=TakeFirst())
    photo = scrapy.Field(input_processor=MapCompose(clean_lpc_photo))
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))


def clean_cpc_nomination_dt(value: str) -> str:
    return value.replace("Nomination Date: ", "")


def clean_cpc_riding(value: str) -> str:
    value = (
        value.replace("\u2009", "")
        .replace("’", "'")
        .replace("—", "-")
        .replace("–", "-")
        .strip()
    )
    if value == "Vancouver--Sunshine Coast--Sea to Sky Country":
        value = "West Vancouver--Sunshine Coast--Sea to Sky Country"
    return value


class CPCCandidate(scrapy.Item):
    name = scrapy.Field()
    riding = scrapy.Field(input_processor=MapCompose(clean_cpc_riding))
    nomination_dt = scrapy.Field(input_processor=MapCompose(clean_cpc_nomination_dt))
    cabinet_position = scrapy.Field()
    photo = scrapy.Field()
    donate = scrapy.Field()
    website = scrapy.Field()
    facebook = scrapy.Field()
    instagram = scrapy.Field()
    twitter = scrapy.Field()
    phone = scrapy.Field()
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))


def parse_bq_name(value: str) -> str:
    return value.split(":")[1].strip()


def parse_bq_riding(value: str) -> str:
    return value.split(":")[0].strip()


def prepend_bq_photo(value: str) -> str:
    return f"http://www.blocquebecois.org/candidats{value}"


def parse_bq_phone(value: str) -> str:
    return value.replace("Pour joindre l'\u00e9quipe:", "").strip()


def parse_bq_email(value: str) -> str:
    return value.replace("%", "@").replace("!", ".")


class BQCandidate(scrapy.Item):
    name = scrapy.Field(
        input_processor=TakeFirst(), output_processor=MapCompose(parse_bq_name)
    )
    riding = scrapy.Field(
        input_processor=TakeFirst(), output_processor=MapCompose(parse_bq_riding)
    )
    photo = scrapy.Field(input_processor=MapCompose(prepend_bq_photo))
    website = scrapy.Field()
    facebook = scrapy.Field()
    instagram = scrapy.Field()
    twitter = scrapy.Field()
    phone = scrapy.Field(
        input_processor=MapCompose(strip_remove_blanks, parse_bq_phone, clean_phone)
    )
    email = scrapy.Field(input_processor=MapCompose(parse_bq_email))
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
