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


def clean_mailto(value: str) -> str:
    return value.replace("mailto:", "")


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
    title = scrapy.Field(
        input_processor=MapCompose(str.strip), output_processor=TakeFirst()
    )
    short_name = scrapy.Field(
        input_processor=MapCompose(clean_field, str.strip), output_processor=TakeFirst()
    )
    eligible_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip),
        output_processor=TakeFirst(),
    )
    registered_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip),
        output_processor=TakeFirst(),
    )
    deregistered_dt = scrapy.Field(
        input_processor=MapCompose(clean_field, cleanse_date, str.strip),
        output_processor=TakeFirst(),
    )
    leader = scrapy.Field(
        input_processor=MapCompose(clean_field, str.strip), output_processor=TakeFirst()
    )
    logo = scrapy.Field(
        input_processor=MapCompose(resolve_relative_url), output_processor=TakeFirst()
    )
    website = scrapy.Field(output_processor=TakeFirst())
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
    name = scrapy.Field(output_processor=TakeFirst())
    ed_code = scrapy.Field(output_processor=TakeFirst())
    donate = scrapy.Field(output_processor=TakeFirst())
    twitter = scrapy.Field(output_processor=TakeFirst())
    facebook = scrapy.Field(output_processor=TakeFirst())
    instagram = scrapy.Field(output_processor=TakeFirst())
    website = scrapy.Field(output_processor=TakeFirst())
    photo = scrapy.Field(
        input_processor=MapCompose(clean_lpc_photo), output_processor=TakeFirst()
    )
    bio = scrapy.Field(
        input_processor=MapCompose(strip_remove_blanks), output_processor=Join("\n")
    )


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
    if value == "Vancouver-Sunshine Coast-Sea to Sky Country":
        value = "West Vancouver-Sunshine Coast-Sea to Sky Country"
    if value == "Jonquière-Haut-Saguenay":
        value = "Jonquière"
    return value


class CPCCandidate(scrapy.Item):
    name = scrapy.Field(output_processor=TakeFirst())
    riding = scrapy.Field(
        input_processor=MapCompose(clean_cpc_riding), output_processor=TakeFirst()
    )
    nomination_dt = scrapy.Field(
        input_processor=MapCompose(clean_cpc_nomination_dt),
        output_processor=TakeFirst(),
    )
    cabinet_position = scrapy.Field(
        input_processor=MapCompose(strip_remove_blanks), output_processor=TakeFirst()
    )
    photo = scrapy.Field(output_processor=TakeFirst())
    donate = scrapy.Field(output_processor=TakeFirst())
    website = scrapy.Field(output_processor=TakeFirst())
    facebook = scrapy.Field(output_processor=TakeFirst())
    instagram = scrapy.Field(output_processor=TakeFirst())
    twitter = scrapy.Field(output_processor=TakeFirst())
    phone = scrapy.Field(output_processor=TakeFirst())
    bio = scrapy.Field(
        input_processor=MapCompose(strip_remove_blanks), output_processor=Join("\n")
    )


def parse_bq_name(value: str) -> str:
    return value.split(":")[1].strip()


def parse_bq_riding(value: str) -> str:
    return value.split(":")[0].strip()


def prepend_bq_photo(value: str) -> str:
    return f"http://www.blocquebecois.org{value}"


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


def clean_ndp_name(value: str) -> str:
    return value.strip().replace("  ", " ")


def prepend_ndp_link(value: str) -> str:
    return f"https://www.ndp.ca{value}"


class NDPCandidate(scrapy.Item):
    name = scrapy.Field(input_processor=MapCompose(clean_ndp_name))
    ed_code = scrapy.Field()
    cabinet_position = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
    photo = scrapy.Field()
    donate = scrapy.Field(input_processor=MapCompose(prepend_ndp_link))
    lawnsign = scrapy.Field(input_processor=MapCompose(prepend_ndp_link))
    volunteer = scrapy.Field()
    website = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
    facebook = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
    instagram = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
    twitter = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))


class PPCCandidate(scrapy.Item):
    name = scrapy.Field()
    riding = scrapy.Field()
    website = scrapy.Field()
    facebook = scrapy.Field()
    twitter = scrapy.Field()


def clean_gpc_ed_code(value: str) -> str:
    return value.replace("#rid-", "")


def prepend_gpc_link(value: str) -> str:
    return f"https://www.greenparty.ca{value}"


def clean_gpc_email(value: str) -> str:
    if not "@" in value:
        return None
    return value.strip()


class GPCCandidate(scrapy.Item):
    name = scrapy.Field()
    ed_code = scrapy.Field(input_processor=MapCompose(clean_gpc_ed_code))
    photo = scrapy.Field()
    volunteer = scrapy.Field(input_processor=MapCompose(prepend_gpc_link))
    donate = scrapy.Field(input_processor=MapCompose(prepend_gpc_link))
    facebook = scrapy.Field()
    instagram = scrapy.Field()
    twitter = scrapy.Field()
    email = scrapy.Field(input_processor=MapCompose(clean_mailto))
    phone = scrapy.Field(input_processor=MapCompose(clean_phone))
    bio = scrapy.Field(input_processor=MapCompose(strip_remove_blanks))
