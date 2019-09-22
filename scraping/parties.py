import bs4
import csv
import datetime as dt
import luigi
import os
import re
import scraping.helpers as hlp

from luigi.contrib import sqla
from sqlalchemy import String, Integer


COMPILED_RX = {
    "city_prov_post": re.compile(
        r"^([\w\- \.']+)(\s+|,\s*)(NL|PE|NS|NB|QC|ON|MB|SK|AB|BC|YT|NT|NU|Newfoundland and Labrador|Prince Edward Island|Nova Scotia|New Brunswick|Quebec|Ontario|Manitoba|Saskatchewan|Alberta|British Columbia|Yukon|Northwest Territories|Nunavut)\s+(\w\d\w\s?\d\w\d)$",
        re.IGNORECASE,
    ),
    "city_prov": re.compile(
        r"^([\w\- \.']+)(\s+|,\s*)(NL|PE|NS|NB|QC|ON|MB|SK|AB|BC|YT|NT|NU|Newfoundland and Labrador|Prince Edward Island|Nova Scotia|New Brunswick|Quebec|Ontario|Manitoba|Saskatchewan|Alberta|British Columbia|Yukon|Northwest Territories|Nunavut)$",
        re.IGNORECASE,
    ),
    "postal_code": re.compile(r"^(\w\d\w\s?\d\w\d)$", re.IGNORECASE),
    "po_box": re.compile(r"^P\.?O\.? Box.*$", re.IGNORECASE),
    "phone_extra": re.compile(r"[\.\-\(\) ]"),
    "website": re.compile(r"([^\s]+\.(ca|com|info|org))", re.IGNORECASE),
    "yyyymmdd_hyphen": re.compile(r"\d{4}-\d{2}-\d{2}"),
    "yyyymmdd_slash": re.compile(r"\d{4}\/\d{2}\/\d{2}"),
}


class ScrapeParties(hlp.ScrapeEntities):
    src_url = luigi.Parameter(
        default="https://www.elections.ca/content.aspx?section=pol&dir=par&document=index&lang=e"
    )
    src_enc = luigi.Parameter(default="mbcs")
    local_html = luigi.Parameter(default="data/parties.html")
    dst_pth = luigi.Parameter(default="data/parties.csv")

    def parse_entity(self, soup: bs4.BeautifulSoup):
        party = {}

        title_soup = soup.find("h3", {"class": "partytitle"})
        title_a_soup = title_soup.find("a")
        if title_a_soup is not None:
            party["title"] = title_a_soup.get_text()
        else:
            party["title"] = title_soup.get_text()

        logo_soup = soup.find("div", {"class": "logopp"})
        if logo_soup is not None:
            logo = logo_soup.find("img")["src"]
            party["logo"] = "https://www.elections.ca{}".format(logo)

        website = None

        paragraphs = soup.find_all("p")
        for p in paragraphs:

            # Replace superscript (street directions) with uppercase character
            for sup in p.findAll("sup"):
                sup.replaceWith(sup.text.upper())

            p = bs4.BeautifulSoup(str(p), "html.parser")

            text = p.get_text("\n").replace("\n\n", "\n")
            parts = list(
                filter(
                    lambda p: len(p) > 0,
                    map(lambda p: p.replace(":", "").strip(), text.split("\n")),
                )
            )
            prop = parts[0].replace(":", "")

            if prop == "Short-form Name":
                party["short"] = parts[1]
            elif prop == "Party Leader":
                party["leader"] = parts[1]
            elif prop == "National Headquarters":
                nhq_contact_info = {
                    f"nhq_{k}": v for k, v in self.parse_contact_info(parts[1:]).items()
                }
            elif prop == "Web site":
                website = self.parse_website(parts[1])
            elif prop == "Eligible":
                eligible = self.parse_date(parts[1])
                if eligible != None:
                    party["eligible"] = eligible
            elif prop == "Registered":
                registered = self.parse_date(parts[1])
                if registered != None:
                    party["registered"] = registered
            elif prop == "Deregistered":
                deregistered = self.parse_date(parts[1])
                if deregistered != None:
                    party["deregistered"] = deregistered
            elif prop == "Chief Agent":
                ca_contact_info = {
                    f"ca_{k}": v for k, v in self.parse_contact_info(parts[1:]).items()
                }
            elif prop == "Auditor":
                au_contact_info = {
                    f"au_{k}": v for k, v in self.parse_contact_info(parts[1:]).items()
                }

        party = {**party, **nhq_contact_info}
        party = {**party, **ca_contact_info}
        party = {**party, **au_contact_info}
        if "nhq_website" not in party and website is not None:
            party["nhq_website"] = website

        if "nhq_website" in party:
            party["website"] = party.pop("nhq_website")

        return party

    def get_soups(self):
        with open(self.local_html, "r", encoding="utf-8") as f:
            html = f.read()
        soup = bs4.BeautifulSoup(html, "html.parser")
        entities = soup.find_all("div", {"class": "borderbox1"})
        for entity in entities:
            yield entity

    def parse_contact_info(self, parts: list) -> dict:
        contact_info = {}

        addr_type = None
        n_addr_lines = None
        for i in range(len(parts)):
            if COMPILED_RX["city_prov_post"].match(parts[i]):
                n_addr_lines = i + 1
                addr_type = "city prov post"

        if n_addr_lines == None:
            for i in range(len(parts)):
                if COMPILED_RX["postal_code"].match(parts[i]):
                    n_addr_lines = i + 1
                    addr_type = "city prov / post"

        if addr_type == None:
            raise Exception("Unkown Address Type")

        addr_lines = parts[:n_addr_lines]

        for i, line in enumerate(addr_lines):
            contact_info["address_line_{}".format(i + 1)] = line

        if addr_type == "city prov post":
            addr_capture = re.search(COMPILED_RX["city_prov_post"], addr_lines[-1])

            contact_info["city"] = addr_capture.group(1)
            contact_info["prov"] = addr_capture.group(3)
            contact_info["postal_code"] = addr_capture.group(4).replace(" ", "")

        elif addr_type == "city prov / post":
            addr_capture = re.search(COMPILED_RX["city_prov"], addr_lines[-2])

            contact_info["city"] = addr_capture.group(1)
            contact_info["prov"] = addr_capture.group(3)
            contact_info["postal_code"] = addr_lines[-1].replace(" ", "")

        more_lines = parts[n_addr_lines:]

        for line in more_lines:
            if line.startswith("Tel"):
                phone = self.parse_phone_number(line.replace("Tel", ""))
                if phone.startswith("/Fax") or phone.startswith("Fax"):
                    phone = self.parse_phone_number(
                        phone.replace("/Fax", "").replace("Fax", "")
                    )
                    contact_info["phone"] = phone
                    contact_info["fax"] = phone
                else:
                    contact_info["phone"] = phone
            elif line.startswith("Fax"):
                contact_info["fax"] = self.parse_phone_number(line.replace("Fax", ""))
            elif COMPILED_RX["website"].match(line):
                contact_info["website"] = self.parse_website(line)
        return contact_info

    def parse_phone_number(self, raw_phone: str) -> str:
        return re.sub(COMPILED_RX["phone_extra"], "", raw_phone).strip()

    def parse_website(self, raw_site: str) -> str:
        capture = re.search(COMPILED_RX["website"], raw_site)
        website = capture.group(1)
        website = website.replace("https//", "https://").replace("http//", "http://")
        return website

    def parse_date(self, raw_date: str) -> dt.date:
        fmt = None
        if COMPILED_RX["yyyymmdd_hyphen"].match(raw_date):
            fmt = "%Y-%m-%d"
        elif COMPILED_RX["yyyymmdd_slash"].match(raw_date):
            fmt = "%Y/%m/%d"
        else:
            return None

        return dt.datetime.strptime(raw_date, fmt).date()


class LoadParties(sqla.CopyToTable):
    src_pth = luigi.Parameter(default="data/parties.csv")

    columns = [
        (["title", String(100)], {"primary_key": True}),
        (["short_name", String(64)], {}),
        (["eligible_dt", String(10)], {}),
        (["registered_dt", String(10)], {}),
        (["deregistered_dt", String(10)], {}),
        (["leader", String(64)], {}),
        (["logo", String(250)], {}),
        (["website", String(250)], {}),
        (["national_hq_address_line_1", String(100)], {}),
        (["national_hq_address_line_2", String(100)], {}),
        (["national_hq_address_line_3", String(100)], {}),
        (["national_hq_address_line_4", String(100)], {}),
        (["national_hq_city", String(64)], {}),
        (["national_hq_prov", String(2)], {}),
        (["national_hq_postal_code", String(6)], {}),
        (["national_hq_phone", String(20)], {}),
        (["national_hq_fax", String(20)], {}),
        (["chief_agent_address_line_1", String(100)], {}),
        (["chief_agent_address_line_2", String(100)], {}),
        (["chief_agent_address_line_3", String(100)], {}),
        (["chief_agent_address_line_4", String(100)], {}),
        (["chief_agent_address_line_5", String(100)], {}),
        (["chief_agent_city", String(64)], {}),
        (["chief_agent_prov", String(2)], {}),
        (["chief_agent_postal_code", String(6)], {}),
        (["chief_agent_phone", String(20)], {}),
        (["chief_agent_fax", String(20)], {}),
        (["auditor_address_line_1", String(100)], {}),
        (["auditor_address_line_2", String(100)], {}),
        (["auditor_address_line_3", String(100)], {}),
        (["auditor_address_line_4", String(100)], {}),
        (["auditor_city", String(64)], {}),
        (["auditor_prov", String(2)], {}),
        (["auditor_postal_code", String(6)], {}),
        (["auditor_phone", String(20)], {}),
        (["auditor_fax", String(20)], {}),
    ]
    connection_string = "sqlite:///data/election.db"
    table = "parties"

    def requires(self):
        return ScrapeParties()

    def rows(self):
        with open(self.src_pth, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield (
                    row["title"],
                    row["short"],
                    row["eligible"],
                    row["registered"],
                    row["deregistered"],
                    row["leader"],
                    row["logo"],
                    row["website"],
                    row["nhq_address_line_1"],
                    row["nhq_address_line_2"],
                    row["nhq_address_line_3"],
                    row["nhq_address_line_4"],
                    row["nhq_city"],
                    row["nhq_prov"],
                    row["nhq_postal_code"],
                    row["nhq_phone"],
                    row["nhq_fax"],
                    row["ca_address_line_1"],
                    row["ca_address_line_2"],
                    row["ca_address_line_3"],
                    row["ca_address_line_4"],
                    row["ca_address_line_5"],
                    row["ca_city"],
                    row["ca_prov"],
                    row["ca_postal_code"],
                    row["ca_phone"],
                    row["ca_fax"],
                    row["au_address_line_1"],
                    row["au_address_line_2"],
                    row["au_address_line_3"],
                    row["au_address_line_4"],
                    row["au_city"],
                    row["au_prov"],
                    row["au_postal_code"],
                    row["au_phone"],
                    row["au_fax"],
                )


if __name__ == "__main__":
    # luigi.build([ScrapeParties()], local_scheduler=True)
    luigi.build([LoadParties()], local_scheduler=True)
