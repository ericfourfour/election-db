import bs4
import csv
import datetime as dt
import os
import re
import scraping.helpers as sh

config = {
    "url": "https://www.elections.ca/content.aspx?section=pol&dir=par&document=index&lang=e",
    "data_folder": "data",
    "source_file": "parties.html",
    "dest_file": "parties.csv",
}


def download(config: dict):
    source_path = os.path.join(config["data_folder"], config["source_file"])
    sh.download_source(config["url"], source_path)


def get_soup(config: dict) -> bs4.BeautifulSoup:
    source_path = os.path.join(config["data_folder"], config["source_file"])
    src = open(source_path, encoding="ansi")
    soup = bs4.BeautifulSoup(src.read(), "html.parser")
    src.close()
    return soup


# download(config)

soup = get_soup(config)
parties_soup = soup.find_all("div", {"class": "borderbox1"})


def get_party(soup: bs4.BeautifulSoup) -> dict:
    party = {}

    title = None
    logo = None

    title_soup = soup.find("h3", {"class": "partytitle"})
    title_a_soup = title_soup.find("a")
    if title_a_soup is not None:
        title = title_a_soup.get_text()
    else:
        title = title_soup.get_text()

    logo_soup = soup.find("div", {"class": "logopp"})
    if logo_soup is not None:
        logo = logo_soup.find("img")["src"]

    nhq_street_address = None
    nhq_unit = None
    nhq_city = None
    nhq_prov = None
    nhq_postal_code = None

    registered = None
    deregistered = None

    ca_unit = None
    au_unit = None

    nhq_phone = None
    ca_phone = None
    au_phone = None

    nhq_fax = None
    ca_fax = None
    au_fax = None

    website = None

    dt_rx1 = re.compile(r"\d{4}-\d{2}-\d{2}")
    dt_rx2 = re.compile(r"\d{4}\/\d{2}\/\d{2}")

    paragraphs = soup.find_all("p")
    for p in paragraphs:
        text = p.get_text("\n").replace("\n\n", "\n")
        parts = list(
            filter(
                lambda p: len(p) > 0,
                map(lambda p: p.replace(":", "").strip(), text.split("\n")),
            )
        )
        prop = parts[0].replace(":", "")

        if prop == "Short-form Name":
            short_name = parts[1]
        elif prop == "Party Leader":
            leader = parts[1]
        elif prop == "National Headquarters":
            if len(parts) > 3:
                addr_lines = 2 if parts[3].startswith("Tel") else 3
            else:
                addr_lines = 2

            if addr_lines == 3:
                if parts[1].replace(".", "").startswith("PO Box"):
                    nhw_street_address = parts[2]
                    nhq_unit = parts[1]
                else:
                    nhq_street_address = parts[1]
                    nhq_unit = parts[2]
            else:
                nhq_street_address = parts[1]

            nhq_city = parts[addr_lines][:-11].strip()
            nhq_prov = parts[addr_lines][-11:-8].strip()
            nhq_postal_code = parts[addr_lines][-7:].replace(" ", "")

            if len(parts) - 1 >= addr_lines + 1:
                nhq_phone = parts[addr_lines + 1].replace("Tel.", "").strip()
            if len(parts) - 1 >= addr_lines + 2:
                nhq_fax = parts[addr_lines + 2].replace("Fax", "").strip()
            website_soup = p.find("a")
            if website_soup is not None:
                website = website_soup["href"]
        elif prop == "Eligible":
            if dt_rx1.match(parts[1]):
                eligible = dt.datetime.strptime(parts[1], "%Y-%m-%d").date()
            elif dt_rx2.match(parts[1]):
                eligible = dt.datetime.strptime(parts[1], "%Y/%m/%d").date()
        elif prop == "Registered":
            if dt_rx1.match(parts[1]):
                registered = dt.datetime.strptime(parts[1], "%Y-%m-%d").date()
            elif dt_rx2.match(parts[1]):
                registered = dt.datetime.strptime(parts[1], "%Y/%m/%d").date()
        elif prop == "Deregistered":
            if dt_rx1.match(parts[1]):
                deregistered = dt.datetime.strptime(parts[1], "%Y-%m-%d").date()
            elif dt_rx2.match(parts[1]):
                deregistered = dt.datetime.strptime(parts[1], "%Y/%m/%d").date()
        elif prop == "Chief Agent":
            addr_lines = 2 if parts[4].startswith("Tel") else 3

            ca_name = parts[1]

            ca_street_address = parts[2]
            if addr_lines == 3:
                ca_unit = parts[3]

            ca_city = parts[addr_lines + 1][:-11].strip()
            ca_prov = parts[addr_lines + 1][-11:-8].strip()
            ca_postal_code = parts[addr_lines + 1][-7:].replace(" ", "").strip()

            if len(parts) - 1 >= addr_lines + 2:
                ca_phone = parts[addr_lines + 2].replace("Tel.", "").strip()
            if len(parts) - 1 >= addr_lines + 3:
                ca_fax = parts[addr_lines + 3].replace("Fax", "").strip()
        elif prop == "Auditor":
            addr_lines = 2 if parts[4].startswith("Tel") else 3

            au_name = parts[1]
            au_street_address = parts[2]
            if addr_lines == 3:
                au_unit = parts[3]

            au_city = parts[addr_lines + 1][:-11].strip()
            au_prov = parts[addr_lines + 1][-11:-8].strip()
            au_postal_code = parts[addr_lines + 1][-7:].replace(" ", "")

            if len(parts) - 1 >= addr_lines + 2:
                au_phone = parts[addr_lines + 2].replace("Tel.", "").strip()
            if len(parts) - 1 >= addr_lines + 3:
                au_fax = parts[addr_lines + 3].replace("Fax", "").strip()

    party["title"] = title
    party["logo"] = "https://www.elections.ca{}".format(logo)
    party["short"] = short_name
    party["leader"] = leader
    party["nhq_street_address"] = nhq_street_address
    party["nhq_unit"] = nhq_unit
    party["nhq_city"] = nhq_city
    party["nhq_prov"] = nhq_prov
    party["nhq_postal_code"] = nhq_postal_code
    party["nhq_phone"] = nhq_phone
    party["nhq_fax"] = nhq_fax
    party["website"] = website
    party["eligible"] = eligible
    party["registered"] = registered
    party["deregistered"] = deregistered
    party["ca_name"] = ca_name
    party["ca_street_address"] = ca_street_address
    party["ca_unit"] = ca_unit
    party["ca_city"] = ca_city
    party["ca_prov"] = ca_prov
    party["ca_postal_code"] = ca_postal_code
    party["ca_phone"] = ca_phone
    party["ca_fax"] = ca_fax
    party["au_name"] = au_name
    party["au_street_address"] = au_street_address
    party["au_unit"] = au_unit
    party["au_city"] = au_city
    party["au_prov"] = au_prov
    party["au_postal_code"] = au_postal_code
    party["au_phone"] = au_phone
    party["au_fax"] = au_fax

    return party


def save_parties(config: dict, parties: list):
    dest_path = os.path.join(config["data_folder"], config["dest_file"])

    keys = set().union(*(d.keys() for d in parties))
    with open(dest_path, "w", encoding="utf8", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(parties)


parties = []
for party_soup in parties_soup:
    party = get_party(party_soup)
    parties.append(party)

save_parties(config, parties)

