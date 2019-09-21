import bs4
import csv
import datetime as dt
import os
import scraping.helpers as sh
import time

config = {
    "data_folder": "data",
    "html_subfolder": "candidates_cpc_more",
    "candidates_file": "candidates_cpc.csv",
    "more_file": "candidates_cpc_more.csv",
}


def download(config):
    candidates_file = os.path.join(config["data_folder"], config["candidates_file"])
    html_folder = os.path.join(config["data_folder"], config["html_subfolder"])

    if not os.path.exists(html_folder):
        os.makedirs(html_folder)

    with open(candidates_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            html_file = os.path.join(html_folder, "{}.html".format(row["name"]))
            if not os.path.exists(html_file):
                sh.download_source(row["more_link"], html_file)
                time.sleep(5)  # Don't spam request


def get_candidate_soup(config: dict, name: str) -> bs4.BeautifulSoup:
    source_path = os.path.join(
        config["data_folder"], config["html_subfolder"], "{}.html".format(name)
    )
    src = open(source_path, encoding="utf-8")
    soup = bs4.BeautifulSoup(src.read(), "html.parser")
    src.close()
    return soup


def get_candidate(soup: bs4.BeautifulSoup) -> dict:
    candidate = {}

    parallax_soup = soup.find("section", {"class": "section--parallax-text-block"})

    name = parallax_soup.find("h1")
    cabinet = parallax_soup.find("p")
    nomination = parallax_soup.find("p", {"class": "nomination_date"})

    text_soup = soup.find("section", {"class": "section--text-block"})
    text_cell_soup = text_soup.find("div", {"class": "cell"})

    social_soup = soup.find("section", {"class": "section--social-share"})

    facebook = social_soup.find("a", {"data-type": "facebook"})
    twitter = social_soup.find("a", {"data-type": "twitter"})
    website = social_soup.find("a", {"data-type": "website"})
    donate = social_soup.find("a", {"data-type": "donate"})
    instagram = social_soup.find("a", {"data-type": "instagram"})

    phone_soup = social_soup.find("div", {"class": "team-member-phone"})

    if name is not None:
        candidate["name"] = name.get_text()

    if cabinet is not None:
        candidate["cabinet"] = cabinet.get_text()

    if nomination is not None:
        candidate["nomination"] = dt.datetime.strptime(
            nomination.get_text().replace("Nomination Date: ", ""), "%d/%m/%Y"
        ).date()

    if text_cell_soup is not None:
        bio = text_cell_soup.find("div")
        if bio is not None:
            candidate["bio"] = bio.get_text()

    if facebook is not None:
        candidate["facebook"] = facebook["href"]
    if twitter is not None:
        candidate["twitter"] = twitter["href"]
    if website is not None:
        candidate["website"] = website["href"]
    if donate is not None:
        candidate["donate"] = donate["href"]
    if instagram is not None:
        candidate["instagram"] = instagram["href"]

    if phone_soup is not None:
        phone = phone_soup.find("a")["href"].replace("tel:", "")
        if len(phone) > 0:
            candidate["phone"] = phone

    return candidate


def get_candidates(config: dict) -> list:
    candidates = []

    candidates_file = os.path.join(config["data_folder"], config["candidates_file"])
    with open(candidates_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            soup = get_candidate_soup(config, row["name"])
            candidate = get_candidate(soup)
            candidates.append(candidate)
    return candidates


def save_candidates(config: dict, candidates: list):
    dest_path = os.path.join(config["data_folder"], config["more_file"])

    keys = set().union(*(d.keys() for d in candidates))
    with open(dest_path, "w", encoding="utf8", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(candidates)


def process(config: dict):
    download(config)
    candidates = get_candidates(config)
    save_candidates(config, candidates)


if __name__ == "__main__":
    process(config)
