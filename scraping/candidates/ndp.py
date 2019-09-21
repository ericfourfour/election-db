"""
https://www.ndp.ca/candidates
"""

import bs4
import csv
import os
import scraping.helpers as sh

config = {
    "url": "https://www.ndp.ca/candidates",
    "data_folder": "data",
    "source_file": "candidates_ndp.html",
    "dest_file": "candidates_ndp.csv",
}


def download(config: dict):
    source_path = os.path.join(config["data_folder"], config["source_file"])
    sh.download_source(config["url"], source_path)


def get_soup(config: dict) -> bs4.BeautifulSoup:
    source_path = os.path.join(config["data_folder"], config["source_file"])
    src = open(source_path, encoding="utf-8")
    soup = bs4.BeautifulSoup(src.read(), "html.parser")
    src.close()
    return soup


"""
<div class="civic-data" data-fullname="Anna Betty  Achneepineskum" data-firstname="Anna Betty" data-lastname="Achneepineskum" data-cabinet-position=""
data-riding-code="35106" data-riding-name="Thunder Bay&mdash;Superior North" data-tag-line="Thunder Bay&mdash;Superior North"
data-donate-link="/donate/35106?source=2019_35106_DON_EN" data-volunteer-link="https://www.ndp.ca/volunteer?source=2019_35106_VOL_EN"
data-lawnsign-link="/get-your-sign?source=2019_35106_SIGN_EN" data-website-link="https://annabettyachneepineskum.ndp.ca" data-twitter-link=""
data-facebook-link="https://www.facebook.com/betty.achneepineskum" data-instagram-link="" data-bio=""></div>
"""


def get_candidate(soup: bs4.BeautifulSoup) -> dict:
    candidate = {}

    data_soup = soup.find("div", {"class": "civic-data"})

    name = data_soup["data-fullname"]
    cabinet = data_soup["data-cabinet-position"]
    ed_code = int(data_soup["data-riding-code"])
    donate = "https://www.ndp.ca{}".format(data_soup["data-donate-link"])
    volunteer = data_soup["data-volunteer-link"]
    lawnsign = "https://www.ndp.ca{}".format(data_soup["data-lawnsign-link"])
    website = data_soup["data-website-link"]
    twitter = data_soup["data-twitter-link"]
    facebook = data_soup["data-facebook-link"]
    instagram = data_soup["data-instagram-link"]
    bio = data_soup["data-bio"]

    candidate["name"] = name
    candidate["ed_code"] = ed_code

    if len(cabinet) > 0:
        candidate["cabinet"] = cabinet
    if len(donate) > 0:
        candidate["donate"] = donate
    if len(volunteer) > 0:
        candidate["volunteer"] = volunteer
    if len(lawnsign) > 0:
        candidate["lawnsign"] = lawnsign
    if len(website) > 0:
        candidate["website"] = website
    if len(twitter) > 0:
        candidate["twitter"] = twitter
    if len(facebook) > 0:
        candidate["facebook"] = facebook
    if len(instagram) > 0:
        candidate["instagram"] = instagram
    if len(bio) > 0:
        candidate["bio"] = bio

    return candidate


def get_candidates(config: dict) -> list:
    soup = get_soup(config)
    candidates_soup = soup.find_all("div", {"class": "campaign-civics-list-item"})
    candidates = []
    for candidate_soup in candidates_soup:
        candidates.append(get_candidate(candidate_soup))
    return candidates


def save_candidates(config: dict, candidates: list):
    dest_path = os.path.join(config["data_folder"], config["dest_file"])

    keys = set().union(*(d.keys() for d in candidates))
    with open(dest_path, "w", encoding="utf8", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(candidates)


def process(config):
    download(config)

    candidates = get_candidates(config)
    save_candidates(config, candidates)


if __name__ == "__main__":
    process(config)

