import bs4
import csv
import os
import scraping.helpers as sh

config = {
    "url": "https://www.conservative.ca/team/2019-candidates/",
    "data_folder": "data",
    "source_file": "candidates_cpc.html",
    "candidates_file": "candidates_cpc.csv",
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


def get_candidate(soup: bs4.BeautifulSoup) -> dict:
    candidate = {}

    name = soup.find("h3")
    photo = soup.find("img")
    riding = soup.find("p", {"class": "riding-title"})
    more_link = soup.find("a", {"class": "button"})

    if name is not None:
        candidate["name"] = name.get_text()

    if photo is not None:
        candidate["photo"] = photo["src"]

    if riding is not None:
        candidate["riding"] = sh.clean(riding.get_text())

    if more_link is not None:
        candidate["more_link"] = more_link["href"]

    return candidate


def get_candidates(soup: bs4.BeautifulSoup) -> list:
    candidates_soup = soup.find_all("div", {"class": "cabinet-member"})
    candidates = []
    for candidate_soup in candidates_soup:
        candidates.append(get_candidate(candidate_soup))
    return candidates


def save_candidates(config: dict, candidates: list):
    dest_path = os.path.join(config["data_folder"], config["candidates_file"])

    keys = set().union(*(d.keys() for d in candidates))
    with open(dest_path, "w", encoding="utf8", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(candidates)


def process(config):
    download(config)

    soup = get_soup(config)
    candidates = get_candidates(soup)
    save_candidates(config, candidates)


if __name__ == "__main__":
    process(config)
