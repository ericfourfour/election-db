import bs4
import csv
import os
import scraping.helpers as sh

config = {
    "url": "https://www.peoplespartyofcanada.ca/our_candidates",
    "data_folder": "data",
    "source_file": "candidates_ppc.html",
    "dest_file": "candidates_ppc.csv",
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

    cells = soup.find_all("td")
    riding = cells[0].get_text()
    name = cells[1].get_text()

    twitter = cells[2].find("a")
    facebook = cells[3].find("a")
    website = cells[4].find("a")

    candidate["name"] = name
    candidate["riding"] = riding

    if twitter is not None:
        candidate["twitter"] = twitter["href"]

    if facebook is not None:
        candidate["facebook"] = facebook["href"]

    if website is not None:
        candidate["website"] = website["href"]

    return candidate


def get_candidates(config: dict) -> list:
    soup = get_soup(config)
    candidates_soup = soup.find("table", {"class": "candidatetable"}).find_all("tr")
    candidates = []
    for candidate_soup in candidates_soup:
        if len(candidate_soup.find_all("td")[1].get_text().strip()) > 0:
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
    # download(config)

    candidates = get_candidates(config)
    save_candidates(config, candidates)


if __name__ == "__main__":
    process(config)
