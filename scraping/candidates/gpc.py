import bs4
import csv
import os
import scraping.helpers as sh

config = {
    "url": "https://www.greenparty.ca/en/candidates",
    "data_folder": "data",
    "source_file": "candidates_gpc.html",
    "dest_file": "candidates_gpc.csv",
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

    candidate_soup = soup.find("div", {"class": "candidate"})
    modal_soup = soup.find("div", {"class": "candidate-modal"})

    name = candidate_soup.find("div", {"class": "candidate-name"})
    ed_code = candidate_soup.find("div", {"class": "well"})["data-target"]

    photo_data = candidate_soup.find("div", {"class": "candidate-image"})["data-src"]
    photo = bs4.BeautifulSoup(photo_data, "lxml").find("img")["src"]

    bio = modal_soup.find("div", {"class": "modal-bio"})

    contact_soup = modal_soup.find("div", {"class": "candidate-contact"})
    contact_links = contact_soup.find_all("a")

    facebook = None
    instagram = None
    twitter = None
    email = None

    for link in contact_links:
        if link.has_attr("title"):
            if link["title"] == "Facebook":
                facebook = link["href"]
            elif link["title"] == "Instagram":
                instagram = link["href"]
            elif link["title"] == "Twitter":
                twitter = link["href"]
        else:
            email = link["href"].replace("mailto:", "")
    phone = contact_soup.find("i", {"class": "fa-phone"})

    donate = modal_soup.find("a", {"class": "btn-donate"})

    candidate["name"] = name.get_text()
    candidate["ed_code"] = int(ed_code.replace("#rid-", ""))

    if photo is not None:
        candidate["photo"] = photo

    if bio is not None:
        candidate["bio"] = bio.get_text().strip().replace("\n\n", "\n")

    if email is not None:
        candidate["email"] = email

    if facebook is not None:
        candidate["facebook"] = facebook

    if instagram is not None:
        candidate["instagram"] = instagram

    if twitter is not None:
        candidate["twitter"] = twitter

    if donate is not None:
        candidate["donate"] = "https://www.greenparty.ca{}".format(donate["href"])

    if phone is not None:
        candidate["phone"] = phone.next_sibling.strip()

    return candidate


def get_candidates(config: dict) -> list:
    soup = get_soup(config)
    candidates_soup = soup.find_all("div", {"class": "candidate-card"})
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
    # download(config)

    candidates = get_candidates(config)
    save_candidates(config, candidates)


if __name__ == "__main__":
    process(config)

