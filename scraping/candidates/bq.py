import bs4
import csv
import os
import re
import scraping.helpers as sh

config = {
    "url": "http://www.blocquebecois.org/candidats/",
    "data_folder": "data",
    "source_file_prefix": "candidates_bq",
    "dest_file": "candidates_bq.csv",
}


def download(config: dict):
    source_path = os.path.join(
        config["data_folder"], "{}_1.html".format(config["source_file_prefix"])
    )
    sh.download_source(config["url"], source_path)

    src = open(source_path, encoding="utf-8")
    soup = bs4.BeautifulSoup(src.read(), "html.parser")
    src.close()
    num_candidates = int(
        soup.find("select", {"name": "full_name"})
        .find("option", {"value": ""})
        .get_text()
        .replace("Candidats (", "")[:-1]
    )
    pages = (num_candidates - 1) // 10 + 1

    for i in range(pages - 1):
        page = i + 2
        source_path = os.path.join(
            config["data_folder"],
            "{}_{}.html".format(config["source_file_prefix"], page),
        )
        url = "{}page/{}".format(config["url"], page)
        sh.download_source(url, source_path)


def get_candidate(soup: bs4.BeautifulSoup) -> dict:
    candidate = {}

    desc_soup = soup.find("div", {"class": "desc"})
    image_soup = soup.find("div", {"class": "image"})
    info_soup = soup.find("div", {"class": "infos"})
    contact_soup = info_soup.find("div", {"class": "contact"})

    name_element = info_soup.find("h2")
    riding_element = info_soup.find("h1")

    name = name_element.find("a").get_text(" ").strip()
    riding = riding_element.find("a").get_text("\n").strip()

    phone = list(contact_soup.children)[0].replace("Pour joindre l'équipe:", "").strip()
    email = contact_soup.find("a", {"class": "email-coded"})

    link_soup = contact_soup.find("span", {"class": "links"})
    website = link_soup.find("a", {"class": None})

    social_links = link_soup.find_all("a", {"class": "fb"})

    for social in social_links:
        classes = social.find("i")["class"]
        if "fa-facebook-square" in classes:
            candidate["facebook"] = social["href"]
        elif "fa-twitter-square" in classes:
            candidate["twitter"] = social["href"]
        elif "fa-instagram" in classes:
            candidate["instagram"] = social["href"]

    candidate["name"] = name
    candidate["riding"] = riding
    candidate["phone"] = phone

    if email is not None:
        candidate["email"] = email["data-mail"].replace("%", "@").replace("!", ".")

    if image_soup is not None:
        photo = image_soup.find("img")
        if photo is not None:
            candidate["photo"] = "http://www.blocquebecois.org{}".format(photo["src"])

    if desc_soup is not None:
        candidate["bio"] = desc_soup.get_text().strip()

    return candidate


def get_candidates(config: dict) -> list:
    pages = list(
        filter(
            lambda f: f.startswith(config["source_file_prefix"])
            and f.endswith(".html"),
            os.listdir(config["data_folder"]),
        )
    )
    candidates = []
    for page in pages:
        source_path = os.path.join(config["data_folder"], page)
        src = open(source_path, encoding="utf-8")

        # Fix an issue with mixed up closing tags
        html = src.read()
        issue_rx = re.compile(r"<\/h1>\s+<\/a>")
        html = re.sub(issue_rx, "</a></h1>", html)

        soup = bs4.BeautifulSoup(html, "html.parser")
        src.close()

        candidates_soup = soup.find("div", {"class": "candidates"})
        articles_soup = candidates_soup.find_all("article")

        for candidate_soup in articles_soup:
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
