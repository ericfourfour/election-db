import csv
import os

from bs4 import BeautifulSoup
from contextlib import closing
from requests import get
from requests.exceptions import RequestException


def simple_get(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error("Error during requests to {0} : {1}".format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers["Content-Type"].lower()
    return (
        resp.status_code == 200
        and content_type is not None
        and content_type.find("html") > -1
    )


def log_error(e):
    """
    It is always a good idea to log errors. 
    This function just prints them, but you can
    make it do anything.
    """
    print(e)


config = {"index": "https://www.liberal.ca/team-trudeau-2019-candidates/", 'folder': 'data', 'filename': 'candidates_lpc.csv'}

raw_html = simple_get(config["index"])

raw_html
html = BeautifulSoup(raw_html, "html")
candidates_area = html.find("div", {"id": "candidates-results-area"})
candidates_area

candidate_cards = html.findAll("li", {"class": "candidate-card"})
candidate_cards[0].find('a', text='Donate')

candidates = []
for card in candidate_cards:
    candidate = {}

    name = card.find("h2", {"class": "name"})
    ed_code = card.find("div", {"class": "candidate-riding-id"})
    twitter = card.find("a", {"class": "candidate-social-link--twitter"})
    facebook = card.find("a", {"class": "candidate-social-link--facebook"})
    instagram = card.find("a", {"class": "candidate-social-link--instagram"})
    website = card.find("a", {"class": "candidate-social-link--website"})
    donate = card.find('a', text='Donate')
    bio = card.find('div', {'class': "candidate-modal-bio"})

    candidate["name"] = name.get_text()
    candidate["ed_code"] = int(ed_code.get_text())

    if donate is not None:
        candidate['donate'] = donate['href']

    if twitter is not None:
        candidate["twitter"] = twitter["href"]

    if facebook is not None:
        candidate["facebook"] = facebook["href"]

    if instagram is not None:
        candidate["instagram"] = instagram["href"]

    if website is not None:
        candidate["website"] = website["href"]

    if card.has_attr("data-photo-url"):
        photo_url = card["data-photo-url"][4:-1]
        if "placeholder" not in photo_url:
            candidate["photo"] = photo_url

    if bio is not None:
        bio_text = bio.get_text().strip()
        if len(bio_text) > 0:
            candidate['bio'] = bio_text

    candidates.append(candidate)

path = os.path.join(config['folder'], config['filename'])

keys = ['name', 'ed_code', 'donate', 'twitter', 'facebook', 'instagram', 'website', 'photo', 'bio']
with open(path, 'w', encoding='utf8', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(candidates)
