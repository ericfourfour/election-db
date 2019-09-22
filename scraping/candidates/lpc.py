import bs4
import csv
import luigi
import scraping.helpers as hlp

from luigi.contrib import sqla
from sqlalchemy import String, Integer, ForeignKey


class ScrapeCandidates(hlp.ScrapeEntities):
    src_url = luigi.Parameter(
        default="https://www.liberal.ca/team-trudeau-2019-candidates/"
    )
    src_enc = luigi.Parameter(default="utf-8")
    local_html = luigi.Parameter(default="data/candidates/lpc.html")
    dst_pth = luigi.Parameter(default="data/candidates/lpc.csv")

    def parse_entity(self, soup: bs4.BeautifulSoup):
        candidate = {}

        name = soup.find("h2", {"class": "name"})
        ed_code = soup.find("div", {"class": "candidate-riding-id"})
        twitter = soup.find("a", {"class": "candidate-social-link--twitter"})
        facebook = soup.find("a", {"class": "candidate-social-link--facebook"})
        instagram = soup.find("a", {"class": "candidate-social-link--instagram"})
        website = soup.find("a", {"class": "candidate-social-link--website"})
        donate = soup.find("a", text="Donate")
        bio = soup.find("div", {"class": "candidate-modal-bio"})

        candidate["name"] = name.get_text()
        candidate["ed_code"] = int(ed_code.get_text())

        if donate is not None:
            candidate["donate"] = donate["href"]

        if twitter is not None:
            candidate["twitter"] = twitter["href"]

        if facebook is not None:
            candidate["facebook"] = facebook["href"]

        if instagram is not None:
            candidate["instagram"] = instagram["href"]

        if website is not None:
            candidate["website"] = website["href"]

        if soup.has_attr("data-photo-url"):
            photo_url = soup["data-photo-url"][4:-1]
            if "placeholder" not in photo_url:
                candidate["photo"] = photo_url

        if bio is not None:
            bio_text = bio.get_text().strip()
            if len(bio_text) > 0:
                candidate["bio"] = bio_text
        return candidate

    def get_soups(self):
        with open(self.local_html, "r", encoding="utf-8") as f:
            html = f.read()
        soup = bs4.BeautifulSoup(html, "html.parser")
        entities = soup.find("div", {"id": "candidates-results-area"}).findAll(
            "li", {"class": "candidate-card"}
        )
        for entity in entities:
            yield entity


class LoadCandidates(sqla.CopyToTable):
    src_pth = luigi.Parameter(default="data/candidates/lpc.csv")

    reflect = True
    connection_string = "sqlite:///data/election.db"
    table = "candidates"

    def requires(self):
        return ScrapeCandidates()

    def rows(self):
        with open(self.src_pth, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield (
                    row["name"],
                    row["ed_code"],
                    "Liberal Party of Canada",
                    None,
                    None,
                    None,
                    None,
                    row["photo"],
                    row["donate"],
                    row["website"],
                    row["facebook"],
                    row["instagram"],
                    row["twitter"],
                    row["bio"],
                )


if __name__ == "__main__":
    # luigi.build([ScrapeCandidates()], local_scheduler=True)
    luigi.build([LoadCandidates()], local_scheduler=True)
