import bs4
import csv
import datetime as dt
import luigi
import os
import pandas as pd
import scraping.helpers as hlp
import sqlite3

from luigi.contrib import sqla
from sqlalchemy import String, Integer, ForeignKey


class ScrapeCandidatesList(hlp.ScrapeEntities):
    src_url = luigi.Parameter(
        default="https://www.conservative.ca/team/2019-candidates/"
    )
    src_enc = luigi.Parameter(default="utf-8")
    local_html = luigi.Parameter(default="data/candidates/cpc.html")
    dst_pth = luigi.Parameter(default="data/candidates/cpc_list.csv")

    def parse_entity(self, soup: bs4.BeautifulSoup):
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
            candidate["riding"] = (
                riding.get_text()
                .replace("\u2009—\u2009", "--")
                .replace("’", "'")
                .replace("—", "-")
                .replace("–", "-")
                .strip()
            )
            if candidate["riding"] == "Vancouver--Sunshine Coast--Sea to Sky Country":
                candidate[
                    "riding"
                ] = "West Vancouver--Sunshine Coast--Sea to Sky Country"

        if more_link is not None:
            candidate["more_link"] = more_link["href"]

        return candidate

    def get_soups(self):
        with open(self.local_html, "r", encoding="utf-8") as f:
            html = f.read()
        soup = bs4.BeautifulSoup(html, "html.parser")
        entities = soup.find_all("div", {"class": "cabinet-member"})
        for entity in entities:
            yield entity


class ScrapeMoreInfo(hlp.ScrapeEntities):
    src_enc = luigi.Parameter(default="utf-8")
    key = luigi.Parameter()

    def parse_entity(self, soup: bs4.BeautifulSoup):
        candidate = {}

        parallax_soup = soup.find("section", {"class": "section--parallax-text-block"})

        candidate["key"] = self.key

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

    def get_soups(self):
        with open(self.local_html, "r", encoding="utf-8") as f:
            html = f.read()
        soup = bs4.BeautifulSoup(html, "html.parser")
        yield soup


class ScrapeCandidates(luigi.Task):
    more_folder = luigi.Parameter(default="data/candidates/cpc")
    lst_pth = luigi.Parameter(default="data/candidates/cpc_list.csv")
    mor_pth = luigi.Parameter(default="data/candidates/cpc_more.csv")
    dst_pth = luigi.Parameter(default="data/candidates/cpc.csv")

    def requires(self):
        yield ScrapeCandidatesList(dst_pth=self.lst_pth)

    def run(self):
        candidates = []
        with open(self.lst_pth, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                candidate = {
                    "name": row["name"],
                    "photo": row["photo"],
                    "riding": row["riding"],
                }

                html_file = os.path.join(
                    self.more_folder, "{}.html".format(row["name"])
                )
                dst_file = os.path.join(self.more_folder, "{}.csv".format(row["name"]))
                scraper = yield ScrapeMoreInfo(
                    src_url=row["more_link"],
                    local_html=html_file,
                    dst_pth=dst_file,
                    key=row["name"],
                )
                with open(dst_file, newline="", encoding="utf-8") as csvfile2:
                    reader2 = csv.DictReader(csvfile2)
                    for row2 in reader2:
                        candidate = {**candidate, **row2}
                        break
                candidates.append(candidate)

        headers = list(set().union(*(d.keys() for d in candidates)))
        headers.sort()
        with open(self.output().path, "w", encoding="utf-8", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, headers)
            dict_writer.writeheader()
            dict_writer.writerows(candidates)

    def output(self):
        return luigi.LocalTarget(self.dst_pth)


class LoadCandidates(sqla.CopyToTable):
    src_pth = luigi.Parameter(default="data/candidates/cpc.csv")

    reflect = True
    connection_string = "sqlite:///data/election.db"
    table = "candidates"

    def requires(self):
        return ScrapeCandidates()

    def rows(self):
        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            districts = pd.read_sql_query("SELECT * FROM electoral_districts", db)

        with open(self.src_pth, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(row["riding"])
                ed_code_df = districts.loc[
                    districts["ed_namee"].str.replace("--", "-")
                    == row["riding"].replace("--", "-")
                ]
                ed_code = int(ed_code_df['ed_code'].values[0])
                yield (
                    row["name"],
                    ed_code,
                    "Conservative Party of Canada",
                    row["nomination"],
                    row["cabinet"],
                    None,
                    row["phone"],
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
