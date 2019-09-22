import bs4
import csv
import codecs
import luigi
import os
import pathlib as pl
import shutil
import urllib.request

from pathlib import Path


class DownloadTask(luigi.Task):
    src_url = luigi.Parameter()
    dst_pth = luigi.Parameter()
    user_agent = luigi.Parameter(default="Mozilla/5.0")

    def run(self):
        mk_parent_dirs(self.dst_pth)
        req = urllib.request.Request(
            self.src_url, headers={"User-Agent": self.user_agent}
        )
        with urllib.request.urlopen(req) as response:
            with open(self.output().path, "wb") as out_file:
                shutil.copyfileobj(response, out_file)

    def output(self):
        return luigi.LocalTarget(self.dst_pth)


class EncodeFileTask(luigi.Task):
    src_pth = luigi.Parameter()
    dst_pth = luigi.Parameter()
    src_enc = luigi.Parameter()
    dst_enc = luigi.Parameter()
    block_size = luigi.IntParameter(default=1048576)

    def run(self):
        mk_parent_dirs(self.dst_pth)
        with codecs.open(self.src_pth, "r", self.src_enc) as src:
            with codecs.open(self.dst_pth, "w", self.dst_enc) as dst:
                while True:
                    contents = src.read(self.block_size)
                    if not contents:
                        break
                    dst.write(contents)

    def output(self):
        return luigi.LocalTarget(self.dst_pth)


class DownloadEncodeTask(luigi.Task):
    src_url = luigi.Parameter()
    dst_pth = luigi.Parameter()
    user_agent = luigi.Parameter(default="Mozilla/5.0")

    src_enc = luigi.Parameter()
    dst_enc = luigi.Parameter()
    block_size = luigi.IntParameter(default=1048576)

    @property
    def tmp_pth(self):
        return "{}.tmp".format(self.dst_pth)

    def requires(self):
        return DownloadTask(
            src_url=self.src_url, dst_pth=self.tmp_pth, user_agent=self.user_agent
        )

    def run(self):
        mk_parent_dirs(self.dst_pth)
        EncodeFileTask(
            src_pth=self.tmp_pth,
            dst_pth=self.dst_pth,
            src_enc=self.src_enc,
            dst_enc=self.dst_enc,
            block_size=self.block_size,
        ).run()
        os.remove(self.tmp_pth)

    def output(self):
        return luigi.LocalTarget(self.dst_pth)


class ScrapeEntities(luigi.Task):
    src_url = luigi.Parameter()
    src_enc = luigi.Parameter(default="utf-8")
    local_html = luigi.Parameter()
    dst_pth = luigi.Parameter()

    def requires(self):
        return DownloadEncodeTask(
            src_url=self.src_url,
            dst_pth=self.local_html,
            src_enc=self.src_enc,
            dst_enc="utf-8",
        )

    def run(self):
        mk_parent_dirs(self.dst_pth)

        entities = []
        for soup in self.get_soups():
            entities.append(self.parse_entity(soup))

        headers = list(set().union(*(d.keys() for d in entities)))
        headers.sort()
        with open(self.output().path, "w", encoding="utf-8", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, headers)
            dict_writer.writeheader()
            dict_writer.writerows(entities)

    def output(self):
        return luigi.LocalTarget(path=self.dst_pth)

    def get_soups(self):
        pass

    def parse_entity(self, soup: bs4.BeautifulSoup):
        pass


def download_source(url, path) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        with open(path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)


def clean(text: str) -> str:
    return text.replace("\u2009—\u2009", "--").replace("’", "'")


def mk_parent_dirs(path: str):
    p = pl.Path(path)
    if not p.parent.exists():
        p.parent.mkdir()
