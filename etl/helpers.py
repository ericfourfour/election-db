import codecs
import json
import luigi
import os
import pandas as pd
import pathlib
import re
import shutil
import sqlite3
import subprocess
import urllib

from etl.districts import LoadDistricts


def try_get_first(d: dict, key: str):
    return d[key][0] if key in d else None


def mk_parent_dirs(path: str):
    p = pathlib.Path(path)
    if not p.parent.exists():
        p.parent.mkdir()


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
    block_size = luigi.IntParameter(default=1_048_576)

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
    block_size = luigi.IntParameter(default=1_048_576)

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


class RunSpider(luigi.Task):
    spider = luigi.Parameter()
    dst_pth = luigi.Parameter()

    def run(self):
        cmd = ["scrapy", "crawl", self.spider, "-t", "json", "--nolog", "-o", "-"]
        with open(self.dst_pth, "w", encoding="utf-8") as out_f:
            p = subprocess.Popen(cmd, stdout=out_f)
            p.wait()

    def output(self):
        return luigi.LocalTarget(path=self.dst_pth)

    def complete(self):
        return os.path.exists(self.dst_pth) and os.stat(self.dst_pth).st_size > 0


class LookupEDCodes(luigi.Task):
    src_pth = luigi.Parameter()
    dst_pth = luigi.Parameter()
    spider = luigi.Parameter()
    lang = luigi.Parameter(default="EN")

    connection_string = luigi.Parameter(default="sqlite:///election.db")

    def requires(self):
        return [
            LoadDistricts(connection_string=self.connection_string),
            RunSpider(spider=self.spider, dst_pth=self.src_pth),
        ]

    def run(self):
        def simplify_riding(riding: str) -> str:
            riding = riding.replace("œ", "oe").replace("Œ", "OE")
            riding = re.sub(r"[^A-Za-z]+", "", riding)
            riding = riding.lower()
            return riding

        mk_parent_dirs(self.dst_pth)

        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            districts = pd.read_sql_query("SELECT * FROM electoral_districts", db)

        if self.lang == "EN":
            districts["riding"] = districts["ed_namee"].apply(simplify_riding)
        elif self.lang == "FR":
            districts["riding"] = districts["ed_namef"].apply(simplify_riding)
        districts.set_index("riding", inplace=True)
        districts = districts[["ed_code"]].astype("str")

        candidates = pd.read_json(self.src_pth)
        candidates["riding"] = candidates["riding"].apply(simplify_riding)
        candidates.set_index("riding", inplace=True)

        candidates = candidates.join(districts)

        candidates.to_json(self.dst_pth, orient="records")

    def complete(self):
        if not os.path.exists(self.dst_pth):
            return False
        candidates = pd.read_json(self.dst_pth)
        return "ed_code" in candidates.columns

