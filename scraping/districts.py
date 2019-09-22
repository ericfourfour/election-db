import codecs
import csv
import scraping.helpers as hlp
import luigi
import os
import shutil
import urllib.request

from luigi.contrib import sqla
from sqlalchemy import String, Integer


class LoadDistricts(sqla.CopyToTable):
    src_url = luigi.Parameter(
        default="https://www.elections.ca/res/cir/list/ED-Canada_2016.csv"
    )
    dst_pth = luigi.Parameter(default="data/districts.csv")

    columns = [
        (["ed_code", Integer()], {"primary_key": True}),
        (["ed_namee", String(64)], {}),
        (["ed_namef", String(64)], {}),
        (["population", Integer()], {}),
    ]
    connection_string = "sqlite:///data/election.db"
    table = "electoral_districts"

    def requires(self):
        return hlp.DownloadEncodeTask(
            src_url=self.src_url, dst_pth=self.dst_pth, src_enc="mbcs", dst_enc="utf-8"
        )

    def rows(self):
        with open(self.dst_pth, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield (
                    int(row["ED_CODE"]),
                    row["ED_NAMEE"],
                    row["ED_NAMEF"],
                    int(row["POPULATION"]),
                )


if __name__ == "__main__":
    luigi.build([LoadDistricts()], local_scheduler=True)
