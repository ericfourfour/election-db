import json
import luigi
import etl.helpers as hlp

from etl.db import ElectionDbUp
from luigi.contrib import sqla


class LoadParties(sqla.CopyToTable):
    src_pth = "data/parties.json"

    reflect = True
    connection_string = luigi.Parameter(default="sqlite:///election.db")
    table = "parties"

    def requires(self):
        return [
            ElectionDbUp(connection_string=self.connection_string),
            hlp.RunSpider(spider="party-spider", dst_pth=self.src_pth),
        ]

    def rows(self):
        with open(self.src_pth, newline="", encoding="utf-8") as json_file:
            data = json.load(json_file)
            for row in data:
                yield (
                    row.get("title"),
                    row.get("short_name"),
                    row.get("eligible_dt"),
                    row.get("registered_dt"),
                    row.get("deregistered_dt"),
                    row.get("leader"),
                    row.get("logo"),
                    row.get("website"),
                    row.get("national_headquarters"),
                    row.get("chief_agent"),
                    row.get("auditor"),
                )


if __name__ == "__main__":
    luigi.build([LoadParties()], local_scheduler=True)
