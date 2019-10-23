import etl.helpers as hlp
import json
import luigi

from etl.districts import LoadDistricts
from etl.parties import LoadParties
from luigi.contrib import sqla


class LoadNDPCandidates(sqla.CopyToTable):
    src_pth = "data/ndp_candidates.json"

    reflect = True
    connection_string = luigi.Parameter(default="sqlite:///election.db")
    table = "candidates"

    def requires(self):
        return [
            LoadDistricts(connection_string=self.connection_string),
            LoadParties(connection_string=self.connection_string),
            hlp.RunSpider(spider="ndp-candidate-spider", dst_pth=self.src_pth),
        ]

    def rows(self):
        with open(self.src_pth, newline="", encoding="utf-8") as json_file:
            data = json.load(json_file)
            for row in data:
                yield (
                    row.get("ed_code"),
                    "New Democratic Party",
                    row.get("name"),
                    None,
                    row.get("cabinet_position"),
                    None,
                    None,
                    row.get("photo"),
                    row.get("donate"),
                    row.get("volunteer"),
                    row.get("lawnsign"),
                    row.get("website"),
                    row.get("facebook"),
                    row.get("instagram"),
                    row.get("twitter"),
                    row.get("bio"),
                )


if __name__ == "__main__":
    luigi.build([LoadNDPCandidates()], local_scheduler=True)
