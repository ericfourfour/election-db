import etl.helpers as hlp
import json
import luigi

from etl.districts import LoadDistricts
from etl.parties import LoadParties
from luigi.contrib import sqla


class LoadBQCandidates(sqla.CopyToTable):
    src_pth = "data/bq_candidates.json"
    ed_pth = "data/bq_candidates_ed.json"

    reflect = True
    connection_string = luigi.Parameter(default="sqlite:///election.db")
    table = "candidates"

    def requires(self):
        return [
            LoadDistricts(connection_string=self.connection_string),
            LoadParties(connection_string=self.connection_string),
            hlp.LookupEDCodes(
                connection_string=self.connection_string,
                src_pth=self.src_pth,
                dst_pth=self.ed_pth,
                spider="bq-candidate-spider",
                lang="FR",
            ),
        ]

    def rows(self):
        with open(self.ed_pth, newline="", encoding="utf-8") as json_file:
            data = json.load(json_file)
            for row in data:
                yield (
                    row.get("ed_code"),
                    "Bloc Québécois",
                    row.get("name"),
                    None,
                    None,
                    row.get("email"),
                    row.get("phone"),
                    row.get("photo"),
                    None,
                    None,
                    None,
                    row.get("website"),
                    row.get("facebook"),
                    row.get("instagram"),
                    row.get("twitter"),
                    row.get("bio"),
                )


if __name__ == "__main__":
    luigi.build([LoadBQCandidates()], local_scheduler=True)
