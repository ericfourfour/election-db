import etl.helpers as hlp
import json
import luigi

from luigi.contrib import sqla


class LoadCPCCandidates(sqla.CopyToTable):
    src_pth = "data/cpc_candidates.json"
    ed_pth = "data/cpc_candidates_ed.json"

    reflect = True
    connection_string = "sqlite:///data/db/election.db"
    table = "candidates"

    def requires(self):
        return hlp.LookupEDCodes(src_pth=self.src_pth, dst_pth=self.ed_pth)

    def rows(self):
        with open(self.ed_pth, newline="", encoding="utf-8") as json_file:
            data = json.load(json_file)
            for row in data:
                yield (
                    row.get("ed_code"),
                    "Conservative Party of Canada",
                    row.get("name"),
                    row.get("nomination_dt"),
                    row.get("cabinate_position"),
                    None,
                    row.get("phone"),
                    row.get("photo"),
                    row.get("donate"),
                    None,
                    None,
                    row.get("website"),
                    row.get("facebook"),
                    row.get("instagram"),
                    row.get("twitter"),
                    row.get("bio"),
                )


if __name__ == "__main__":
    luigi.build([LoadCPCCandidates()], local_scheduler=True)
