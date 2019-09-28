import etl.helpers as hlp
import json
import luigi

from luigi.contrib import sqla


class LoadPPCCandidates(sqla.CopyToTable):
    src_pth = "data/ppc_candidates.json"
    ed_pth = "data/ppc_candidates_ed.json"

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
                    "People's Party of Canada",
                    row.get("name"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    row.get("website"),
                    row.get("facebook"),
                    None,
                    row.get("twitter"),
                    None,
                )


if __name__ == "__main__":
    luigi.build([LoadPPCCandidates()], local_scheduler=True)
