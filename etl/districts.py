import csv
import etl.helpers as hlp
import luigi

from luigi.contrib import sqla


class LoadDistricts(sqla.CopyToTable):
    reflect = True
    connection_string = "sqlite:///data/db/election.db"
    table = "electoral_districts"

    src_url = "https://www.elections.ca/res/cir/list/ED-Canada_2016.csv"
    dst_pth = "data/districts.csv"

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
