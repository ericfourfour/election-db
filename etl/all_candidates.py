import luigi

from etl.lpc_candidates import LoadLPCCandidates
from etl.cpc_candidates import LoadCPCCandidates
from etl.ndp_candidates import LoadNDPCandidates
from etl.bq_candidates import LoadBQCandidates
from etl.gpc_candidates import LoadGPCCandidates
from etl.ppc_candidates import LoadPPCCandidates


class LoadAllCandidates(luigi.Task):
    connection_string = luigi.Parameter(default="sqlite:///election.db")

    def requires(self):
        return [
            LoadLPCCandidates(connection_string=self.connection_string),
            LoadCPCCandidates(connection_string=self.connection_string),
            LoadNDPCandidates(connection_string=self.connection_string),
            LoadBQCandidates(connection_string=self.connection_string),
            LoadGPCCandidates(connection_string=self.connection_string),
            LoadPPCCandidates(connection_string=self.connection_string),
        ]


if __name__ == "__main__":
    luigi.build(
        [LoadAllCandidates(connection_string="sqlite:///election.db")],
        local_scheduler=True,
    )
