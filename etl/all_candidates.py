import luigi

from etl.lpc_candidates import LoadLPCCandidates
from etl.cpc_candidates import LoadCPCCandidates
from etl.ndp_candidates import LoadNDPCandidates
from etl.bq_candidates import LoadBQCandidates
from etl.gpc_candidates import LoadGPCCandidates
from etl.ppc_candidates import LoadPPCCandidates


class LoadAllCandidates(luigi.Task):
    def requires(self):
        return [
            LoadLPCCandidates(),
            LoadCPCCandidates(),
            LoadNDPCandidates(),
            LoadBQCandidates(),
            LoadGPCCandidates(),
            LoadPPCCandidates(),
        ]


if __name__ == "__main__":
    luigi.build([LoadAllCandidates()], local_scheduler=True)
