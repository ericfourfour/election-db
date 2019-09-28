import codecs
import luigi
import os
import pathlib
import shutil
import urllib


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
    block_size = luigi.IntParameter(default=1048576)

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
    block_size = luigi.IntParameter(default=1048576)

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
