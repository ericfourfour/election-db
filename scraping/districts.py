"""
Download all of the Federal Electoral Districts and save to CSV

Link: https://www.elections.ca/res/cir/list/ED-Canada_2016.csv
"""

import codecs
import os
import shutil
import urllib.request


def process(config):
    file_path = os.path.join(config["folder"], config["filename"])
    tmp_path = "{}.tmp".format(file_path)

    if os.path.exists(file_path):
        os.remove(file_path)

    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    with urllib.request.urlopen(config["url"]) as response:
        with open(tmp_path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)

    BLOCKSIZE = 1048576
    with codecs.open(tmp_path, "r", "mbcs") as sourceFile:
        with codecs.open(file_path, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                targetFile.write(contents)

    os.remove(tmp_path)


if __name__ == "__main__":
    config = {
        "url": "https://www.elections.ca/res/cir/list/ED-Canada_2016.csv",
        "folder": "data",
        "filename": "districts.csv",
    }

    process(config)
