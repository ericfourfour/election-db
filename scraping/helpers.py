import os
import shutil
import urllib.request


def download_source(url, path) -> bytes:
    with urllib.request.urlopen(url) as response:
        with open(path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)


def clean(text: str) -> str:
    return text.replace("\u2009—\u2009", "--").replace("’", "'")

