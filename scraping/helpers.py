import os
import shutil
import urllib.request


def download_source(url, path) -> bytes:
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        with open(path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)


def clean(text: str) -> str:
    return text.replace("\u2009—\u2009", "--").replace("’", "'")

