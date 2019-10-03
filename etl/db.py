import luigi
import os
import pandas as pd
import sqlite3


class ElectionDbUp(luigi.Task):
    script = "schema/up.sql"
    connection_string = luigi.Parameter(default="sqlite:///election.db")
    resources = {"max_workers": 1}

    def run(self):
        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            with open(self.script, "r") as sql_file:
                sql = sql_file.read()
                db.executescript(sql)

    def complete(self):
        if not os.path.exists(self.connection_string.replace("sqlite:///", "")):
            return False
        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            tables = pd.read_sql_query(
                "select name from sqlite_master where type = 'table' order by 1;", db
            )
        return (
            "electoral_districts" in tables["name"].values
            and "parties" in tables["name"].values
            and "candidates" in tables["name"].values
        )


class ElectionDbDown(luigi.Task):
    script = "schema/down.sql"
    connection_string = luigi.Parameter(default="sqlite:///election.db")
    resources = {"max_workers": 1}

    def run(self):
        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            with open(self.script, "r") as sql_file:
                sql = sql_file.read()
                db.executescript(sql)

    def complete(self):
        if not os.path.exists(self.connection_string.replace("sqlite:///", "")):
            return True
        with sqlite3.connect(self.connection_string.replace("sqlite:///", "")) as db:
            tables = pd.read_sql_query(
                "select name from sqlite_master where type = 'table' order by 1;", db
            )
        return not (
            "electoral_districts" in tables["name"].values
            and "parties" in tables["name"].values
            and "candidates" in tables["name"].values
        )

