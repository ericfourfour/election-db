# election-db

Scrapes the web to build a database containing info about the Canadian election

## Run

```cmd
scrapy crawl party-spider -t json --nolog -o - > items.json
```
