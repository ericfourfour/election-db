# election-db

Scrapes the web to build a database containing info about the Canadian election

## Run

```cmd
scrapy crawl party-spider -t json --nolog -o - > data/parties.json
scrapy crawl lpc-candidate-spider -t json --nolog -o - > data/lpc_candidates.json
scrapy crawl cpc-candidate-spider -t json --nolog -o - > data/cpc_candidates.json
scrapy crawl bq-candidate-spider -t json --nolog -o - > data/bq_candidates.json
scrapy crawl ndp-candidate-spider -t json --nolog -o - > data/ndp_candidates.json
scrapy crawl ppc-candidate-spider -t json --nolog -o - > data/ppc_candidates.json
scrapy crawl gpc-candidate-spider -t json --nolog -o - > data/gpc_candidates.json
```
