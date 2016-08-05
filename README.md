# Dilectic Demo
## Instructions
This repo contains import scripts and `docker-compose` infrastructure for spinning up the dilectic demo stack.

To get started:

1. Get hold of the source data and put it somewhere convenient
2. Copy the `config.yml.example` to `config.yml` and edit it to suit your needs.
3. Run `docker-compose up -d` to get the demo stack going on
4. Set up an appropriate python virtual environment and `pip -r requirements.txt`
5. Run `./scripts/import.py` to build all the required tables in postgres.
