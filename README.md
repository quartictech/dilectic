# Dilectic Demo
## Running the stack
 - Grab the data from the cloud bucket here: https://storage.cloud.google.com/quartic-newyork/raw-data`
   `gsutil rsync gs://quartic-newyork/raw-data <repository root>/data/raw`
 - Unzip it in `<repository root>/data` (so you should have `repository root/data/raw`)
 - `sudo docker-compose up -d`
 - Build the data integration docker image: `cd transform-raw && docker build -t data-integration .`
 - Run the the data integrations: `cd integration && ./run.sh`

## Importing into Weyl

    cd weyl_imports
    pip install -r requirements.txt
    python import.py -p jdbc:postgresql://postgis/postgres yml_configs/*

## License

This project is made available under [BSD License 2.0](https://github.com/quartictech/dilectic/blob/develop/LICENSE).
