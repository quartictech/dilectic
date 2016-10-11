# Dilectic Demo
## Running the stack
 - Grab the data from the cloud bucket here: https://storage.cloud.google.com/quartic-newyork/raw-data` 
 - Unzip it in `<repository root>/data` (so you should have `repository root/data/raw`)
 - `sudo docker-compose up -d`
 - Build the data integration docker image: `cd transform-raw && docker build -t data-integration .`
 - Run the the data integrations: `cd integration && ./run.sh`
