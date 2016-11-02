from dilectic.jester import Jester, PostgresDatasource

DEFAULT_ATTRIBUTION = "Unknown"

if __name__ == "__main__":
    jester = Jester("http://localhost:8090/jester/api")

    postgres = PostgresDatasource(
        url = "localhost/postgres",
        user = "postgres",
        password = "dilectic"
    )

    jester.dataset(
        name = "UK Postcodes",
    	description = "All postcode centroids in the UK",
        attribution = DEFAULT_ATTRIBUTION,
    	source = postgres.query("SELECT * from uk_postcodes")
    )
