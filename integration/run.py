import os.path
from doit.doit_cmd import DoitMain
from dilectic.utils import *
import sys

config_file = os.path.join(os.path.dirname(__file__), 'config.yml')
cfg = Config(config_file)

if __name__ == "__main__":
    import integrations.jamcams
    import integrations.preprocess
    import integrations.migration
    import integrations.boroughs
    import integrations.companies
    import integrations.mcdonalds
    import integrations.tube
    import integrations.public_land_assets
    import integrations.postcodes
    import integrations.london_price_houses
    import integrations.crime
    import integrations.boundaries
    import integrations.osm
    import integrations.road_traffic
    import integrations.billboards
    import integrations.ni_num_registrations
    import integrations.disruptions

    task_loader = IntegrationTaskLoader(cfg,
        integrations.preprocess,
        integrations.jamcams,
        integrations.migration,
        integrations.boroughs,
        integrations.companies,
        integrations.mcdonalds,
        integrations.tube,
        integrations.public_land_assets,
        integrations.postcodes,
        integrations.london_price_houses,
        integrations.crime,
        integrations.boundaries,
        integrations.osm,
        integrations.road_traffic,
        integrations.billboards,
        integrations.ni_num_registrations,
        integrations.disruptions
    )
    sys.exit(DoitMain(task_loader).run(sys.argv[1:]))
