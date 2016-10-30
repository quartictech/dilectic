import os.path
from doit.doit_cmd import DoitMain
from dilectic.utils import *
import sys

config_file = os.path.join(os.path.dirname(__file__), 'config.yml')
cfg = Config(config_file)

if __name__ == "__main__":
    import integrations.jamcams
    import integrations.preprocess
    task_loader = IntegrationTaskLoader(cfg,
        integrations.jamcams,
        integrations.preprocess,
    )
    sys.exit(DoitMain(task_loader).run(sys.argv[1:]))
