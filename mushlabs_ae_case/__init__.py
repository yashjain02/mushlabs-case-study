import os

from dagster import Definitions, load_assets_from_modules

from . import assets, resources

ENV = os.getenv("DAGSTER_ENV", "LOCAL")

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources=resources.resource_def[ENV.upper()],
)
