import os
from abc import abstractmethod
from typing import Optional
import json
import requests
from dagster import ConfigurableResource, get_dagster_logger
from dagster_dbt import DbtCliClientResource
from dagster_duckdb_pandas import DuckDBPandasIOManager
from pydantic import Field

# get path from root because the dbt cli is a little buggy with relative paths
FILE_PATH = os.path.dirname(os.path.abspath(__file__))

DBT_PROJECT_DIR = os.path.join(FILE_PATH, "dbt_project")
DBT_PROFILE_DIR = os.path.join(DBT_PROJECT_DIR, "profiles")

logger = get_dagster_logger()

duckdb_io_manager = DuckDBPandasIOManager(
    database=os.path.join(FILE_PATH, "..", "dbt_duckdb.db")
)


class IGHOODataResource(ConfigurableResource):
    @abstractmethod
    def make_api_call(self, *args, **kwargs) -> dict:
        raise NotImplementedError()


class GHOODataResource(IGHOODataResource):
    base_url: str = Field(
        "https://ghoapi.azureedge.net/api/", description="GHO API base url"
    )

    def make_api_call(
        self,
        params
    ) -> dict:
        response=requests.get(self.base_url+params)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data
        else:
            raise Exception(f"API call failed with status code {response.status_code}")


resource_def = {
    "LOCAL": {
        "io_manager": duckdb_io_manager,
        "gho": GHOODataResource.configure_at_launch(),
        "dbt": DbtCliClientResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILE_DIR,
            target="local",
        ),
    },
}
