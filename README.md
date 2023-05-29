# mushlabs-ae-case

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Alternatively, you can install the package via poetry which will also manage the virtual environment by running

```bash
poetry install
```

### Dagster

Start the Dagster UI web server (if using poetry, make sure you activate the poetry environment by running `poetry shell`):

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

### dbt

You can run dbt models directly from the Dagster interface by selecting "materialize". Alternatively, run dbt models from the command line by running the following command while in the dbt_project root directory:

```bash
dbt run --profiles-dir ./profiles
```


## Development


### Unit testing

Tests are in the `mushlabs_ae_case_tests` directory and you can run tests using `pytest`:

```bash
pytest mushlabs_ae_case_tests
```