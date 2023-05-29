from setuptools import find_packages, setup

setup(
    name="mushlabs_ae_case",
    packages=find_packages(exclude=["mushlabs_ae_case_tests"]),
    install_requires=[
        "setuptools",
        "dagster==1.3.5",
        "dagster-dbt==0.19.5",
        "dbt-core==<1.5.0",
        "dbt-duckdb==<1.5.0",
        "dagster-duckdb-pandas==^0.19.5",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
