import pandas as pd
from dagster import asset, op
from dagster_dbt import load_assets_from_dbt_project
from .resources import DBT_PROFILE_DIR, DBT_PROJECT_DIR, IGHOODataResource

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILE_DIR,
)

@op
def drop_columns(dataframe: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
    df_dropped = dataframe.drop(columns=columns_to_drop)
    return df_dropped

@asset(key_prefix=["core"], compute_kind="pandas", group_name="staging")
def raw_gho_gender_inequality(
    gho: IGHOODataResource,
) -> pd.DataFrame:
    get_data = gho.make_api_call("CCO_3")
    gender_inequality_data = pd.DataFrame(get_data['value'])

    columns_to_drop = ['Dim1', 'Dim1Type', 'Dim2', 'Dim2Type', 'DataSourceDim', 'IndicatorCode', 'TimeDimType', 'Dim3',
                       'Dim3Type', 'Low', 'High', 'Comments', 'DataSourceDimType', 'SpatialDimType', 'TimeDimensionValue']
    gender_inequality_data_dropped = drop_columns(gender_inequality_data, columns_to_drop)
    result = gender_inequality_data_dropped.dropna().rename(columns={'SpatialDim': 'CountryCode', 'TimeDim': 'Data Captured'})

    return result


@asset(key_prefix=["core"], compute_kind="pandas", group_name="staging")
def raw_gho_countries(
    gho: IGHOODataResource,
) -> pd.DataFrame:
    get_data = gho.make_api_call("DIMENSION/COUNTRY/DimensionValues")
    country = pd.DataFrame(get_data['value'])

    columns_to_drop = ['ParentDimension', 'Dimension']
    country_dropped = drop_columns(country, columns_to_drop)
    result = country_dropped.dropna().rename(columns={'Title': 'Country', 'ParentCode': 'Region Code',
                                                     'ParentTitle': 'Region', 'Code': 'CountryCode'})
    return result