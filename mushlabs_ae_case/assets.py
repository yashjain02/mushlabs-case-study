import pandas as pd
from dagster import asset, op
from dagster_dbt import load_assets_from_dbt_project
from .resources import DBT_PROFILE_DIR, DBT_PROJECT_DIR, IGHOODataResource

#reads dbt project and run the model
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILE_DIR,
)


#This op drop the given column from the given dataframe
@op
def drop_columns(dataframe: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
    dropped_column_df = dataframe.drop(columns=columns_to_drop)
    return dropped_column_df


#This opeartion convert str to dattime format
@op
def convert_to_date_and_time(converted_date_df: pd.DataFrame, columns_to_convert: list, datetime_format: str) -> pd.DataFrame:
    for column in columns_to_convert:
        converted_date_df[column] = pd.to_datetime(converted_date_df[column], format=datetime_format)
    return converted_date_df

#This opeartion renames the column in given dataframe
@op
def rename_columns(column_rename_df, columns)->pd.DataFrame:
    return column_rename_df.dropna().rename(columns=columns)

#This assest gets gender inequality data and does ETL and loaded to duckdb in local
@asset(key_prefix=["core"], compute_kind="pandas", group_name="staging")
def raw_gho_gender_inequality(
    gho: IGHOODataResource,
) -> pd.DataFrame:
    columns_to_drop = ['Dim1', 'Dim1Type', 'Dim2', 'Dim2Type', 'DataSourceDim', 'IndicatorCode', 'TimeDimType', 'Dim3',
                       'Dim3Type', 'Low', 'High', 'Comments', 'DataSourceDimType', 'SpatialDimType', 'TimeDimensionValue']
    columns_to_convert = ['Date', 'TimeDimensionBegin', 'TimeDimensionEnd']
    columns_to_rename={'SpatialDim': 'CountryCode', 'TimeDim': 'DataCaptured'}
    get_data = gho.make_api_call("CCO_3")
    gender_inequality_data = pd.DataFrame(get_data['value'])
    gender_inequality_data_dropped = drop_columns(gender_inequality_data, columns_to_drop)
    gender_inequality_data_renamed = rename_columns(gender_inequality_data_dropped, columns=columns_to_rename)
    result = convert_to_date_and_time(gender_inequality_data_renamed, columns_to_convert, datetime_format = '%Y-%m-%d %H:%M:%S')
    return result

#This assest gets countries data from the api and does ETL and loaded to duckdb in local
@asset(key_prefix=["core"], compute_kind="pandas", group_name="staging")
def raw_gho_countries(
    gho: IGHOODataResource
) -> pd.DataFrame:
    columns_to_rename = {'Title': 'Country', 'ParentCode': 'RegionCode',
                        'ParentTitle': 'Region', 'Code': 'CountryCode'}
    columns_to_drop = ['ParentDimension', 'Dimension']

    get_data = gho.make_api_call("DIMENSION/COUNTRY/DimensionValues")
    country = pd.DataFrame(get_data['value'])
    country_dropped = drop_columns(country, columns_to_drop)
    result = rename_columns(country_dropped, columns=columns_to_rename)
    return result