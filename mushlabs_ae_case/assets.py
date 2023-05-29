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

@op
def convert_to_date_and_time(df: pd.DataFrame, columns_to_convert: list, datetime_format: str) -> pd.DataFrame:
    for column in columns_to_convert:
        df[column] = pd.to_datetime(df[column], format=datetime_format)
    return df

@op
def rename_columns(column_rename_df, columns):
    return column_rename_df.dropna().rename(columns=columns)

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


@asset(key_prefix=["core"], compute_kind="pandas", group_name="staging")
def raw_gho_countries(
    gho: IGHOODataResource,
) -> pd.DataFrame:
    columns_to_rename = {'Title': 'Country', 'ParentCode': 'RegionCode',
                        'ParentTitle': 'Region', 'Code': 'CountryCode'}
    columns_to_drop = ['ParentDimension', 'Dimension']

    get_data = gho.make_api_call("DIMENSION/COUNTRY/DimensionValues")
    country = pd.DataFrame(get_data['value'])
    country_dropped = drop_columns(country, columns_to_drop)
    result = rename_columns(country_dropped, columns=columns_to_rename)
    return result