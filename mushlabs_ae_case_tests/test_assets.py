from mushlabs_ae_case.assets import (
    raw_gho_countries,  
    rename_columns, 
    drop_columns, 
    convert_to_date_and_time
)
from dagster import op
import pandas as pd
from mushlabs_ae_case.resources import IGHOODataResource



class MockGHOODataResource(IGHOODataResource):
    def make_api_call(self, params):
        data = {
            'value': [
                {"Code":"ABW","Title":"Aruba","ParentDimension":"REGION","Dimension":"COUNTRY","ParentCode":"AMR","ParentTitle":"Americas"},
            ]
        }
        return data


@op
def test_drop_columns():
    sample_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
    columns_to_drop = ['B', 'C']
    expected_output = pd.DataFrame({'A': [1, 2, 3]})
    result = drop_columns(sample_data, columns_to_drop)
    assert result.equals(expected_output)

@op
def test_convert_to_date_and_time():
    sample_data = pd.DataFrame({'Date': ['2013-07-17 13:39:21.807 +0000'], 'TimeDimensionBegin': ['2011-12-31 23:00:00.000 +0000']})
    columns_to_convert = ['Date', 'TimeDimensionBegin']
    datetime_format = '%Y-%m-%d %H:%M:%S'
    expected_output = pd.DataFrame({'Date': [pd.Timestamp('2013-07-17 13:39:21.807 +0000')],
                                    'TimeDimensionBegin': [pd.Timestamp('2011-12-31 23:00:00.000 +0000')]})
    result = convert_to_date_and_time(sample_data, columns_to_convert, datetime_format)
    assert result.equals(expected_output)

@op
def test_rename_columns():
    sample_data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
    columns_to_rename = {'A': 'ColumnA', 'B': 'ColumnB', 'C': 'ColumnC'}
    expected_output = pd.DataFrame({'ColumnA': [1, 2, 3], 'ColumnB': [4, 5, 6], 'ColumnC': [7, 8, 9]})
    result = rename_columns(sample_data, columns_to_rename)
    assert result.equals(expected_output)

def test_raw_gho_countries():
    gho_resource = MockGHOODataResource()

    result = raw_gho_countries(gho=gho_resource)
    expected_df = pd.DataFrame({
        'CountryCode': ['ABW'],
        'Country': ['Aruba'],
        'RegionCode': ['AMR'],
        'Region': ['Americas'],
    })

    assert result.equals(expected_df)