import pytest
import requests_mock
from mushlabs_ae_case.resources import GHOODataResource

@pytest.fixture
def mock_requests():
    with requests_mock.Mocker() as m:
        yield m

def test_make_api_call_success(mock_requests):
    mock_requests.get('https://ghoapi.azureedge.net/api/test', text='{"value": []}', status_code=200)
    resource = GHOODataResource()
    result = resource.make_api_call('test')
    assert result == {'value': []}

def test_make_api_call_failure(mock_requests):
    mock_requests.get('https://ghoapi.azureedge.net/api/test', status_code=404)
    resource = GHOODataResource()
    with pytest.raises(Exception) as exc_info:
        resource.make_api_call('test')
    assert str(exc_info.value) == 'API call failed with status code 404'
