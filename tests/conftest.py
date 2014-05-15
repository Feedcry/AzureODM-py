"""
    conftest for AzureODM
"""
import pytest
import configparser
import os
storage_conifg = configparser.ConfigParser()
storage_conifg.read(os.path.abspath(
    os.path.dirname(__file__) + '/config.ini'))
from AzureODM.Service import (
    connect_table_service, reset_table_service, get_table_service)


@pytest.fixture()
def azure_test_table_name():
    return storage_conifg['Storage']['TestTableName']


@pytest.mark.azure
@pytest.fixture(scope="session")
def azure_ts(request):
    def finalize():
        reset_table_service()
    request.addfinalizer(finalize)
    assert 'Storage' in storage_conifg.sections()
    account_name = storage_conifg['Storage']['AccountName']
    account_access_key = storage_conifg['Storage']['AccountAccessKey']
    assert isinstance(account_name, str)
    assert isinstance(account_access_key, str)
    try:
        reset_table_service()
    except:
        pass
    connect_table_service(
        account_name=account_name,
        account_access_key=account_access_key)
    ts = get_table_service()
    ts.create_table(storage_conifg['Storage']['TestTableName'],
                    fail_on_exist=False)
    return ts


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true",
                     help="run slow tasks")


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")
