"""
    test_Service
"""
import pytest
from AzureODM.Service import *


class Test_connect_table_service:

    """test connect_table_service"""

    def test__table_service_wont_imported(self):
        with pytest.raises(NameError) as e:
            _table_service
        assert '_table_service' in str(e)

    def test_will_call_azure_TableService(self, monkeypatch):
        fake_account_name = 'fan'
        fake_account_access_key = 'faak'

        class fake_azure_table_service:

            def __init__(self, *args, **kwargs):
                assert args[0] == fake_account_name
                assert args[1] == fake_account_access_key
                raise MemoryError('called fake_azure_table_service')

        monkeypatch.setattr(
            'AzureODM.Service.TableService', fake_azure_table_service)

        fake_table_service = None
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)
        with pytest.raises(MemoryError) as e:
            connect_table_service(fake_account_name, fake_account_access_key)
        assert 'called fake_azure_table_service' in str(e)

    def test_raises_if__table_service_exist(self, monkeypatch):
        fake_account_name = 'fan'
        fake_account_access_key = 'faak'

        class fake_azure_table_service:

            def __init__(self, *args, **kwargs):
                assert args[0] == fake_account_name
                assert args[1] == fake_account_access_key
                # return 'lol'

        monkeypatch.setattr(
            'AzureODM.Service.TableService', fake_azure_table_service)

        fake_table_service = None
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)

        connect_table_service(fake_account_name, fake_account_access_key)
        with pytest.raises(Exception) as e:
            connect_table_service(fake_account_name, fake_account_access_key)
        assert 'table_service is exist' in str(e)


class Test_get_table_service:

    """test get_table_service"""

    def test_should_raises_if__table_service_is_None(self, monkeypatch):
        fake_table_service = None
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)
        with pytest.raises(Exception) as e:
            get_table_service()
        assert 'Not connected to any table service' in str(e)

    def test_should_return_global_table_service(self, monkeypatch):
        fake_table_service = 'lol'
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)
        assert get_table_service() == fake_table_service


class Test_set_table_service:

    """test set_table_service"""

    def test_should_raises_if_exists(self, monkeypatch):
        fake_table_service = 'lol'
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)
        with pytest.raises(Exception) as e:
            set_table_service(ts='newlol')
        assert 'table service already exists' in str(e)


class Test_reset_table_service:

    """test reset_table_service"""

    def test_should_make__table_service_None(self, monkeypatch):
        fake_table_service = 'lol'
        monkeypatch.setattr(
            'AzureODM.Service._table_service', fake_table_service)
        reset_table_service()
        with pytest.raises(Exception) as e:
            get_table_service()
        assert 'Not connected to any table service' in str(e)
