"""
    Service
"""
from azure.storage import TableService
__all__ = ['connect_table_service', 'set_table_service',
           'get_table_service', 'reset_table_service']

_table_service = None


def connect_table_service(account_name, account_access_key):
    #global _make_sure_tables_exist
    global _table_service
    # if (_ts is not None
    #        and _ts.account_name == storage_conifg[ENV]['AccountName']):
    if _table_service is not None:
        raise Exception('table_service is exist')
    _table_service = TableService(account_name,
                                  account_access_key)
    return _table_service


def get_table_service():
    global _table_service
    if _table_service is None:
        raise Exception('Not connected to any table service')
    return _table_service


def set_table_service(ts):
    global _table_service
    if _table_service is not None:
        raise Exception('table service already exists')
    _table_service = ts


def reset_table_service():
    global _table_service
    _table_service = None
