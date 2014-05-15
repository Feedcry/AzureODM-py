"""
    test_AzureSDK_Behavior
"""
import pytest
from datetime import timezone, datetime


@pytest.mark.azure
class Test_Table_Entity:

    """test Table Entity"""
    @pytest.fixture()
    def entity_dict(self):
        return {
            'PartitionKey': 'tasksSeattle',
            'RowKey': '1',
            'Description': 'Take out the trash',
            'someInt': 231234123412341,
            'DueDate': datetime(2011, 12, 14, 1, 1, 1, tzinfo=timezone.utc)
        }

    @pytest.fixture()
    def stored_entity(self,
                      entity_dict,
                      request,
                      azure_ts,
                      azure_test_table_name):
        def fin():
            azure_ts.delete_entity(
                azure_test_table_name,
                entity_dict['PartitionKey'],
                entity_dict['RowKey']
            )
        request.addfinalizer(fin)
        response = azure_ts.insert_entity(
            azure_test_table_name,
            entity_dict
        )
        return response

    def test_return_datetime_naive(self,
                                   request,
                                   azure_ts,
                                   azure_test_table_name,
                                   entity_dict,
                                   stored_entity):

        # even insert failed, we will still try to delete

        entity = azure_ts.get_entity(
            azure_test_table_name,
            entity_dict['PartitionKey'],
            entity_dict['RowKey']
        )

        dt = entity.DueDate
        assert dt.tzinfo is None
        assert dt.isoformat() == '2011-12-14T01:01:01'

    def test_merge_return_etag_only_dict(self,
                                         request,
                                         azure_ts,
                                         azure_test_table_name,
                                         entity_dict,
                                         stored_entity):
        to_be_merged = entity_dict
        to_be_merged['Description'] = 'lol'
        rsp = azure_ts.merge_entity(
            table_name=azure_test_table_name,
            partition_key=entity_dict['PartitionKey'],
            row_key=entity_dict['RowKey'],
            entity=to_be_merged
        )
        assert isinstance(rsp, dict)
        assert len(rsp) == 1
        assert 'etag' in rsp
