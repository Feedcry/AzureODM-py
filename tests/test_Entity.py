"""
    test_Entity
"""
import pytest
from AzureODM.Fields import (
    GenericField, FloatField, KeyField, DateField, JSONField)
from AzureODM.Entity import Entity
from azure import WindowsAzureMissingResourceError


class Test___cache_fields:

    """test _cache_fields"""

    def test_find_GenericField_attr_store_in__f(self):
        field1 = GenericField(_type=str, required=False)
        field2 = FloatField(required=False)
        field3 = 'LOLLOLOLOL'

        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = field1
            f2 = field2
            f3 = field3

        s = FakeEntity()
        assert isinstance(s._f, dict)
        assert len(s._f) == 4
        assert s._f['f1'] == field1
        assert s.f1 is None
        assert s._f['f2'] == field2
        assert s.f2 is None
        assert s.f3 == field3

    def test_raises_AttributeError_if_PartitionKey_not_exist(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            RowKey = KeyField()
        with pytest.raises(AttributeError) as e:
            FakeEntity()
        assert 'a StringField PartitionKey is required' in str(e)

    def test_raises_AttributeError_if_PartitionKey_type_not_str(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            PartitionKey = FloatField()
            RowKey = KeyField()
        with pytest.raises(AttributeError) as e:
            FakeEntity()
        assert 'a StringField PartitionKey is required' in str(e)

    def test_raises_AttributeError_if_RowKey_not_exist(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            PartitionKey = KeyField()
        with pytest.raises(AttributeError) as e:
            FakeEntity()
        assert 'a StringField RowKey is required' in str(e)

    def test_raises_AttributeError_if_RowKey_type_not_str(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            RowKey = FloatField()
            PartitionKey = KeyField()
        with pytest.raises(AttributeError) as e:
            FakeEntity()
        assert 'a StringField RowKey is required' in str(e)

    def test_will_only_be_called_once_support_re_cache(self):
        """Won't be able to change the Fields definition later"""
        field1 = GenericField(_type=str, required=False)
        field2 = FloatField(required=False)
        field3 = 'LOLLOLOLOL'
        field4 = FloatField(required=False)

        class FakeEntity(Entity):
            metas = {
                'table_name': 'fakeEntity'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = field1
            f2 = field2
            f3 = field3
            f4 = 'lol'

        s = FakeEntity(f1='lol', f2=1.2)
        assert isinstance(s._f, dict)
        assert len(s._f) == 4
        assert s._f['f1'] == field1
        assert s._f['f2'] == field2
        assert s.f1 == 'lol'
        assert s.f2 == 1.2
        setattr(FakeEntity, 'f4', field4)
        #FakeEntity.__dict__['f4'] = field4
        s2 = FakeEntity(f2=1.1)
        assert s2.f1 is None
        assert s2.f2 == 1.1
        assert getattr(s2._f, 'f4', None) is None
        assert isinstance(s2.f4, FloatField)
        with pytest.raises(KeyError) as e:
            s2._f['f4']
        assert 'f4' in str(e)
        # with pytest.raises(AttributeError):

        s2._cache_fields(re_cache=True)
        assert s2.f2 is None
        assert s2.f4 is None
        assert s2._f['f4'] == field4
        assert s.f1 == 'lol'
        assert s.f2 == 1.2


class Test__validate:

    """test _validate()"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f = FloatField(required=True)
        return FakeEntity

    def test_required_is_None_raises(self, fake_entity):
        en = fake_entity(PartitionKey='p1', RowKey='r1')
        with pytest.raises(ValueError) as e:
            en._validate()
        assert 'f is required' in str(e)

    def test_required_can_be_None_if_is_partial(self, fake_entity):
        en = fake_entity(PartitionKey='p1', RowKey='r1')
        en._is_partial = True
        en._validate()

    def test_type_not_matches_raises(self, fake_entity):
        en = fake_entity(PartitionKey='p1', RowKey='r1', f='123')
        with pytest.raises(TypeError) as e:
            en._validate()
        assert 'f is not type: float' in str(e)

    def test_no_PartitionKey_raises(self, fake_entity):
        en = fake_entity(RowKey='r1', f=123.0)
        with pytest.raises(ValueError) as e:
            en._validate()
        assert 'PartitionKey is required' in str(e)

    def test_no_RowKey_raises(self, fake_entity):
        en = fake_entity(PartitionKey='p1', f=123.0)
        with pytest.raises(ValueError) as e:
            en._validate()
        assert 'RowKey is required' in str(e)


class Test_init:

    """test init"""

    def test_raises_when_no_metas_table_name(self):
        class FakeEntity(Entity):
            pass
        with pytest.raises(AttributeError) as e:
            FakeEntity()
        assert 'no table_name meta provided' in str(e)

    @pytest.mark.parametrize('table_name', [
        '123abc',
        'abc_1234',
        '1'
    ])
    def test_raises_when_table_name_not_valid(self, table_name):
        class FakeEntity(Entity):
            metas = {
                'table_name': table_name
            }
        with pytest.raises(ValueError) as e:
            FakeEntity()
        assert 'table_name is not in valid format, ' in str(e)

    def test_differet_subclass_different_table_name(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 't1'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        class FakeEntity2(Entity):
            metas = {
                'table_name': 't2'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
        assert FakeEntity.metas['table_name'] == 't1'
        assert FakeEntity2.metas['table_name'] == 't2'

    def test_call___cache_fields(self, monkeypatch):
        def fake_cache_fields(*args, **kwargs):
            raise Exception('called _cache_fields')
        monkeypatch.setattr(Entity, '_cache_fields', fake_cache_fields)

        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab123'
            }
        with pytest.raises(Exception) as e:
            FakeEntity()
        assert 'called _cache_fields' in str(e)

    def test_parse_kwargs_and_wont_validate_type(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab123'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()

        e = FakeEntity(f1='lol')
        assert e.f1 == 'lol'

    def test_parse_kwargs_and_raises_when_key_not_found_in_fields(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        with pytest.raises(KeyError) as e:
            FakeEntity(f1='lol', f2='haha')
        assert 'param not matches the schema: ' in str(e)

    def test_default_instance_attribute_is_None(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
            f2 = FloatField()
        s = FakeEntity()
        assert s.f1 is None
        assert s.f2 is None

    def test_different_instances_have_different_values(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
            f2 = FloatField()
        s = FakeEntity(f1=1.1)
        s2 = FakeEntity(f1=1.2)
        assert s.f1 == 1.1
        #assert s.f2 is None
        assert s2.f1 == 1.2
        #assert s2.f2 is None

    def test_status_attributes_defaults_values(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        s = FakeEntity(f1=1.1)
        assert s._saved_copy == {}
        assert s._is_new is True
        assert s._is_changed is False
        assert s._is_partial is False
        assert s._saved_etag is None


class Test__copy_into_saved:

    """test _copy_into_saved"""

    @pytest.fixture()
    def fake_entity_instance(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        s1 = FakeEntity(PartitionKey='p1', RowKey='r1', f1='f1')
        return s1

    def test_should_copy_current_fields(self,  fake_entity_instance):
        s1 = fake_entity_instance
        assert s1._saved_copy == {}
        assert s1.PartitionKey == 'p1'
        assert s1.RowKey == 'r1'
        assert s1.f1 == 'f1'
        s1._copy_into_saved()
        assert s1._saved_copy['PartitionKey'] == 'p1'
        assert s1._saved_copy['RowKey'] == 'r1'
        assert s1._saved_copy['f1'] == 'f1'

    def test_should_default_replace_current_saved(self, fake_entity_instance):
        s1 = fake_entity_instance
        assert s1._saved_copy == {}
        s1._saved_copy['f1'] = 'f2'
        s1._copy_into_saved()
        assert s1._saved_copy['PartitionKey'] == 'p1'
        assert s1._saved_copy['RowKey'] == 'r1'
        assert s1._saved_copy['f1'] == 'f1'


class Test__check_chagned:

    """test _check_chagned"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        return FakeEntity

    def test_should_return_True_if_field_not_in_saved_copy(self, fake_entity):
        s1 = fake_entity(PartitionKey='p1')
        assert s1._saved_copy == {}
        assert s1._check_chagned() is True

    def test_should_return_False_if_same(self, fake_entity):
        s1 = fake_entity(PartitionKey='p1')
        s1._copy_into_saved()
        assert s1._check_chagned() is False


class Test__populate_with_dict:

    """test _populate_with_dict"""

    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        return FakeEntity

    @pytest.fixture()
    def fake_json_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
            j1 = JSONField()

        return FakeEntity

    def test_raises_dic_not_dict(self, fake_entity):
        s1 = fake_entity()
        with pytest.raises(TypeError) as e:
            s1._populate_with_dict(dic='lo')
        assert 'dic is not a dict, ' in str(e)

    def test_raises_if_value_type_not_match(self, fake_entity):
        s1 = fake_entity()
        with pytest.raises(TypeError) as e:
            s1._populate_with_dict(dic={'f1': 'lol'})
        assert 'expect float for key f1, but got str' in str(e)

    def test_will_utc_localize_datetime_if_native(self):
        from datetime import datetime

        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            d1 = DateField()
        s1 = FakeEntity()
        s1._populate_with_dict(dic={
            'PartitionKey': 'p1',
            'RowKey': 'r1',
            'd1': datetime(2014, 1, 1, 0, 0, 0)
        })
        assert s1.d1.tzinfo is not None
        assert s1.d1.isoformat() == '2014-01-01T00:00:00+00:00'

    def test_only_populate_defined_field(self, fake_entity):
        s1 = fake_entity()
        s1._populate_with_dict(dic={'f1': 1.1, 'f2': 1.2})
        assert s1.f1 == 1.1
        with pytest.raises(AttributeError):
            s1.f2

    def test_require_serializing_type_not_match(self, fake_json_entity):
        s1 = fake_json_entity()
        with pytest.raises(TypeError) as e:
            s1._populate_with_dict(dic={'f1': 1.0, 'j1': {'lol': 123}})
        assert 'expect value to be str for deserialization' in str(e)

    def test_require_serializing_type_desrialize(self, fake_json_entity):
        s1 = fake_json_entity()
        json_raw = '{"a1": "normal string", "d1":"2014-01-01T01:01:01+00:00"}'
        s1._populate_with_dict(dic={'f1': 1.0, 'j1': json_raw})
        assert isinstance(s1.j1, dict)
        assert s1.j1['d1'].isoformat() == '2014-01-01T01:01:01+00:00'

    def test_populate_etag_to__saved_etag(self, fake_entity):
        s1 = fake_entity()
        s1._populate_with_dict(dic={'f1': 1.1, 'etag': 'lol'})
        assert s1.f1 == 1.1
        assert s1._saved_etag == 'lol'

    def test_is_partial_will_be_checked_if_dict_count_match_field_count(
            self, fake_entity):
        s1 = fake_entity()
        s1._populate_with_dict(dic={
            'RowKey': 'r1', 'PartitionKey': 'p1', 'f1': 1.1}, is_partial=True)
        assert s1._is_partial is False

    def test_will_change__is_new_to_False_if_etag(self, fake_entity):
        s1 = fake_entity()
        s2 = fake_entity()
        assert s1._is_new is True
        assert s2._is_new is True
        s1._populate_with_dict(dic={
            'RowKey': 'r1', 'PartitionKey': 'p1', 'f1': 1.1})
        assert s1._is_new is True
        s2._populate_with_dict(dic={
            'RowKey': 'r1', 'PartitionKey': 'p1', 'f1': 1.1, 'etag': 'lol'})
        assert s2._saved_etag == 'lol'
        assert s2._is_new is False

    def test_will_not_call__check_changed_if_etag_exist(self,
                                                        fake_entity,
                                                        monkeypatch):
        s1 = fake_entity()

        def fake_check_changed(*args, **kwargs):
            raise MemoryError('called fake_check_changed')
        monkeypatch.setattr(s1, '_check_chagned', fake_check_changed)
        s1._populate_with_dict(
            dic={'RowKey': 'r1',
                 'PartitionKey': 'p1',
                 'f1': 1.1, 'etag': 'lol'})

    def test_will_call__check_changed_if_etag_not_exist(self,
                                                        fake_entity,
                                                        monkeypatch):
        s1 = fake_entity()

        def fake_check_changed(*args, **kwargs):
            raise MemoryError('called fake_check_changed')
        monkeypatch.setattr(s1, '_check_chagned', fake_check_changed)
        with pytest.raises(MemoryError) as e:
            s1._populate_with_dict(
                dic={'RowKey': 'r1',
                     'PartitionKey': 'p1',
                     'f1': 1.1})
        assert 'called fake_check_changed' in str(e)

    def tset_will_not_call__copy_into_saved_if_not_etag(self,
                                                        fake_entity,
                                                        monkeypatch):
        s1 = fake_entity()

        def fake_copy_into_saved(*args, **kwargs):
            raise MemoryError('called fake_copy_into_saved')
        monkeypatch.setattr(s1, '_copy_into_saved', fake_copy_into_saved)
        s1._populate_with_dict(dic={
            'RowKey': 'r1', 'PartitionKey': 'p1', 'f1': 1.1})

    def test_will_call__copy_into_saved_if_etag_exist(self,
                                                      fake_entity,
                                                      monkeypatch):
        s1 = fake_entity()

        def fake_copy_into_saved(*args, **kwargs):
            raise MemoryError('called fake_copy_into_saved')
        monkeypatch.setattr(s1, '_copy_into_saved', fake_copy_into_saved)
        with pytest.raises(MemoryError) as e:
            s1._populate_with_dict(
                dic={'RowKey': 'r1',
                     'PartitionKey': 'p1',
                     'f1': 1.1, 'etag': 'lol'})
        assert 'called fake_copy_into_saved' in str(e)


class Test__to_dict:

    """test _to_dict"""

    def test_generate_dic(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
        s1 = FakeEntity()
        s1.PartitionKey = 'lol'
        s1.RowKey = 'haha'
        s1.f1 = 1.1
        dic = s1._to_dict()
        assert isinstance(dic, dict)
        assert len(dic) == 3
        assert dic['PartitionKey'] == s1.PartitionKey
        assert dic['RowKey'] == s1.RowKey
        assert dic['f1'] == s1.f1

    def test_wont_include_undefined_fields(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'
        s1 = FakeEntity()
        s1.PartitionKey = 'lol'
        s1.RowKey = 'haha'
        dic = s1._to_dict()
        assert isinstance(dic, dict)
        assert len(dic) == 2
        assert dic['PartitionKey'] == s1.PartitionKey
        assert dic['RowKey'] == s1.RowKey
        assert 'f1' not in dic

    @pytest.fixture()
    def fake_json_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()
            j1 = JSONField()

        return FakeEntity

    def test_require_serializing(self, fake_json_entity):
        from datetime import datetime, timezone
        j1 = {
            's1': 'lol',
            'd1': datetime(2014, 4, 1, 1, 1, 1, tzinfo=timezone.utc)
        }
        s1 = fake_json_entity(f1=1.0, j1=j1)
        dct = s1._to_dict()
        assert isinstance(dct['j1'], str)
        assert '"d1": "2014-04-01T01:01:01+00:00"' in dct['j1']

    def test_require_serializing_None(self, fake_json_entity):
        j1 = None
        s1 = fake_json_entity(f1=1.0, j1=j1)
        dct = s1._to_dict()
        assert dct['j1'] is None


class Test_inject_table_service:

    """test inject_table_service"""

    def test_inject_will_call_get_table_service(self, monkeypatch):
        class fake_ts:

                def create_table(*args, **kwargs):
                    pass

        def fake_get_table_service(*args, **kwargs):

            return fake_ts()

        monkeypatch.setattr(
            'AzureODM.Entity.get_table_service',
            fake_get_table_service)

        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'

            @Entity.inject_table_service
            def test_fn(self, ts=None):
                #assert ts == 'lol'
                return ts

        e = FakeEntity()
        a = e.test_fn()
        assert isinstance(a, fake_ts)

    def test_will_not_inject_if_ts_in_kwargs_and_not_None(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'

            @Entity.inject_table_service
            def test_fn(self, ts=None):
                #assert ts == 'lol'
                return ts

        e = FakeEntity()
        a = e.test_fn(ts='notlol')
        assert a == 'notlol'

    def test_will_inject_if_ts_in_kwargs_but_is_None(self, monkeypatch):
        class fake_ts:

                def create_table(*args, **kwargs):
                    pass

        def fake_get_table_service(*args, **kwargs):

            return fake_ts()

        monkeypatch.setattr(
            'AzureODM.Entity.get_table_service',
            fake_get_table_service)

        class FakeEntity(Entity):
            metas = {
                'table_name': 'ab14'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'

            @Entity.inject_table_service
            def test_fn(self, ts=None):
                #assert ts == 'lol'
                return ts

        e = FakeEntity()
        a = e.test_fn(ts=None)
        assert isinstance(a, fake_ts)


class Test_findOne:

    """test findOne"""

    def test_pass_params_to_get_entity(self):
        table_name = 'asdfsadf'
        partition_key = 'asdf'
        row_key = 'lol'
        select = 'RowKey'

        class fake_ts:

            def get_entity(*args, **kwargs):
                assert kwargs['table_name'] == table_name
                assert kwargs['partition_key'] == partition_key
                assert kwargs['row_key'] == row_key
                assert kwargs['select'] == select
                raise MemoryError('called get_entity')

        class FakeEntity(Entity):
            metas = {
                'table_name': table_name
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'

        #s = FakeEntity()
        with pytest.raises(MemoryError) as e:
            FakeEntity.findOne(partition_key=partition_key,
                               row_key=row_key, select=select, ts=fake_ts)
        assert 'called get_entity' in str(e)

    def test_raise_if_select_not_string(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'asdfsadf'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'
        with pytest.raises(TypeError) as e:
            FakeEntity.findOne(partition_key='p1',
                               row_key='r1',
                               select=None,
                               ts={})
        assert 'select is not a string' in str(e)

    def test_return_None_when_not_found(self):
        table_name = 'asdfsadf'
        partition_key = 'asdf'
        row_key = 'lol'
        select = 'RowKey'

        class fake_ts:

            def get_entity(*args, **kwargs):
                assert kwargs['table_name'] == table_name
                assert kwargs['partition_key'] == partition_key
                assert kwargs['row_key'] == row_key
                assert kwargs['select'] == select
                raise WindowsAzureMissingResourceError('called get_entity')

        class FakeEntity(Entity):
            metas = {
                'table_name': table_name
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'
        #s = FakeEntity()
        result = FakeEntity.findOne(partition_key=partition_key,
                                    row_key=row_key, select=select, ts=fake_ts)
        assert result is None

    def test_return_Entity_instance_when_found_populate_RK(self):
        table_name = 'asdfsadf'
        partition_key = 'asdf'
        row_key = 'lol'
        select = 'RowKey'

        class fake_ts:

            def get_entity(*args, **kwargs):
                assert kwargs['table_name'] == table_name
                assert kwargs['partition_key'] == partition_key
                assert kwargs['row_key'] == row_key
                assert kwargs['select'] == select
                return {
                    'RowKey': row_key + 'returned row_key'
                }

        class FakeEntity(Entity):
            metas = {
                'table_name': table_name
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'
        #s = FakeEntity()
        result = FakeEntity.findOne(partition_key=partition_key,
                                    row_key=row_key, select=select, ts=fake_ts)
        assert isinstance(result, FakeEntity)
        assert result.PartitionKey == partition_key
        assert result.RowKey == row_key + 'returned row_key'

    def test_return_Entity_instance_when_found_populate_PK(self):
        table_name = 'asdfsadf'
        partition_key = 'asdf'
        row_key = 'lol'
        select = 'PartitionKey'

        class fake_ts:

            def get_entity(*args, **kwargs):
                assert kwargs['table_name'] == table_name
                assert kwargs['partition_key'] == partition_key
                assert kwargs['row_key'] == row_key
                assert kwargs['select'] == select
                return {
                    'PartitionKey': partition_key + 'returned partition_key'
                }

        class FakeEntity(Entity):
            metas = {
                'table_name': table_name
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = 'f1lol'
        #s = FakeEntity()
        result = FakeEntity.findOne(partition_key=partition_key,
                                    row_key=row_key, select=select, ts=fake_ts)
        assert isinstance(result, FakeEntity)
        assert result.PartitionKey == partition_key + 'returned partition_key'
        assert result.RowKey == row_key


class Test_find:

    """test find()"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity

    def test_TypeError_raises(self, fake_entity):
        with pytest.raises(TypeError) as e:
            fake_entity.find(filter=123, ts={})
        assert 'filter has to be None or str, ' in str(e)

        with pytest.raises(TypeError) as e:
            fake_entity.find(select=123, ts={})
        assert 'select has to be None or str, ' in str(e)

        with pytest.raises(TypeError) as e:
            fake_entity.find(limit='123', ts={})
        assert 'limit has to be None or int, ' in str(e)

    def test_call_query_entities(self, fake_entity):
        filte = 'f'
        select = 'PartitionKey'
        limit = 1

        class fake_ts:

            def query_entities(*args, **kwargs):
                assert kwargs['table_name'] == fake_entity.metas['table_name']
                assert kwargs['filter'] == filte
                assert kwargs['select'] == select
                assert kwargs['top'] == limit
                raise MemoryError('called query_entities')
        with pytest.raises(MemoryError) as e:
            fake_entity.find(
                filter=filte, select=select, limit=limit, ts=fake_ts)
        assert 'called query_entities' in str(e)

    def test_return_empty_list_if_none_found(self, fake_entity):
        filte = 'f'
        select = 'PartitionKey'
        #limit = None

        class fake_ts:

            def query_entities(*args, **kwargs):
                assert kwargs['table_name'] == fake_entity.metas['table_name']
                assert kwargs['filter'] == filte
                assert kwargs['select'] == select
                assert kwargs['top'] is None
                return []

        entities = fake_entity.find(
            filter=filte, select=select, limit=None, ts=fake_ts)
        assert isinstance(entities, list)
        assert len(entities) == 0

    def test_return_Entity_list_if_found(self, fake_entity):
        filte = 'f'
        select = 'PartitionKey'
        partition_key = 'haha'
        #limit = None
        #fake_entity.PartitionKey = partition_key

        class fake_ts:

            def query_entities(*args, **kwargs):
                assert kwargs['table_name'] == fake_entity.metas['table_name']
                assert kwargs['filter'] == filte
                assert kwargs['select'] == select
                assert kwargs['top'] is None
                return [
                    {'PartitionKey': partition_key +
                        'returned partition_key1'},
                    {'PartitionKey': partition_key +
                        'returned partition_key2'},
                    {'PartitionKey': partition_key +
                     'returned partition_key3'}
                ]

        entities = fake_entity.find(
            filter=filte, select=select, limit=None, ts=fake_ts)
        assert isinstance(entities, list)
        assert len(entities) == 3
        for entity in entities:
            assert isinstance(entity, Entity), '{}'.format(entity)
        assert entities[0].PartitionKey == partition_key + \
            'returned partition_key1'
        assert entities[1].PartitionKey == partition_key + \
            'returned partition_key2'
        assert entities[2].PartitionKey == partition_key + \
            'returned partition_key3'


class Test__pre_save:

    """test _pre_save"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity()

    def fake_validate(*args, **kwargs):
        raise MemoryError('called fake_validate')

    def fake_validate_pass(*args, **kwargs):
        pass

    def fake_check_changed(*args, **kwargs):
        raise MemoryError('called fake_check_changed')

    def test_call__validate(self, fake_entity, monkeypatch):
        monkeypatch.setattr(fake_entity, '_validate', self.fake_validate)
        with pytest.raises(MemoryError) as e:
            fake_entity._pre_save()
        assert 'called fake_validate' in str(e)

    def test_call__check_changed(self, fake_entity, monkeypatch):
        monkeypatch.setattr(fake_entity, '_validate', self.fake_validate_pass)
        monkeypatch.setattr(fake_entity,
                            '_check_chagned',
                            self.fake_check_changed)
        with pytest.raises(MemoryError) as e:
            fake_entity._pre_save()
        assert 'called fake_check_changed' in str(e)


class Test_select:

    """test select()"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity

    def test_will_return_QuerySet_select(self, monkeypatch, fake_entity):
        en = fake_entity
        f = '*'

        def fake_select(self, entity, fields):
            assert entity is en
            assert fields == f
            raise MemoryError('called fake_select')

        monkeypatch.setattr(
            'AzureODM.QuerySet.QuerySet.select', fake_select)
        with pytest.raises(MemoryError) as e:
            en.select(fields=f)
        assert 'called fake_select' in str(e)


class Test_save_:

    """test save_insert, save_merge, save_replace, save_insert_or_replace,
    save_insert_or_merge"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f = FloatField()

        return FakeEntity(PartitionKey='p1', RowKey='r1', f=1.2)

    @pytest.fixture()
    def fake_ts(self):
        class TS:

            def insert_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['entity'] == {
                    'PartitionKey': 'p1',
                    'RowKey': 'r1',
                    'f': 1.2
                }
                raise MemoryError('called insert_entity')

            def insert_or_replace_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['partition_key'] == 'p1'
                assert kwargs['row_key'] == 'r1'
                assert kwargs['entity'] == {
                    'PartitionKey': 'p1',
                    'RowKey': 'r1',
                    'f': 1.2
                }
                raise MemoryError('called insert_or_replace_entity')

            def insert_or_merge_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['partition_key'] == 'p1'
                assert kwargs['row_key'] == 'r1'
                assert kwargs['entity'] == {
                    'PartitionKey': 'p1',
                    'RowKey': 'r1',
                    'f': 1.2
                }
                raise MemoryError('called insert_or_merge_entity')

            def merge_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['partition_key'] == 'p1'
                assert kwargs['row_key'] == 'r1'
                assert kwargs['entity'] == {
                    'PartitionKey': 'p1',
                    'RowKey': 'r1',
                    'f': 1.2
                }
                raise MemoryError('called merge_entity')

            def update_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['partition_key'] == 'p1'
                assert kwargs['row_key'] == 'r1'
                assert kwargs['entity'] == {
                    'PartitionKey': 'p1',
                    'RowKey': 'r1',
                    'f': 1.2
                }
                raise MemoryError('called update_entity')
        return TS()

    def test_save_insert(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.save_insert(ts=fake_ts)
        assert 'called insert_entity' in str(e)

    def test_save_insert_or_replace(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.save_insert_or_replace(ts=fake_ts)
        assert 'called insert_or_replace_entity' in str(e)

    def test_save_insert_or_merge(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.save_insert_or_merge(ts=fake_ts)
        assert 'called insert_or_merge_entity' in str(e)

    def test_save_merge(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.save_merge(ts=fake_ts)
        assert 'called merge_entity' in str(e)

    def test_save_replace(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.save_replace(ts=fake_ts)
        assert 'called update_entity' in str(e)


class Test_save:

    """test save"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity(PartitionKey='p1', RowKey='r1')

    @pytest.fixture()
    def entity_is_new(self, fake_entity):
        assert fake_entity._is_new is True
        return fake_entity

    @pytest.fixture()
    def entity_not_new(self, fake_entity):
        fake_entity._is_new = False
        fake_entity._is_changed = True
        fake_entity._saved_etag = 'fake etag'
        return fake_entity

    @pytest.fixture()
    def entity_not_new_not_changed(self, entity_not_new):
        assert entity_not_new._is_new is False
        entity_not_new._is_changed = False
        return entity_not_new

    @pytest.fixture()
    def entity_not_new_not_partial(self, entity_not_new):
        assert entity_not_new._is_new is False
        entity_not_new._is_partial = False
        return entity_not_new

    @pytest.fixture()
    def entity_not_new_partial(self, entity_not_new):
        assert entity_not_new._is_new is False
        entity_not_new._is_partial = True
        return entity_not_new

    def fake_pre_save(*args, **kwargs):
        raise MemoryError('called fake_pre_save')

    def fake_pre_save_pass(*args, **kwargs):
        return

    def fake_after_save(*args, **kwargs):
        assert 'etag' in kwargs['saved_entity_dict']
        raise MemoryError('called fake_after_save')

    def fake_save_insert(*args, **kwargs):
        if 'rt' in kwargs:
            def f(*args, **kwargs):
                return {'etag': 'fake_etag'}
            return f

        def f(*args, **kwargs):
            raise MemoryError('called fake_save_insert')
        return f

    def fake_save_insert_or_replace(*args, **kwargs):
        if 'rt' in kwargs:
            def f(*args, **kwargs):
                return {'etag': 'fake_etag'}
            return f

        def f(*args, **kwargs):
            raise MemoryError('called fake_save_insert_or_replace')
        return f

    def fake_save_insert_or_merge(*args, **kwargs):
        if 'rt' in kwargs:
            def f(*args, **kwargs):
                return {'etag': 'fake_etag'}
            return f

        def f(*args, **kwargs):
            raise MemoryError('called fake_save_insert_or_merge')
        return f

    def fake_save_replace(*args, **kwargs):
        if 'rt' in kwargs:
            def f(*args, **kwargs):
                return {'etag': 'fake_etag'}
            return f

        def f(*args, **kwargs):
            raise MemoryError('called fake_save_replace')
        return f

    def fake_save_merge(*args, **kwargs):
        if 'rt' in kwargs:
            def f(*args, **kwargs):
                return {'etag': 'fake_etag'}
            return f

        def f(*args, **kwargs):
            raise MemoryError('called fake_save_merge')
        return f

    def test_should_call__pre_save_first(self, fake_entity,
                                         entity_is_new,
                                         entity_not_new,
                                         entity_not_new_not_changed,
                                         entity_not_new_not_partial,
                                         entity_not_new_partial, monkeypatch):
        entities = [entity_is_new,
                    entity_not_new,
                    entity_not_new_not_changed,
                    entity_not_new_not_partial,
                    entity_not_new_partial]
        for entity in entities:
            monkeypatch.setattr(entity, '_pre_save', self.fake_pre_save)
            with pytest.raises(MemoryError) as e:
                entity.save()
            assert 'called fake_pre_save' in str(e)

    def test_call_save_insert_or_merge_if_new_and_force_merge(self,
                                                              monkeypatch,
                                                              entity_is_new):
        monkeypatch.setattr(
            entity_is_new, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_is_new,
            'save_insert_or_merge',
            self.fake_save_insert_or_merge())
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_merge=True)
        assert 'called fake_save_insert_or_merge' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_is_new,
            'save_insert_or_merge',
            self.fake_save_insert_or_merge(rt=True))
        monkeypatch.setattr(
            entity_is_new,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_merge=True)
        assert 'called fake_after_save' in str(e)

    def test_call_save_insert_or_replace_if_new_and_force_replace(self,
                                                                  monkeypatch,
                                                                  entity_is_new):
        monkeypatch.setattr(
            entity_is_new, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_is_new,
            'save_insert_or_replace',
            self.fake_save_insert_or_replace())
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_replace=True)
        assert 'called fake_save_insert_or_replace' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_is_new,
            'save_insert_or_replace',
            self.fake_save_insert_or_replace(rt=True))
        monkeypatch.setattr(
            entity_is_new,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_replace=True)
        assert 'called fake_after_save' in str(e)

    def test_call_save_insert_if_new_not_force_merge_not_force_replace(self,
                                                                       monkeypatch,
                                                                       entity_is_new):
        monkeypatch.setattr(
            entity_is_new, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_is_new, 'save_insert', self.fake_save_insert())
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_replace=False, force_merge=False)
        assert 'called fake_save_insert' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_is_new,
            'save_insert',
            self.fake_save_insert(rt=True))
        monkeypatch.setattr(
            entity_is_new,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_is_new.save(force_replace=False, force_merge=False)
        assert 'called fake_after_save' in str(e)

    def test_call_save_insert_can_ignore_conflict(self,
                                                  monkeypatch,
                                                  entity_is_new):
        from azure import WindowsAzureConflictError
        monkeypatch.setattr(
            entity_is_new, '_pre_save', self.fake_pre_save_pass)

        def fake_save_insert(*args, **kwargs):
            raise WindowsAzureConflictError('called fake_save_insert')
        monkeypatch.setattr(
            entity_is_new, 'save_insert', fake_save_insert)
        with pytest.raises(WindowsAzureConflictError) as e:
            entity_is_new.save(force_replace=False, force_merge=False)
        assert 'called fake_save_insert' in str(e)
        a = entity_is_new.save(ignore_conflict=True)
        assert a is False

    def test_call_save_insert_if_new_default(self, entity_is_new, monkeypatch):
        monkeypatch.setattr(
            entity_is_new, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_is_new, 'save_insert', self.fake_save_insert())
        with pytest.raises(MemoryError) as e:
            entity_is_new.save()
        assert 'called fake_save_insert' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_is_new,
            'save_insert',
            self.fake_save_insert(rt=True))
        monkeypatch.setattr(
            entity_is_new,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_is_new.save()
        assert 'called fake_after_save' in str(e)

    def test_return_False_if_not_new_not_changed_not_force_save(self,
                                                                entity_not_new_not_changed,
                                                                monkeypatch):
        monkeypatch.setattr(
            entity_not_new_not_changed, '_pre_save', self.fake_pre_save_pass)
        #monkeypatch.setattr(entity_not_new_not_changed, 'save_insert', self.fake_save_insert)
        assert entity_not_new_not_changed.save(force_save=False) is False

    def test_call_save_replace_if_not_new_is_partial_force_replace(self,
                                                                   entity_not_new_partial,
                                                                   monkeypatch):
        monkeypatch.setattr(
            entity_not_new_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_partial, 'save_replace', self.fake_save_replace())
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_replace=True)
        assert 'called fake_save_replace' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_partial,
            'save_replace',
            self.fake_save_replace(rt=True))
        monkeypatch.setattr(
            entity_not_new_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_replace=True)
        assert 'called fake_after_save' in str(e)

    def test_call_save_merge_if_not_new_is_partial_not_force_replace(self,
                                                                     entity_not_new_partial,
                                                                     monkeypatch):
        monkeypatch.setattr(
            entity_not_new_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_partial, 'save_merge', self.fake_save_merge())
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_replace=False)
        assert 'called fake_save_merge' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_partial,
            'save_merge',
            self.fake_save_merge(rt=True))
        monkeypatch.setattr(
            entity_not_new_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_replace=False)
        assert 'called fake_after_save' in str(e)

    def test_call_save_merge_if_not_new_is_partial_default(self,
                                                           entity_not_new_partial,
                                                           monkeypatch):
        monkeypatch.setattr(
            entity_not_new_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_partial, 'save_merge', self.fake_save_merge())
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save()
        assert 'called fake_save_merge' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_partial,
            'save_merge',
            self.fake_save_merge(rt=True))
        monkeypatch.setattr(
            entity_not_new_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save()
        assert 'called fake_after_save' in str(e)

    def test_call_save_merge_if_not_new_not_partial_force_merge(self,
                                                                entity_not_new_partial,
                                                                monkeypatch):
        monkeypatch.setattr(
            entity_not_new_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_partial, 'save_merge', self.fake_save_merge())
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_merge=True)
        assert 'called fake_save_merge' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_partial,
            'save_merge',
            self.fake_save_merge(rt=True))
        monkeypatch.setattr(
            entity_not_new_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_partial.save(force_merge=True)
        assert 'called fake_after_save' in str(e)

    def test_call_save_replace_if_not_new_not_partial_not_force_merge(self,
                                                                      entity_not_new_not_partial,
                                                                      monkeypatch):
        monkeypatch.setattr(
            entity_not_new_not_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_not_partial, 'save_replace', self.fake_save_replace())
        with pytest.raises(MemoryError) as e:
            entity_not_new_not_partial.save(force_merge=False)
        assert 'called fake_save_replace' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_not_partial,
            'save_replace',
            self.fake_save_replace(rt=True))
        monkeypatch.setattr(
            entity_not_new_not_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_not_partial.save(force_merge=False)
        assert 'called fake_after_save' in str(e)

    def test_call_save_replace_if_not_new_not_partial_default(self,
                                                              entity_not_new_not_partial,
                                                              monkeypatch):
        monkeypatch.setattr(
            entity_not_new_not_partial, '_pre_save', self.fake_pre_save_pass)
        monkeypatch.setattr(
            entity_not_new_not_partial, 'save_replace', self.fake_save_replace())
        with pytest.raises(MemoryError) as e:
            entity_not_new_not_partial.save()
        assert 'called fake_save_replace' in str(e)

        # test call after save
        monkeypatch.setattr(
            entity_not_new_not_partial,
            'save_replace',
            self.fake_save_replace(rt=True))
        monkeypatch.setattr(
            entity_not_new_not_partial,
            '_after_save',
            self.fake_after_save)
        with pytest.raises(MemoryError) as e:
            entity_not_new_not_partial.save()
        assert 'called fake_after_save' in str(e)


class Test__after_save:

    """test _after_save"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()

        return FakeEntity(PartitionKey='p1', RowKey='r1', f1=1.1)

    def test_raises_if_dic_wrong_type(self, fake_entity):
        with pytest.raises(TypeError) as e:
            fake_entity._after_save(saved_entity_dict='lol')
        assert 'saved_entity_dict is not a dict, ' in str(e)

    def test_raises_if_etag_not_in_dict(self, fake_entity):
        with pytest.raises(KeyError) as e:
            fake_entity._after_save(saved_entity_dict={'lol': 'lolol'})
        assert 'etag is not in : ' in str(e)

    def test_should_change_values_and_status_etag(self, fake_entity):
        fake_entity._is_new = True
        fake_entity._is_changed = True
        fake_entity._saved_etag = 'ads'
        fake_entity._after_save(saved_entity_dict={
            'etag': 'new_etag'
        })
        assert fake_entity._is_changed is False
        assert fake_entity._is_new is False
        assert fake_entity._saved_etag == 'new_etag'

    def test_should_change_values_and_status_full(self, fake_entity):
        fake_entity._is_new = True
        fake_entity._is_changed = True
        fake_entity._saved_etag = 'ads'
        fake_entity._after_save(saved_entity_dict={
            'PartitionKey': 'p1',
            'RowKey': 'r1',
            'f1': 1.3,
            'etag': 'new_etag'
        })
        assert fake_entity._is_changed is False
        assert fake_entity._is_new is False
        assert fake_entity._saved_etag == 'new_etag'
        assert fake_entity._saved_copy['f1'] == 1.3

    def test_should_return_True_if_good(self, fake_entity):
        fake_entity._is_new = True
        fake_entity._is_changed = True
        fake_entity._saved_etag = 'ads'
        r = fake_entity._after_save(saved_entity_dict={
            'PartitionKey': 'p1',
            'RowKey': 'r1',
            'f1': 1.3,
            'etag': 'new_etag'
        })
        assert r is True


class Test_delete:

    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()

        return FakeEntity(PartitionKey='p1', RowKey='r1', f1=1.1)
    """test delete"""
    @pytest.fixture()
    def fake_ts(self):
        class TS:

            def delete_entity(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['partition_key'] == 'p1'
                assert kwargs['row_key'] == 'r1'
                raise MemoryError('called delete_entity')

        return TS()

    def test_raise_if_PartitionKey_invalid(self, fake_entity, fake_ts):
        fake_entity.PartitionKey = None
        with pytest.raises(TypeError) as e:
            fake_entity.delete(ts=fake_ts)
        assert 'PartitionKey is not a string, ' in str(e)

    def test_raise_if_RowKey_invalid(self, fake_entity, fake_ts):
        fake_entity.RowKey = None
        with pytest.raises(TypeError) as e:
            fake_entity.delete(ts=fake_ts)
        assert 'RowKey is not a string, ' in str(e)

    def test_raise_if_delete_none_saved(self, fake_entity, fake_ts):
        with pytest.raises(Exception) as e:
            fake_entity.delete(ts=fake_ts)
        assert 'trying to delete a none saved entity,' in str(e)

    def test_able_to_force_delete_none_saved(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.delete(force_delete=True, ts=fake_ts)
        assert 'called delete_entity' in str(e)


class Test_drop_table:

    """test drop_table"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()

        return FakeEntity
    """test delete"""
    @pytest.fixture()
    def fake_ts(self):
        class TS:

            def delete_table(*args, **kwargs):
                assert kwargs['table_name'] == 'lolol'
                assert kwargs['fail_not_exist'] == 'haha'
                raise MemoryError('called delete_table')

        return TS()

    def test_call_delete_table(self, fake_entity, fake_ts):
        with pytest.raises(MemoryError) as e:
            fake_entity.drop_table(fail_not_exist='haha', ts=fake_ts)
        assert 'called delete_table' in str(e)


class Test__create_table:

    """test _create_table"""
    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()
            f1 = FloatField()

        return FakeEntity

    def test_call_ts_create_table(self, fake_entity):
        class fake_ts:

            def create_table(*args, **kwargs):
                return True

        assert fake_entity._created_table is False
        fake_entity._create_table(ts=fake_ts)
        assert fake_entity._created_table is True

    def test_wont_call_if_created_table_True(self, monkeypatch, fake_entity):
        class fake_ts:

            def create_table(*args, **kwargs):
                pass

        fake_entity._created_table = True
        fake_entity._create_table(ts=fake_ts)
