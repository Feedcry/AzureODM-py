"""
    test_QuerySet
"""
from AzureODM.QuerySet import (
    QuerySet, obj_to_query_value, Q, QCombination, AndOperator, OrOperator)
from AzureODM.Entity import Entity
from AzureODM.Fields import GenericField, FloatField, KeyField
import pytest
import re
from datetime import datetime, timezone
regex = re.compile(QuerySet.cmp_exp)


class Test_QCombination:

    """test QCombination"""

    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity

    def test_q1_q_q2_q(self):
        q1 = Q(PartitionKey='p1')
        q2 = Q(RowKey='r1')
        a = QCombination(AndOperator, q1, q2)
        assert a.subquires[0].k == 'PartitionKey'
        assert a.subquires[0].v == 'p1'
        assert a.subquires[1].__name__ == 'AndOperator'
        assert a.subquires[2].k == 'RowKey'
        assert a.subquires[2].v == 'r1'

    def test_q1_qcom_q2_q(self):
        q1 = Q(PartitionKey='p1')
        q2 = Q(RowKey='r1')
        qc = QCombination(AndOperator, q1, q2)
        assert len(qc.subquires) == 3
        a = QCombination(AndOperator, qc, Q(Temp='lol'))
        print('---------{}'.format(a.subquires))
        assert len(a.subquires) == 5

    def test_q1_qcom_q2_qcom(self):
        q1 = Q(PartitionKey='p1')
        q2 = Q(RowKey='r1')
        q3 = Q(PartitionKey='p1')
        q4 = Q(RowKey='r1')
        qc = QCombination(AndOperator, q1, q2)
        qc2 = QCombination(AndOperator, q3, q4)
        a = QCombination(OrOperator, qc, qc2)
        assert len(a.subquires) == 7
        assert a.subquires[1] is AndOperator
        assert a.subquires[3] is OrOperator
        assert a.subquires[5] is AndOperator

    def test_compile(self, fake_entity):
        a = Q(PartitionKey='p1') | Q(RowKey='r1') & Q(RowKey='r3')
        qs = a.compile(entity=fake_entity)
        expected = "(PartitionKey eq 'p1' or RowKey eq 'r1' and RowKey eq 'r3')"
        assert expected == qs


class Test_Q:

    """Test q"""

    def test_two_combine_and(self):
        a = (Q(PartitionKey='p1') & Q(RowKey='r1'))
        assert isinstance(a, QCombination)
        assert len(a.subquires) == 3
        assert a.subquires[0].k == 'PartitionKey'
        assert a.subquires[0].v == 'p1'
        assert a.subquires[1].__name__ == 'AndOperator'
        assert a.subquires[2].k == 'RowKey'
        assert a.subquires[2].v == 'r1'

    def test_two_combine_or(self):
        a = (Q(PartitionKey='p1') | Q(RowKey='r1'))
        assert isinstance(a, QCombination)
        assert len(a.subquires) == 3
        assert a.subquires[0].k == 'PartitionKey'
        assert a.subquires[0].v == 'p1'
        assert a.subquires[1].__name__ == 'OrOperator'
        assert a.subquires[2].k == 'RowKey'
        assert a.subquires[2].v == 'r1'

    def test_three_combine_and_or(self):
        a = (Q(PartitionKey='p1') | Q(RowKey='r1') & Q(RowKey='r3'))
        assert isinstance(a, QCombination)
        assert len(a.subquires) == 5
        assert a.subquires[0].k == 'PartitionKey'
        assert a.subquires[0].v == 'p1'
        assert a.subquires[1].__name__ == 'OrOperator'
        assert a.subquires[2].k == 'RowKey'
        assert a.subquires[2].v == 'r1'

    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity

    def test_in_query(self, fake_entity):
        a = Q(RowKey__in=[123, 124, '412'])
        query_string = a.compile(entity=fake_entity)
        expected = "(RowKey eq 123 or RowKey eq 124 or RowKey eq '412')"
        assert query_string == expected


class Test_obj_to_query_value:

    """test obj_to_query_value"""
    @pytest.mark.parametrize('obj,expected', [
        ('thisisgood', "'thisisgood'"),
        (123456, '123456'),
        (123.456, '123.456'),
        (True, 'true'),
        (False, 'false'),
        (datetime(2014, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
         "datetime'2014-01-01T00:00:00Z'")
    ])
    def test_obj_conversion(self, obj, expected):
        assert obj_to_query_value(obj) == expected

    def test_raises_ValueError_if_datetime_is_naive(self):
        with pytest.raises(ValueError) as e:
            obj_to_query_value(datetime(2014, 1, 1, 1, 1, 1, 1))
        assert 'only timezone awared datetime is accpeted' in str(e)

    def test_type_not_match(self):
        with pytest.raises(Exception) as e:
            obj_to_query_value(None)
        assert 'obj type is unknown, ' in str(e)


class Test_q_regexp:

    """test q_regexp"""

    @pytest.mark.parametrize('query_string,expected_field,expected_operator', [
        ('thisisgood', 'thisisgood', None),
        ('thisisgood__gt', 'thisisgood', 'gt'),
        ('thisisgood__ge', 'thisisgood', 'ge'),
        ('thisisgood__lt', 'thisisgood', 'lt'),
        ('thisisgood__le', 'thisisgood', 'le'),
        ('thisisgood__ne', 'thisisgood', 'ne')
    ])
    def test_regexp(self, query_string, expected_field, expected_operator):
        m = regex.match(query_string)
        assert m is not None
        assert m.group('field') == expected_field
        if expected_operator is not None:
            assert m.group('operator') == expected_operator
        else:
            assert m.group('operator') is None

    @pytest.mark.parametrize('query_string', [
        '',
        '__gt',
        '__',
        '_asdf__gt',
        '.'
    ])
    def test_should_not_match(self, query_string):
        m = regex.match(query_string)
        assert m is None


class Test_query_parser:

    """test query_parser"""

    @pytest.fixture()
    def fake_entity(self):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        return FakeEntity

    @pytest.fixture()
    def fake_entity_selected(self, fake_entity):

        a = fake_entity.select(fields=None)
        return a

    def test_query_parser_wrap_where(self, fake_entity_selected):

        a = fake_entity_selected.where(PartitionKey='lol')
        expected = "PartitionKey eq 'lol'"
        assert a.filter == expected
        a.andWhere(RowKey__gt='r1')
        expected += " and RowKey gt 'r1'"
        assert a.filter == expected
        a.orWhere(RowKey__ge='o1')
        expected += " or RowKey ge 'o1'"
        assert a.filter == expected
        a.notWhere(RowKey__ne=True)
        expected += " not RowKey ne true"
        assert a.filter == expected

    def test_query_parser_wrap_raises_when_args(self, fake_entity_selected):
        a = fake_entity_selected
        with pytest.raises(Exception) as e:
            a.where('lol')
        assert 'you cannot put args into query function' in str(e)

    def test_raises_when_more_than_one_kwargs(self, fake_entity_selected):
        a = fake_entity_selected
        with pytest.raises(Exception) as e:
            a.where(RowKey='lol', PartitionKey='lll')
        assert 'you cannot put more than one args into query function' in str(
            e)

    def test_raises_when_no_target_entity(self, fake_entity_selected):
        a = fake_entity_selected
        a._targeted_entity = None
        with pytest.raises(Exception) as e:
            a.where(RowKey='lol')
        assert 'please call select before using query' in str(e)

    def test_raises_when_value_is_None(self, fake_entity_selected):
        with pytest.raises(Exception) as e:
            fake_entity_selected.where(RowKey=None)
        assert 'comparison value cannot be None' in str(e)

    def test_raises_when_query_is_not_valid(self, fake_entity_selected):
        with pytest.raises(ValueError) as e:
            fake_entity_selected.where(RowKey__xx='lol')
        assert 'is not a valid query' in str(e)

    def test_raises_when_field_is_not_defined(self, fake_entity_selected):
        with pytest.raises(KeyError) as e:
            fake_entity_selected.where(XXXX__lt='lol')
        assert 'field is not defined, ' in str(e)

    def test_limit_raises_if_not_None_or_int(self, fake_entity_selected):
        with pytest.raises(TypeError) as e:
            fake_entity_selected.limit(limit='lol')
        assert 'limit is not an int, ' in str(e)

    def test_limit_will_set__limit_attr(self, fake_entity_selected):
        fake_entity_selected.limit(limit=123)
        assert fake_entity_selected._limit == 123
        fake_entity_selected.limit(limit=None)
        assert fake_entity_selected._limit is None


class Test_select:

    """test select"""

    def test_raise_if_entity_not_Entity(self):
        q = QuerySet()
        with pytest.raises(TypeError):
            q.select(entity={}, fields=None)

    def test_raise_if_fields_not_None_not_list_not_star(self):
        q = QuerySet()
        with pytest.raises(TypeError) as e:
            q.select(entity=Entity, fields=123)
        assert 'fields can only be list or None' in str(e)

    def test_stringify_fields_for_select(self):
        q = QuerySet()
        q.select(entity=Entity, fields=['PartitionKey', 'RowKey', 'f1'])
        assert q._select == 'PartitionKey,RowKey,f1'

    def test_stringify_fields_None_for_select(self):
        q = QuerySet()
        q.select(entity=Entity, fields=None)
        assert q._select == '*'

    def test_fields_accept_star(self):
        q = QuerySet()
        q.select(entity=Entity, fields='*')
        assert q._select == '*'

    def test_added_PartitionKey_RowKey_to_Fields_if_not_existed(self):
        q = QuerySet()
        q.select(entity=Entity, fields=['f1'])
        assert q._select == 'f1,PartitionKey,RowKey'

    def test_save_values_to_attributes(self):
        q = QuerySet()
        q.select(entity=Entity, fields=['PartitionKey', 'RowKey', 'f1'])
        assert q._targeted_entity == Entity


class Test_go:

    """test select"""

    def test_raise_if__target_entity_is_None(self):
        q = QuerySet()
        with pytest.raises(Exception) as e:
            q.go()
        assert 'you must call select before call go' in str(e)

    def test_call_and_return__targeted_entity_find(self, monkeypatch):
        class FakeEntity(Entity):
            metas = {
                'table_name': 'lolol'
            }
            PartitionKey = KeyField()
            RowKey = KeyField()

        def fake_find(*args, **kwargs):
            assert kwargs['filter'] == "PartitionKey eq 'lol'"
            assert kwargs['select'] == 'PartitionKey,RowKey,f1'
            assert kwargs['limit'] == 10
            raise MemoryError('called fake_find')
        monkeypatch.setattr(FakeEntity, 'find', fake_find)
        q = QuerySet()
        q.select(entity=FakeEntity, fields=['PartitionKey', 'RowKey', 'f1'])
        q.where(PartitionKey='lol').limit(10)
        with pytest.raises(MemoryError) as e:
            q.go()
        assert 'called fake_find' in str(e)
