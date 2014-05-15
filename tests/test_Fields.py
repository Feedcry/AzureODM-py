"""
    test_Fields
"""
import pytest
from AzureODM.Fields import *


class Test_GenericField:

    """test GenericField"""

    def test_type_is_required(self):
        with pytest.raises(TypeError) as e:
            GenericField()
        assert '_type' in str(e)

    def test_default_values(self):
        f = GenericField(_type=str)
        assert f.required is False
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_KeyField:

    """Test KeyField"""

    def test_type_is_str(self):
        f = KeyField()
        assert f._type.__name__ == 'str'
        assert f.required is True
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_StringField:

    """test StringField"""

    def test_type_is_str(self):
        f = StringField()
        assert f._type.__name__ == 'str'
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_FloatField:

    """test FloatField"""

    def test_type_is_float(self):
        f = FloatField()
        assert f._type.__name__ == 'float'
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_IntField:

    """test IntField"""

    def test_type_is_int(self):
        f = IntField()
        assert f._type.__name__ == 'int'
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_BooleanField:

    """test BooleanField"""

    def test_type_is_bool(self):
        f = BooleanField()
        assert f._type.__name__ == 'bool'
        assert f.require_serializing is False
        assert f.serialized_type is None


class Test_DateField:

    """test DateField"""

    def test_type_is_datetime(self):
        f = DateField()
        assert f._type.__name__ == 'datetime'
        assert f.require_serializing is False
        assert f.serialized_type is None

    def test_not_accept_date(self):
        from datetime import date, datetime
        f = DateField()
        assert isinstance(datetime(1999, 1, 1, 1, 1, 1), f._type)
        assert not isinstance(date(1999, 1, 1), f._type)


class Test_JSONField:

    """test JSONField"""

    def test_type_is_list_dict(self):
        f = JSONField()
        assert f._type == (list, dict)

    def test_require_serializing_True(self):
        f = JSONField()
        assert f.require_serializing is True
        assert f.serialized_type is str

    def test_serialize_deserialize_None(self):
        s = JSONField.serialize(value=None)
        assert s is None

        s = 'null'
        j = JSONField.deserialize(value=s)
        assert j is None

        j = JSONField.deserialize(value=None)
        assert j is None

    def test_serialize_list_with_dict_datetime(self):
        import datetime
        l = [
            {
                'a1': 'normal string',
                'dt1': datetime.datetime(2014, 1, 1, 1, 1, 1,
                                         tzinfo=datetime.timezone.utc),
                'd1': datetime.date(2014, 1, 1)
            },
            {
                'a2': 'normal string 2',
                'dt2': datetime.datetime(2014, 1, 1, 1, 1, 2,
                                         tzinfo=datetime.timezone.utc),
                'd2': datetime.date(2014, 1, 2)
            }
        ]

        j = JSONField.serialize(value=l)
        assert JSONField.deserialize(value=j) == l
