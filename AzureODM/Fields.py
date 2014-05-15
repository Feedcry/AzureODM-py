"""
    AzureTableODM.Fields
"""
from datetime import datetime
import simplejson as json
from . import _datetime_json_encoder, _datetime_json_object_hook


class GenericField(object):

    def __init__(self,
                 _type,
                 required=False,
                 require_serializing=False,
                 serialized_type=None):
        """The Filed Abstract Class or the most basic Field

        :param type type: the type of the field's value
        :param bool required: whether this field is required
        :param bool require_serializing: if this is true, when retrieve
            value of this field, it will go through deserializing by calling
            :func:`deserialize`, when store value into this field,
            it will require serializing by calling :func:`serialize`
        """
        #: the type
        self._type = _type
        #: whether it's required
        self.required = required
        #: whether need to be serialized
        self.require_serializing = require_serializing
        #: what the stored type after serialized
        self.serialized_type = serialized_type

    @classmethod
    def serialize(self, value):
        """The serializer function

        :param value:
        :returns: serialized value
        """
        return value

    @classmethod
    def deserialize(self, value):
        """The deserializer function

        :param value:
        :returns: deserialized object
        """
        return value


class KeyField(GenericField):

    _type = str
    require_serializing = False
    serialized_type = None
    required = True

    def __init__(self):
        pass


class StringField(GenericField):
    require_serializing = False
    serialized_type = None
    _type = str

    def __init__(self, required=False):
        self.required = required


class FloatField(GenericField):
    require_serializing = False
    serialized_type = None
    _type = float

    def __init__(self, required=False):
        self.required = required


class IntField(GenericField):
    require_serializing = False
    serialized_type = None
    _type = int

    def __init__(self, required=False):
        self.required = required


class DateField(GenericField):
    require_serializing = False
    serialized_type = None
    _type = datetime

    def __init__(self, required=False):
        self.required = required


class BooleanField(GenericField):
    require_serializing = False
    serialized_type = None
    _type = bool

    def __init__(self, required=False):
        self.required = required


class JSONField(GenericField):

    """This is a field that store content in JSON

    The type of this field is a Python Object (List, or Dict),
    when store it into the database, it need be converted into
    JSON by calling :func:`serialize`. When retrieve it, it will be converted
    back into Python Object by calling: :func:`deserialize`
    """
    require_serializing = True
    serialized_type = str
    _type = (list, dict)
    keep_Null = False

    def __init__(self, required=False, keep_Null=False):
        self.required = required
        self.keep_Null = keep_Null

    @classmethod
    def serialize(self, value):
        if value is None and self.keep_Null is False:
            return None
        return json.dumps(value, default=_datetime_json_encoder)

    @classmethod
    def deserialize(self, value):
        if self.keep_Null is False and value is None:
            return None
        result = json.loads(value, object_hook=_datetime_json_object_hook)
        assert result is None or isinstance(result, self._type)
        return result
