"""
    AzureTableODM.Entity

    The table entity superclass
"""
from .Fields import GenericField
from .QuerySet import QuerySet
from re import compile as re_compile
from re import IGNORECASE as re_IGNORECASE
from functools import wraps
from .Service import get_table_service
from azure.storage import Entity as AzureTableEntity
from azure import WindowsAzureMissingResourceError, WindowsAzureConflictError
from datetime import datetime, timezone


class Entity:

    """
        Entity superclass
    """
    metas = {
        'table_name': None
    }
    _table_name_re = re_compile('^[a-z][a-z|0-9]*$', re_IGNORECASE)
    _created_table = False

    def __init__(self, *args, **kwargs):
        """

        :raises AttributeError: if ``table_name`` not in :attr:`metas`
        :raises ValueError: if :attr:`metas.table_name` not alphnum and starts
            with [a-z]
        :raises KeyError: if ``kwargs`` has undefined key
        """

        if not isinstance(self.metas['table_name'], str):
            raise AttributeError('no table_name meta provided')
        if not self._table_name_re.search(self.metas['table_name']):
            raise ValueError('table_name is not in valid format, {}'.format(
                self.metas['table_name']))

        #: indicate whether the entity is new (haven't been saved to Azure)
        self._is_new = True
        #: whether any attributes changed comparing to :attr:`_saved_copy`
        self._is_changed = False
        #: whether the document contains only a subset of fileds (select used)
        self._is_partial = False
        #: when populate the entity, the original copy is saved here
        self._saved_copy = {}
        #: saved_etag, store the etag meta from the server
        self._saved_etag = None

        self._cache_fields()
        # taking values from ``kwargs``
        k_copy = kwargs.copy()
        for field in self._f:
            if field in k_copy.keys():
                self.__dict__[field] = k_copy[field]
                del k_copy[field]

        # if there are keys in ``kwargs`` not match the field
        if len(k_copy) > 0:
            raise KeyError('param not matches the schema: {}'.format(kwargs))

    def _cache_fields(self, re_cache=False):
        """create instance attribute """
        if getattr(self.__class__, '_f', None) is None or re_cache is True:
            setattr(self.__class__, '_f', {})
            for field in filter(
                lambda x:  isinstance(
                    self.__class__.__dict__[x], GenericField),
                    self.__class__.__dict__):
                self.__class__._f[field] = self.__class__.__dict__[field]
            # require ``PartitionKey`` and ``RowKey``
            if 'PartitionKey' not in self.__class__._f or \
                    self.__class__._f['PartitionKey']._type.__name__ != 'str':
                raise AttributeError('a StringField PartitionKey is required')
            if 'RowKey' not in self.__class__._f or \
                    self.__class__._f['RowKey']._type.__name__ != 'str':
                raise AttributeError('a StringField RowKey is required')
        for field in self.__class__._f:
            self.__dict__[field] = None

    def _validate(self):
        """
        :raises ValueError: if required field is None
            when :attr:`_is_partial` is True, when :attr:`_is_partial` is
            False, only ``PartitionKey`` and ``RowKey`` is required
        :raises TypeError: if field value doesn't match the type
        """
        for field in self._f:
            if self.__dict__[field] is None:
                if (self.__class__.__dict__[field].required is True):
                    if self._is_partial is False or \
                            field in ['PartitionKey', 'RowKey']:
                        raise ValueError('{} is required'.format(field))
            else:
                if not isinstance(self.__dict__[field], self._f[field]._type):
                    raise TypeError('{} is not type: {}'.format(
                        field, self._f[field]._type.__name__))

    def _copy_into_saved(self):
        """
        copy the current entity values into :attr:`_saved_copy`

        This should be called after:

        * :func:`_populate_with_dict`
        * :func:`save`
        """
        #self._saved_copy = {}
        for field in self._f:
            self._saved_copy[field] = self.__dict__[field]

    def _check_chagned(self):
        """
        compare the current values with :attr:`_saved_copy`

        :returns: True if any one field is different
        :returns: False if none of the field is different
        """
        for field in self._f:
            if not field in self._saved_copy \
                    or self._saved_copy[field] != self.__dict__[field]:
                return True
        return False

    def _populate_with_dict(self, dic, is_partial=False):
        """
        populte attributes with a dictionary contains key-value pairs

        Will only populate exist fields, none-exist fields will be ignored

        will convert datetime object to UTC aware ``datetime``

        :param dict dic: can also be :class:`AzureTableEntity`
        :param bool is_partial: whether the ``dic`` is only a partial of
            the entity (return by using ``select``) default is ``False``,
            if the number of valid field value in ``dic`` equals the length
            of :attr:`_f`, we will ignore the ``is_partial`` and set it to
            ``False``
        :raises TypeError: if ``dic`` is not a ``dict`` or
            ``azure.storage.Entity``
        :raises TypeError: if the ``key`` in dict doesn't match the type
            of ``field`` 's type
        """
        if not isinstance(dic, (dict, AzureTableEntity)):
            raise TypeError('dic is not a dict, {}'.format(dic))
        if isinstance(dic, AzureTableEntity):
            dic = dic.__dict__
        valid_fields_count = 0
        etagged = False
        for key, value in dic.items():
            if key in self._f:
                # handle require_serializing
                if self._f[key].require_serializing:
                    if isinstance(value, self._f[key].serialized_type):
                        self.__dict__[key] = self._f[key].deserialize(value)
                    else:
                        msg = 'expect value to be {} for deserialization,' +\
                            ' but got {}'
                        raise TypeError(
                            msg.format(self._f[key].serialized_type.__name__,
                                       value)
                        )
                else:
                    if not isinstance(value, self._f[key]._type):
                        raise TypeError('expect {} for key {}, but got {}'
                                        .format(
                                        self._f[key]._type.__name__,
                                        key,
                                        value.__class__.__name__))
                    if isinstance(value, datetime) and value.tzinfo is None:
                        value = value.replace(tzinfo=timezone.utc)
                    self.__dict__[key] = value
                    valid_fields_count += 1
            elif key == 'etag':
                # save etag to :attr:`_saved_etag`
                self._saved_etag = value
                etagged = True
        if valid_fields_count == len(self._f):
            self._is_partial = False
        else:
            self._is_partial = is_partial

        if etagged is False:
            self._is_changed = self._check_chagned()
        else:
            self._is_new = False
            self._is_changed = False
            self._copy_into_saved()

    def _to_dict(self):
        """
        generate a ``dict`` for Azure SDK

        :rtype: :class:`dict`
        """
        dic = {}
        for name, field in self._f.items():
            if field.require_serializing:
                dic[name] = field.serialize(self.__dict__[name])
                assert dic[name] is None \
                    or isinstance(dic[name], field.serialized_type)
            else:
                dic[name] = self.__dict__[name]
        return dic

    #@staticmethod
    def inject_table_service(f):
        """a decorator to ensure :func:`get_table_service` won't raise
        """
        @wraps(f)
        def wrapper(*args, **kwargs):
            # will only inject if ``ts`` not in ``kwargs``
            if not 'ts' in kwargs or kwargs['ts'] is None:
                ts = get_table_service()
                kwargs['ts'] = ts
            return f(*args, **kwargs)
        if hasattr(wrapper, '__doc__') and isinstance(wrapper.__doc__, str):
            wrapper.__doc__ += '\n        .. py:decoratormethod::' + \
                ' inject_table_service'
        return wrapper

    @classmethod
    def _create_table(self, ts=None):
        if self._created_table is False:
            ts.create_table(self.metas['table_name'],
                            fail_on_exist=False)
            self._created_table = True
    # query

    @classmethod
    @inject_table_service
    def findOne(self, partition_key, row_key, select='*', ts=None):
        """a wrapper around :func:`azure.storage.TableService.get_entity`

        :param str partition_key:
        :param str row_key:
        :param str select:
        :param azure.storage.TableService ts:
        :raises TypeError: if select is not a string
        :returns: None if not found
        :returns: an instance of :class:`Entity` if found
        """
        if not isinstance(select, str):
            raise TypeError('select is not a string, {}'.format(select))
        try:
            raw_entity = ts.get_entity(table_name=self.metas['table_name'],
                                       partition_key=partition_key,
                                       row_key=row_key,
                                       select=select)
        except WindowsAzureMissingResourceError:
            return None
        is_partial = (select is not None and select != '*')
        new_entity = self()
        new_entity._populate_with_dict(dic=raw_entity, is_partial=is_partial)
        if not 'PartitionKey' in select:
            new_entity.PartitionKey = partition_key
        if not 'RowKey' in select:
            new_entity.RowKey = row_key
        return new_entity

    @classmethod
    @inject_table_service
    def find(self, filter=None, select=None, limit=None, ts=None):
        """a wrapper around :func:`azure.storage.TableService.query_entity`

        :param str filter:
        :param str select:
        :param int limit: alias for ``top``
        :raises TypeError: if filter is not None or str
        :raises TypeError: if select is not None or str
        :raises TypeError: if limit is not None or int
        :returns: empty list if nothing
        :returns: list of :class:`Entity`
        """
        if filter is not None and not isinstance(filter, str):
            raise TypeError('filter has to be None or str, {}'.format(filter))
        if select is not None and not isinstance(select, str):
            raise TypeError('select has to be None or str, {}'.format(select))
        if limit is not None and not isinstance(limit, int):
            raise TypeError('limit has to be None or int, {}'.format(limit))
        raw_entities = ts.query_entities(
            table_name=self.metas['table_name'],
            filter=filter,
            select=select,
            top=limit,
        )
        is_partial = (select is not None and select != '*')
        entities = []
        for raw_entity in raw_entities:
            new_entity = self()
            new_entity._populate_with_dict(
                dic=raw_entity, is_partial=is_partial)
            entities.append(new_entity)
        return entities

    @classmethod
    def select(self, fields=None):
        """query entry point

        will pass the select to :class:`QuerySet`"""
        return QuerySet().select(entity=self, fields=fields)

    def _pre_save(self):
        """perform presave operations, maybe can provide pre_save hooks
        in the future

        * check if the entity is valid
        * check if :func:`_check_chagned` and change :attr:`_is_changed`
        """
        self._validate()
        self._is_changed = self._check_chagned()

    @inject_table_service
    def save_insert(self, ts=None):
        """a wrapper around Azure's :func:`insert_entity`

        """
        table_name = self.metas['table_name']
        entity = self._to_dict()
        return ts.insert_entity(table_name=table_name, entity=entity)

    @inject_table_service
    def save_insert_or_replace(self, ts=None):
        """a wrapper around Azure's :func:`insert_or_replace_entity`"""
        table_name = self.metas['table_name']
        entity = self._to_dict()
        return ts.insert_or_replace_entity(table_name=table_name,
                                           partition_key=self.PartitionKey,
                                           row_key=self.RowKey,
                                           entity=entity)

    @inject_table_service
    def save_insert_or_merge(self, ts=None):
        """a wrapper around Azure's :func:`insert_or_merge_entity`"""
        table_name = self.metas['table_name']
        entity = self._to_dict()
        return ts.insert_or_merge_entity(table_name=table_name,
                                         partition_key=self.PartitionKey,
                                         row_key=self.RowKey,
                                         entity=entity)

    @inject_table_service
    def save_merge(self, ts=None):
        """a wrapper around Azure's :func:`merge_entity`"""
        table_name = self.metas['table_name']
        entity = self._to_dict()
        return ts.merge_entity(table_name=table_name,
                               partition_key=self.PartitionKey,
                               row_key=self.RowKey,
                               entity=entity)

    @inject_table_service
    def save_replace(self, ts=None):
        """a wrapper around Azure's :func:`update_entity`"""
        table_name = self.metas['table_name']
        entity = self._to_dict()
        return ts.update_entity(table_name=table_name,
                                partition_key=self.PartitionKey,
                                row_key=self.RowKey,
                                entity=entity)

    def save(self, force_replace=False,
             force_merge=False, force_save=False,
             ignore_conflict=False):
        """save the entity to server

        **possible actions:**

        * ``force_save`` to override :attr:`_is_changed`

        * insert (avoiding unknowleged replacing/merging) :func:`insert_entity`

            * if :attr:`_is_new` is ``True``
            * if ``ignore_conflict`` is True,
                :class:`azure.WindowsAzureConflictError`, will be
                ignored

        * insert or replace (update) :func:`insert_or_replace_entity`

            * if :attr:`_is_new` is True
            * **and** ``force_replace`` is True

        * insert or merge :func:`insert_or_merge_entity`

            * if :attr:`_is_new` is True
            * **and** ``force_merge`` is True

        * merge :func:`merge_entity`

            * if :attr:`_is_new` is False
            * **and** if :attr:`_is_partial` is True
            * **or** ``force_merge`` is True

        * replace (update) :func:`update_entity`

            * if :attr:`_is_new` is False
            * **and** if :attr:`_is_partial` is False
            * **or** ``force_replace`` is True


        **we need to check the validation and check changes**

        by calling :func:`_pre_save`

        :param bool force_replace: default = False
        :param bool force_merge: default = False
        :param bool force_save: default = False
        """
        self._pre_save()
        result = None
        if self._is_new:
            if force_merge is True:
                result = self.save_insert_or_merge()
            elif force_replace is True:
                result = self.save_insert_or_replace()
            else:
                try:
                    result = self.save_insert()
                except WindowsAzureConflictError as e:
                    if ignore_conflict is False:
                        raise WindowsAzureConflictError(e)
                    else:
                        return False
        else:
            if self._is_changed is False and force_save is False:
                return False
            if self._is_partial is True:
                if force_replace is True:
                    result = self.save_replace()
                else:
                    result = self.save_merge()
            else:
                if force_merge is True:
                    result = self.save_merge()
                else:
                    result = self.save_replace()
        return self._after_save(saved_entity_dict=result)

    def _after_save(self, saved_entity_dict):
        """called after save function, if successful

        some functions will return a full entity_dict,
        we will call :func:`_populate_with_dict`

        * ``insert_entity``

        some functions will only return a dict with only 'etag' key,
        we will manually call :func:`_copy_into_saved` and change status
        variables

        * ``update_entity``
        * ``merge_entity``
        * ``insert_or_replace_entity``
        * ``insert_or_merge_entity``

        * change :attr:`_is_new` to False (via :func:`_populate_with_dict`)
        * change :attr:`_is_changed` to False (via :func:`_populate_with_dict`)
        * change :attr:`_saved_etag` (via :func:`_populate_with_dict`)

        :param dict saved_entity_dict: could also be AzureEntity
        :raises TypeError: if saved_entity_dict is other,
        :raises KeyError: if etag is not found in dict
        :returns: True if everything is fine
        """
        if not isinstance(saved_entity_dict, (dict, AzureTableEntity)):
            raise TypeError('saved_entity_dict is not a dict, {}'.format(
                saved_entity_dict))
        if isinstance(saved_entity_dict, AzureTableEntity):
            saved_entity_dict = saved_entity_dict.__dict__
        if not 'etag' in saved_entity_dict:
            raise KeyError('etag is not in : {}'.format(saved_entity_dict))
        if len(saved_entity_dict) == 1:
            self._is_new = False
            self._is_changed = False
            self._saved_etag = saved_entity_dict['etag']
            self._copy_into_saved()
        else:
            self._populate_with_dict(
                dic=saved_entity_dict, is_partial=self._is_partial)
        return True

    @inject_table_service
    def delete(self, force_delete=False, ts=None):
        """
        a wrapper around Azure's :func:`delete_entity`


        :param bool force_delete: perform delete even if :attr:`_is_new`
            is True, **default** False, this will surpress ``NotFound`` error
        :raises TypeError: if :attr:`PartitionKey` is not a string
        :raises TypeError: if :attr:`RowKey`: is not a string
        """
        if not isinstance(self.PartitionKey, self._f['PartitionKey']._type):
            raise TypeError(
                'PartitionKey is not a string, {}'.format(self.PartitionKey))
        if not isinstance(self.RowKey, self._f['RowKey']._type):
            raise TypeError(
                'RowKey is not a string, {}'.format(self.RowKey))
        if self._is_new is True and force_delete is not True:
            raise Exception(
                'trying to delete a none saved entity,' +
                ' please use force_delete=True')
        ts.delete_entity(
            table_name=self.metas['table_name'],
            partition_key=self.PartitionKey,
            row_key=self.RowKey)

    @classmethod
    @inject_table_service
    def drop_table(self, fail_not_exist=False, ts=None):
        """
        a wrapper around Azure's :func:`delete_table`
        """
        ts.delete_table(
            table_name=self.metas['table_name'],
            fail_not_exist=fail_not_exist)
