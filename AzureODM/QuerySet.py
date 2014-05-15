"""
    QuerySet
"""
from functools import wraps
from re import compile as re_compile
#from re import IGNORECASE as re_IGNORECASE
from datetime import datetime, timezone
#from .Entity import Entity
dt_format = '%Y-%m-%dT%H:%M:%S'


def obj_to_query_value(obj):
    """convert obj to query value

    * str: 'value'
    * int: value
    * float: value
    * bool: true or false
    * datetime: datetime'2008-07-10T00:00:00Z'
    """
    if isinstance(obj, str):
        return "'{}'".format(obj)
    if isinstance(obj, bool):
        if obj is True:
            return 'true'
        else:
            return 'false'
    if isinstance(obj, (int, float)):
        return '{}'.format(obj)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            raise ValueError('only timezone awared datetime is accpeted')
        new_dt = obj.astimezone(timezone.utc)
        return "datetime'{}Z'".format(new_dt.strftime(dt_format))

    raise Exception('obj type is unknown, {}'.format(obj))


class QOperator:

    """Store & (AND), | (OR)

    used when combining queries
    """


class AndOperator(QOperator):

    o = 'and'


class OrOperator(QOperator):

    o = 'or'


class InOperator(QOperator):

    o = 'in'


class QNode:

    """Query Node"""
    cmp_exp = '^(?P<field>[a-zA-Z0-9]+)(__(?P<operator>ne|gt|ge|lt|le|ne|in))?$'
    cmp_re = re_compile(cmp_exp)

    def _combine(self, other, operation):
        """Other can be either Q, or QCombination"""
        return QCombination(operation=operation, q1=self, q2=other)

    def __or__(self, other):
        return self._combine(other, OrOperator)

    def __and__(self, other):
        return self._combine(other, AndOperator)


class Q(QNode):

    """
    where(Q(PartitionKey='p1') | Q(PartitionKey='p2')) ->
    ``(PartitionKey eq 'p1' or PartitionKey eq 'p2')``

    if we attach another andWhere(Q(RowKey='r1') | Q(RowKey='r2'))
    the query will become:

    ``(PartitionKey eq 'p1' or PartitionKey eq 'p2') and (RowKey eq 'r1 or
        RowKey eq 'r2')``

    """

    def __init__(self, **query):
        """Parse the query to string

        :raise ValueError: if ``query`` is none
        """
        k, v = query.popitem()
        if v is None:
            raise ValueError('comparison value cannot be None')
        self.k = k
        self.v = v

    def compile(self, entity):
        if entity is None:
            raise TypeError('entity is not a subclass of Entity')
        m = self.cmp_re.match(self.k)
        if m is None:
            raise ValueError(
                '{}={} is not a valid query'.format(self.k, self.v))
        query_string = m.group('field')
        if not query_string in entity.__dict__:
            raise KeyError('field is not defined, {}'.format(query_string))
        if m.group('operator') is None:
            query_string += ' eq'
        elif m.group('operator') == 'in':
            if not isinstance(self.v, list):
                raise TypeError('in operator must followed be list')
            if len(self.v) == 0:
                raise ValueError('no empty list after in operator')
            in_query_format = "{} eq {}"
            query_string = '(' + in_query_format.format(
                m.group('field'), obj_to_query_value(self.v.pop(0)))
            for val in self.v:
                query_string += ' or ' + \
                    in_query_format.format(
                        m.group('field'), obj_to_query_value(val))
            query_string += ')'
            return query_string
        else:
            query_string += ' ' + m.group('operator')
        query_string += ' ' + obj_to_query_value(self.v)
        return query_string


class QCombination(QNode):

    """Combination of Q and QOperator s"""

    def __init__(self, operation, q1, q2):
        """


        :param QNode q2:
        :param QNode q1: it should be exactly two items in queries
            and they will be combined into a new QCombination with
            ``operation`` in between

        :raises TypeError: if queries is not a list
        :raises ValueError: if len(queries) != 2
        :raises TypeError: if operation is not a :class:`QOperator`
        """
        if not isinstance(q1, QNode):
            raise TypeError('q1 is not a list, {}'.format(q1))
        if not isinstance(q1, QNode):
            raise TypeError('q2 is not a list, {}'.format(q2))
        if not issubclass(operation, QOperator):
            raise TypeError(
                'operation is not a subclass QOperator, {}'.format(operation))

        self.subquires = []
        if isinstance(q1, Q):
            self.subquires.append(q1)
        elif isinstance(q1, QCombination):
            self.subquires += q1.subquires
        self.subquires.append(operation)
        if isinstance(q2, Q):
            self.subquires.append(q2)
        elif isinstance(q2, QCombination):
            self.subquires += q2.subquires

    def compile(self, entity):
        if entity is None:
            raise TypeError('entity is not a subclass of Entity')
        query_string = '('
        next_type = Q
        for query in self.subquires:
            if (next_type is Q and not isinstance(query, next_type)) or \
                (next_type is QOperator and not issubclass(query, next_type)
                 ):
                raise TypeError(
                    'QOperator has to be used to connect two query')
            if isinstance(query, Q):
                query_string += query.compile(entity=entity)
            elif issubclass(query, QOperator):
                query_string += ' ' + query.o + ' '

            if next_type is Q:
                next_type = QOperator
            else:
                next_type = Q

        query_string += ')'
        return query_string


class QuerySet:

    """
    support queries:

    * eq ``field__eq=value``
    * gt ``field__gt=value``
    * ge ``field__ge=value``
    * lt ``field__lt=value``
    * le ``field__le=value``
    * ne ``field__ne=value``
    * in ``field__in=[]``
    * and :func:`andWhere`
    * not :func:`notWhere`
    * or :func:`orWhere`
    """

    #COMPARISON_OPERATORS = ('ne', 'gt', 'ge', 'lt', 'le', 'ne')
    cmp_exp = '^(?P<field>[a-zA-Z0-9]+)(__(?P<operator>ne|gt|ge|lt|le|ne))?$'
    cmp_re = re_compile(cmp_exp)

    def __init__(self):
        self._targeted_entity = None
        self._select = None
        self.filter = ''
        self._limit = None

    def query_parser(f):
        """
        a decorator to compile where queries
        """
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            # will only inject if ``ts`` not in ``kwargs``
            if len(args) == 1:
                q = args[0]
                if not isinstance(q, QCombination):
                    raise KeyError('you cannot put args into query function')
                query_string = q.compile(entity=self._targeted_entity)
            else:
                # if len(kwargs) == 0:
                #    raise KeyError('you have to have at least one kwargs')
                if len(kwargs) != 1:
                    raise KeyError(
                        'you cannot put more than one args into query function')
                if self._targeted_entity is None:
                    raise Exception('please call select before using query')
                q = Q(**kwargs)
                query_string = q.compile(entity=self._targeted_entity)
            return f(self, query_string=query_string)
        if hasattr(wrapper, '__doc__') and isinstance(wrapper.__doc__, str):
            wrapper.__doc__ += '\n        .. py:decoratormethod::' + \
                ' query_parser'
        return wrapper

    @query_parser
    def where(self, query_string):
        """the entry point of ``filter``"""
        if self.filter != '':
            raise Exception(
                'you have to call where() before other `where` like queries')
        else:
            self.filter += query_string
        return self

    @query_parser
    def andWhere(self, query_string):
        """ ``and``
        """
        if self.filter == '':
            raise Exception('you have to call `where()` first')
        self.filter += ' and ' + query_string
        return self

    @query_parser
    def orWhere(self, query_string):
        """ ``or``
        """
        if self.filter == '':
            raise Exception('you have to call `where()` first')
        self.filter += ' or ' + query_string
        return self

    @query_parser
    def notWhere(self, query_string):
        """ ``not``
        """
        if self.filter == '':
            raise Exception('you have to call `where()` first')
        self.filter += ' not ' + query_string
        return self

    def select(self, entity, fields=None):
        """

        :param fields: fields can be ether a list or '*' or ``None``, if
            its not ``None`` or '*', ``PartitionKey`` and ``RowKey`` will be
            added if not existed
        :param Entity entity: must be an instance of `Entity`
        :raises TypeError: if fileds is not ``dict`` or None
        :raises TypeError: if entity is not an instance of ``Entity``
        """
        from .Entity import Entity
        if not issubclass(entity, Entity):
            raise TypeError('entity is not an instance of Entity')
        if fields is not None \
                and fields != '*' \
                and not isinstance(fields, list):
            raise TypeError('fields can only be list or None')
        if isinstance(fields, list):
            if not 'PartitionKey' in fields:
                fields.append('PartitionKey')
            if not 'RowKey' in fields:
                fields.append('RowKey')
            self._select = ','.join(fields)
        elif fields is None:
            self._select = '*'
        elif fields == '*':
            self._select = fields
        else:
            raise ValueError('invalid select, {}'.format(fields))
        self._targeted_entity = entity
        return self

    def limit(self, limit):
        """set the ``top`` query

        :param int limit:
        :raises TypeError: if limit is not int
        """
        if limit is not None and not isinstance(limit, int):
            raise TypeError('limit is not an int, {}'.format(limit))
        self._limit = limit
        return self

    def go(self):
        """will call :attr:`_targeted_entity` 's :func:`Entity.find`

        this will pass :attr:`_select` to :func:`Entity.find` and pass
        :attr:`filter` (query string) with the limit
        """
        if self._targeted_entity is None:
            raise Exception('you must call select before call go')
        return self._targeted_entity.find(
            filter=self.filter,
            select=self._select,
            limit=self._limit,
        )
