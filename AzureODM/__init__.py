from re import compile as re_compile
iso_date_format = '%Y-%m-%d'
# time %H:%M:%S
iso_datetime_format_reg = re_compile(
    '^(?P<Y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})(?P<time>T(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})[-|\+]00:00)?$')
import datetime


def _datetime_json_encoder(o):
    """Check if object is: ``datetime.date`` ``datetime.datetime``,
    if ``datetime.datetime`` has None ``tzinfo``, ``timezone.utc`` will
    be added, if ``tzinfo`` is not ``utc``, will be converted to ``utc``
    using ``astimezone``
    """
    if isinstance(o, datetime.datetime):
        if o.tzinfo is None:
            return o.replace(tzinfo=datetime.timezone.utc).isoformat()
        else:
            return o.astimezone(tz=datetime.timezone.utc).isoformat()
    elif isinstance(o, datetime.date):
        return o.isoformat()
    elif isinstance(o, datetime.time):
        raise TypeError("datetime.time is not JSON serializable")
    else:
        raise TypeError(repr(o) + " is not JSON serializable")


def _datetime_json_object_hook(dct):
    for key, value in dct.items():
        if isinstance(value, str):
            m = iso_datetime_format_reg.match(value)
            if m is not None:
                if m.group('time') is not None:
                    dct[key] = datetime.datetime(
                        int(m.group('Y')),
                        int(m.group('m')),
                        int(m.group('d')),
                        int(m.group('H')),
                        int(m.group('M')),
                        int(m.group('S')),
                        tzinfo=datetime.timezone.utc
                    )
                    continue
                else:
                    dct[key] = datetime.date(
                        int(m.group('Y')),
                        int(m.group('m')),
                        int(m.group('d'))
                    )
                    continue
    return dct
