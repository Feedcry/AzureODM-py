"""
    test_root.py

    test __init__.py of module
"""
import pytest
from AzureODM import (
    iso_datetime_format_reg, _datetime_json_encoder, _datetime_json_object_hook)
import datetime


class Test_iso_datetime_format_reg:

    """test iso_datetime_format_reg, iso_datetime_regular_expression
    """

    @pytest.mark.parametrize('dt_string,matched,time_matched,expected', [
        ('2014-01-01T00:00:00+00:00', True, True, '2014-01-01-00-00-00'),
        ('2011-03-12T23:59:59-00:00', True, True, '2011-03-12-23-59-59'),
        ('2011-01-01', True, False, '2011-01-01-None-None-None'),
        ('2014-01-01T00:00:00+05:00', False,
         False, 'None-None-None-None-None-None'),
        ('1981-1-1T1:1:1+00:00', False, False, 'None-None-None-None-None-None')
    ])
    def test_match_datetime_strings(self,
                                    dt_string,
                                    matched,
                                    time_matched,
                                    expected):
        m = iso_datetime_format_reg.match(dt_string)
        assert (m is not None) is matched
        if m is not None:
            output = '{}-{}-{}-{}-{}-{}'.format(m.group('Y'),
                                                m.group('m'),
                                                m.group('d'),
                                                m.group('H'),
                                                m.group('M'),
                                                m.group('S'))
            assert (m.group('time') is not None) is time_matched
            assert output == expected


class Test__datetime_json_encoder:

    """test _datetime_json_encoder"""
    @pytest.mark.parametrize('value', [
        '123456',
        123455,
        12345.0,
        datetime.time(1, 1, 1),
        ['lol'],
        [],
        {},
        {'lol': 'lol'}
    ])
    def test_invalid_type_raises(self, value):
        with pytest.raises(TypeError):
            _datetime_json_encoder(value)

    @pytest.mark.parametrize('value,expected', [
        (datetime.date(2014, 1, 1), '2014-01-01'),
        (datetime.date(1922, 12, 23), '1922-12-23'),
        (datetime.datetime(2014, 1, 1, 1, 1, 1), '2014-01-01T01:01:01+00:00'),
        # convert other timezone to utc
        (datetime.datetime(2014, 1, 1, 1, 1, 1, tzinfo=datetime.timezone(
                           datetime.timedelta(
                               minutes=1))), '2014-01-01T01:00:01+00:00'),
        (datetime.datetime(2014, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc),
         '2014-01-01T01:01:02+00:00')
    ])
    def test_return_iso_format_with_utc_tz(self, value, expected):
        assert _datetime_json_encoder(value) == expected

    def test_datetime_time_raise_special_error(self):
        with pytest.raises(TypeError) as e:
            _datetime_json_encoder(datetime.time(1, 1, 1))
        assert 'datetime.time is not JSON serializable' in str(e)


class Test__datetime_json_object_hook:

    """test _datetime_json_object_hook"""

    def test_convert_matching_string_in_dict(self):
        dct = {
            'fulldt': '2014-01-01T01:01:01+00:00',
            'fulld': '2014-01-01',
            'invaliddt': '2014-01-01T01:01:01+00:01',
            'invalidd': '2014-1-1',
            'other_string': 'lsadf',
            'other_type': 123.1
        }

        expected_dct = {
            'fulldt': datetime.datetime(2014, 1, 1, 1, 1, 1,
                                        tzinfo=datetime.timezone.utc),
            'fulld': datetime.date(2014, 1, 1),
            'invaliddt': '2014-01-01T01:01:01+00:01',
            'invalidd': '2014-1-1',
            'other_string': 'lsadf',
            'other_type': 123.1
        }

        result = _datetime_json_object_hook(dct=dct)
        assert result == expected_dct
