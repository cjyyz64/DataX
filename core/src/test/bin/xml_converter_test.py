#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# python: 2.7

__author__ = 'xiafei.qiuxf'

import unittest
from string import Template
import json
from xml_convertor import XmlConverter


try:
    from xml.etree import ElementTree as XmlTree
except:
    import elementtree.ElementTree as XmlTree


class XmlConverterTest(unittest.TestCase):

    def test_invalid_xml(self):
        self.assertRaises(Exception, XmlConverter, "")

    def test_no_reader_writer(self):
        with open('./case/no_reader.xml') as f:
            self.assertRaisesRegexp(Exception, 'reader or writer can not be none', XmlConverter, f.read())
        with open('./case/no_writer.xml') as f:
            self.assertRaisesRegexp(Exception, 'reader or writer can not be none', XmlConverter, f.read())

    def test_reader_dispatch(self):
        with open('./case/empty.xml') as f:
            t = Template(f.read())
            for plugin in ['mysqlreader', 'sqlserverreader', 'odpsreader', 'streamreader']:
                s = t.substitute(READER_TYPE=plugin, WRITER_TYPE='xxxwriter')
                c = XmlConverter(s)
                setattr(c, 'parse_' + plugin, lambda: 'I_AM_' + plugin)
                self.assertEqual(c.parse_reader(), 'I_AM_' + plugin)

            for plugin in ['txtfilewriter', 'txtfilewriter', 'txtfilewriter', 'txtfilewriter']:
                s = t.substitute(READER_TYPE='xxxreader', WRITER_TYPE=plugin)
                c = XmlConverter(s)
                setattr(c, 'parse_' + plugin, lambda: 'I_AM_' + plugin)
                self.assertEqual(c.parse_writer(), 'I_AM_' + plugin)

            s = t.substitute(READER_TYPE='NO_SUCH_READER', WRITER_TYPE='NO_SUCH_WRITER')
            c = XmlConverter(s)
            self.assertFalse(c.parse_reader())
            self.assertFalse(c.parse_writer())

    def test_set_run_speed(self):
        with open('./case/ok.xml') as f:
            c = XmlConverter(f.read())
            c.get_value_from_xml = lambda x, y: '3'
            c.set_run_speed()
            self.assertEqual(c.job_setting['speed']['byte'], 1024 * 1024 * 3)

            c.get_value_from_xml = lambda x, y: '-2'
            c.set_run_speed()
            self.assertEqual(c.job_setting['speed']['byte'], 1024 * 1024 * 1)

            c.get_value_from_xml = lambda x, y: ''
            c.set_run_speed()
            self.assertEqual(c.job_setting['speed']['byte'], 1024 * 1024 * 1)

    def test_error_limit(self):
        with open('./case/ok.xml') as f:
            c = XmlConverter(f.read())
            # 0表示不限制
            # mock it
            c.get_value_from_xml = lambda x, y: '0'
            self.assertTrue(c.set_error_limit())
            self.assertIsNone(c.job_setting['errorLimit']['record'])
            self.assertIsNone(c.job_setting['errorLimit'].get('percentage'))

            # < 0也表示不限制
            c.get_value_from_xml = lambda x, y: '-1'
            self.assertTrue(c.set_error_limit())
            self.assertIsNone(c.job_setting['errorLimit']['record'])
            self.assertIsNone(c.job_setting['errorLimit'].get('percentage'))

            # 5 表示限制最多4条
            c.get_value_from_xml = lambda x, y: '5'
            self.assertTrue(c.set_error_limit())
            self.assertEqual(c.job_setting['errorLimit']['record'], 4)
            self.assertIsNone(c.job_setting['errorLimit'].get('percentage'))

            # 小数表示百分数
            c.get_value_from_xml = lambda x, y: '0.5'
            self.assertTrue(c.set_error_limit())
            self.assertIsNone(c.job_setting['errorLimit'].get('record'))
            self.assertEqual(c.job_setting['errorLimit']['percentage'], 0.5)

            # 小数表示百分数
            c.get_value_from_xml = lambda x, y: 's9x'
            self.assertFalse(c.set_error_limit())

            # 空表示不限制 TODO 确认datax行为
            c.get_value_from_xml = lambda x, y: ''
            c.job_setting = {}
            self.assertTrue(c.set_error_limit())
            self.assertIsNone(c.job_setting.get('errorLimit'))

    def test_get_value_from_xml(self):
        with open('./case/empty.xml') as f:
            c = XmlConverter(f.read())
            value = c.get_value_from_xml(c.reader, 'project')
            self.assertEqual(value, 'xxxx-proj')

            value = c.get_value_from_xml(c.reader, 'what_is_up')
            self.assertIsNone(value)

    def test_parse_column(self):
        # self.longMessage = True
        self.assertEqual(XmlConverter.parse_column(''), ['*'])
        self.assertEqual(XmlConverter.parse_column('*'), ['*'])
        self.assertEqual(XmlConverter.parse_column(' * '), ['*'])
        self.assertEqual(XmlConverter.parse_column('a,1,Null, null, NULL'), ['a', '1', None, None, None])
        for item in [['a', 'b', 'c'],
                     ['a', 'b()', 'c'],
                     ['"a"', 'b()', 'c'],
                     ['a', 'b("", x, \'s\')', 'c'],
                     # ['count(distinct(a))', 'b("", x, \'(\')', 'c']
                     # ['a(\'"),\')', 'c', 'd']
                     ]:
            s = ','.join(item)
            self.assertEqual(XmlConverter.parse_column(s), item)

        with open('./case/column_case.txt') as f:
            for line_num, line in enumerate(f.readlines()):
                data = [i.strip() for i in json.loads(line)]
                s = ','.join(data)
                self.assertEqual(XmlConverter.parse_column(s),
                                 [None if i.strip().lower() == 'null' else i for i in data],
                                 msg='Not equal on line: %d' % (int(line_num) + 1))


