#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# author: jingxing
# python: 2.7

import json
import time
import sys

try:
    from xml.etree import ElementTree as XmlTree
except:
    import elementtree.ElementTree as XmlTree


def str_is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def str_is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


class XmlConverter:
    def __init__(self, job_string):
        self.root = XmlTree.fromstring(job_string)
        self.reader = self.root.find("job/reader")
        self.writer = self.root.find("job/writer")
        if self.reader is None or self.writer is None:
            raise Exception("reader or writer can not be none")

        self.job_setting = {}
        self.reader_parameter = {}
        self.reader_dict = {"parameter": self.reader_parameter}
        self.writer_parameter = {}
        self.writer_dict = {"parameter": self.writer_parameter}

    # 返回json字符串
    def parse_to_json(self):
        if not self.parse_reader():
            print >>sys.stderr, "parse reader error"
            return None

        if not self.parse_writer():
            print >>sys.stderr, "parse writer error"
            return None

        job_json = {
            "setting": self.job_setting,
            "content": [
                {
                    "reader": self.reader_dict,
                    "writer": self.writer_dict
                }
            ]
        }

        return json.dumps({"job": job_json}, sort_keys=True, indent=4)

    def parse_reader(self):
        reader_plugin = self.reader.find("plugin").text
        self.reader_dict["name"] = reader_plugin
        self.set_run_speed()

        if reader_plugin == "streamreader":
            return self.parse_streamreader()
        elif reader_plugin == "mysqlreader":
            return self.parse_mysqlreader()
        elif reader_plugin == "odpsreader":
            return self.parse_odpsreader()
        elif reader_plugin == "sqlserverreader":
            return self.parse_sqlserverreader()
        else:
            print >>sys.stderr, "unsupported reader plugin[%s]"%(reader_plugin)
            return False

    def parse_writer(self):
        writer_plugin = self.writer.find("plugin").text
        self.writer_dict["name"] = writer_plugin
        if not self.set_error_limit():
            return False

        if writer_plugin == "streamwriter":
            return self.parse_streamwriter()
        elif writer_plugin == "odpswriter":
            return self.parse_odpswriter()
        elif writer_plugin == "mysqlwriter":
            return self.parse_mysqlwriter()
        elif writer_plugin == "txtfilewriter":
            return self.parse_txtfilewriter()
        else:
            print >>sys.stderr, "unsupported writer plugin[%s]" % writer_plugin
            return False

    def set_run_speed(self):
        # 速度是原reader的concurrency * 1M
        concurrency = self.get_value_from_xml(self.reader, "concurrency")
        if not concurrency:
            concurrency = "1"
        concurrency = int(concurrency)
        self.job_setting["speed"] = {
            "byte": 1024 * 1024 * (1 if concurrency < 1 else concurrency)
        }

    def set_error_limit(self):
        """
        新版0表示不允许脏数据，旧版是1。
        新版record和percentage都是null表示不限制，旧版是0。
        浮点情况下语意是一致的。
        :return:
        """
        error_limit = self.get_value_from_xml(self.writer, "error-limit")
        if not error_limit:
            # TODO 没设置认为不限制？需要确认dataX默认值
            return True

        if str_is_int(error_limit):
            record = int(error_limit) - 1
            self.job_setting["errorLimit"] = {
                'record': None if record < 0 else record
            }
            return True

        if str_is_float(error_limit):
            limit_val = float(error_limit)
            if limit_val <= 0 or limit_val >= 1:
                print >>sys.stderr, "invalid error limit value: " + error_limit
                return False
            else:
                self.job_setting["errorLimit"] = {
                    'percentage': limit_val
                }
                return True

        print >>sys.stderr, "invalid error limit value: " + error_limit
        return False

    @staticmethod
    def get_value_from_xml(node_root, key):
        value = None
        try:
            for item in node_root.getiterator(tag="param"):
                if item.attrib["key"] == key:
                    value = item.attrib["value"]
                    break
        finally:
            return value

    @staticmethod
    def parse_map_column(columns, is_map=True):
        if not columns or not columns.strip() or columns == "*":
            return ["*"]

        columns = columns.strip().strip(",")
        column_array = []
        bracket_count = 0
        quote_count = 0
        begin = 0
        for i, ch in enumerate(columns):
            if i == len(columns)-1:
                column_array.append({"name": columns[begin:]} if is_map else columns[begin:])
            elif ch == ",":
                if bracket_count == 0 and quote_count % 2 == 0:
                    column_array.append({"name": columns[begin:i]} if is_map else columns[begin:i])
                    begin = i+1
            elif ch == "'" or ch == "\"":
                quote_count += 1
            elif ch == "(":
                bracket_count += 1
            elif ch == ")":
                bracket_count -= 1

        return [i.strip() for i in column_array]

    @staticmethod
    def parse_column(columns):
        return XmlConverter.parse_map_column(columns, is_map=False)

    ############ stream #############
    def parse_streamreader(self):
        sliceRecordCount = self.get_value_from_xml(self.reader, "record-count")
        if sliceRecordCount:
            self.reader_parameter["sliceRecordCount"] = sliceRecordCount
        else:
             self.reader_parameter["sliceRecordCount"] = 100000

        if self.get_value_from_xml(self.reader, "column"):
            columns = []
            self.reader_parameter["column"] = columns
            columns.append({"value":"filed", "type":"string"})
            columns.append({"value":100, "type":"int"})
            columns.append({"value":int(time.time()), "type":"date"})
            columns.append({"value":True, "type":"bool"})
            columns.append({"value":"byte string", "type":"byte"})
        else:
            field_delimiter = self.get_value_from_xml(self.reader, "field-delimiter")
            if not field_delimiter:
                field_delimiter = ","
            self.reader_parameter["fieldDelimiter"] = field_delimiter

        return True

    def parse_streamwriter(self):
        is_visible = self.get_value_from_xml(self.writer, "visible")
        if is_visible and "true"==is_visible.lower():
            self.writer_parameter["visible"] = True
            encoding = self.get_value_from_xml(self.writer, "encoding")
            if not encoding:
                encoding = "UTF-8"
            self.writer_parameter["encoding"] = encoding
            field_delimiter = self.get_value_from_xml(self.writer, "field-delimiter")
            if not field_delimiter:
                field_delimiter = ","
            self.writer_parameter["fieldDelimiter"] = field_delimiter
        else:
            self.writer_parameter["visible"] = False

        return True

    ############ mysql #############
    def parse_mysqlreader(self):
        self.reader_parameter["username"] = self.get_value_from_xml(self.reader, "username")
        self.reader_parameter["password"] = self.get_value_from_xml(self.reader, "password")

        jdbc_url_list = self.get_value_from_xml(self.reader, "jdbc-url").split("|")
        encoding = self.get_value_from_xml(self.reader, "encoding")
        if encoding:
            for index in range(len(jdbc_url_list)):
                jdbc_url = jdbc_url_list[index]
                if jdbc_url.find("characterEncoding") < 0:
                    jdbc_url_list[index] = "%s?useUnicode=true&characterEncoding=%s"%(jdbc_url, encoding)
        connection_list = []
        self.reader_parameter["connection"] = connection_list

        pointed_sql = self.get_value_from_xml(self.reader, "sql")
        if pointed_sql:
            connection_dict = {
                "jdbcUrl": jdbc_url_list,
                "querySql": pointed_sql
            }
            connection_list.append(connection_dict)
        else:
            tables = self.get_value_from_xml(self.reader, "table")
            table_list = tables.split("|")
            if len(table_list) != len(jdbc_url_list):
                print >>sys.stderr, "table length[%d] not equal to jdbc-url length[%d]"%(len(table_list), len(jdbc_url_list))
                return False
            for i in range(len(table_list)):
                connection_dict = {}
                connection_list.append(connection_dict)
                connection_dict["table"] = table_list[i].split(",")
                connection_dict["jdbcUrl"] = jdbc_url_list[i].split(",")
            where = self.get_value_from_xml(self.reader, "where")
            if where:
                self.reader_parameter["where"] = where
            self.reader_parameter["column"] = self.parse_column(self.get_value_from_xml(self.reader, "column"))

        return True

    def parse_mysqlwriter(self):
        self.writer_parameter["user"] = self.get_value_from_xml(self.writer, "user")
        self.writer_parameter["password"] = self.get_value_from_xml(self.writer, "password")
        replace = self.get_value_from_xml(self.writer, "replace")
        if replace:
            if replace.lower() == "true":
                replace = "replace"
            else:
                replace = "insert"
        else:
            replace = "replace"
        self.writer_parameter["insertOrReplace"] = replace
        self.writer_parameter["column"] = self.parse_column(self.get_value_from_xml(self.writer, "column"))

        jdbc_url = self.get_value_from_xml(self.writer, "jdbc-url")
        if not jdbc_url:
            ip = self.get_value_from_xml(self.writer, "ip")
            port = self.get_value_from_xml(self.writer, "port")
            if not port:
                port = "3306"
            database = self.get_value_from_xml(self.writer, "database")
            jdbc_url = "jdbc:mysql://%s:%s/%s"%(ip, port, database)

        connection_list = []
        self.writer_parameter["connection"] = connection_list
        jdbc_url_array = jdbc_url.split("|")
        tables = self.get_value_from_xml(self.writer, "table")
        table_array = tables.split("|")
        if len(jdbc_url_array) != len(table_array):
            print >>sys.stderr, "mysql writer table length[%d] not equal to jdbcUrl length[%d]"%(len(table_array), len(jdbc_url_array))
            return False
        for i in range(len(table_array)):
            connection_dict = {}
            connection_list.append(connection_dict)
            connection_dict["jdbcUrl"] = jdbc_url_array[i]
            connection_dict["table"] = table_array[i].split(",")
        self.writer_parameter["preSql"] = self.get_value_from_xml(self.writer, "pre")
        self.writer_parameter["postSql"] = self.get_value_from_xml(self.writer, "post")

        return True

    ############ odps #############
    def parse_odpsreader(self):
        self.reader_parameter["accessId"] = self.get_value_from_xml(self.reader, "access-id")
        self.reader_parameter["accessKey"] = self.get_value_from_xml(self.reader, "access-key")
        self.reader_parameter["project"] = self.get_value_from_xml(self.reader, "project")
        self.reader_parameter["table"] = self.get_value_from_xml(self.reader, "table")
        self.reader_parameter["partition"] = self.get_value_from_xml(self.reader, "partition")
        self.reader_parameter["odpsServer"] = self.get_value_from_xml(self.reader, "odps-server")
        self.reader_parameter["tunnelServer"] = self.get_value_from_xml(self.reader, "tunnel-server")
        self.reader_parameter["column"] = self.parse_map_column(self.get_value_from_xml(self.reader, "column"))

        return True

    def parse_odpswriter(self):
        self.writer_parameter["accessId"] = self.get_value_from_xml(self.writer, "access-id")
        self.writer_parameter["accessKey"] = self.get_value_from_xml(self.writer, "access-key")
        self.writer_parameter["project"] = self.get_value_from_xml(self.writer, "project")
        self.writer_parameter["table"] = self.get_value_from_xml(self.writer, "table")
        self.writer_parameter["partition"] = self.get_value_from_xml(self.writer, "partition")
        self.writer_parameter["odpsServer"] = self.get_value_from_xml(self.writer, "odps-server")
        self.writer_parameter["tunnelServer"] = self.get_value_from_xml(self.writer, "tunnel-server")
        self.writer_parameter["column"] = self.parse_map_column(self.get_value_from_xml(self.writer, "column"))

        is_truncate = True
        truncate = self.get_value_from_xml(self.writer, "truncate")
        if truncate and truncate.lower() == "false":
            is_truncate = False
        self.writer_parameter["truncate"] = is_truncate

        account_provider = self.get_value_from_xml(self.writer, "account-provider")
        if account_provider:
            self.writer_parameter["accountProvider"] = account_provider

        return True

    ############ sqlserver #############
    def parse_sqlserverreader(self):
        self.reader_parameter["username"] = self.get_value_from_xml(self.reader, "username")
        self.reader_parameter["password"] = self.get_value_from_xml(self.reader, "password")

        connection_dict = {}
        self.reader_parameter["connection"] = [connection_dict]

        pointed_sql = self.get_value_from_xml(self.reader, "sql")
        if pointed_sql:
            connection_dict["querySql"] = pointed_sql
        else:
            self.reader_parameter["column"] = self.parse_column(self.get_value_from_xml(self.reader, "column"))
            where = self.get_value_from_xml(self.reader, "where")
            if where:
                self.reader_parameter["where"] = where
            tables = self.get_value_from_xml(self.reader, "table")
            connection_dict["table"] = tables.split("|")

        jdbc_url = self.get_value_from_xml(self.reader, "jdbc_url")
        if not jdbc_url:
            ip = self.get_value_from_xml(self.reader, "ip")
            port = self.get_value_from_xml(self.reader, "port")
            database = self.get_value_from_xml(self.reader, "database")
            jdbc_url = "jdbc:sqlserver://%s:%s;DatabaseName=%s"%(ip, port, database)
        connection_dict["jdbcUrl"] = [jdbc_url]

        return True

    ############ txtfile #############
    def parse_txtfilewriter(self):
        self.writer_parameter["path"] = self.get_value_from_xml(self.writer, "path")
        self.writer_parameter["concurrency"] = self.get_value_from_xml(self.writer, "concurrency")

        return True

if __name__ == "__main__":
    f = open("job.xml")
    context = f.read()
    f.close()

    converter = XmlConverter(context)
    print converter.parse_to_json()


# TODO 转换java option
# TODO concurrency以reader为准还是writer为准