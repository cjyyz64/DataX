#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# author: jingxing
# python: 2.7

import json
import time

try:
    from xml.etree import ElementTree as XmlTree
except:
    import elementtree.ElementTree as XmlTree

class XmlConvertor:
    def __init__(self, job_string):
        self.root = XmlTree.fromstring(job_string)
        self.reader = self.root.find("job/reader")
        self.writer = self.root.find("job/writer")
        if self.reader is None or self.writer is None:
            raise Exception("reader or writer can not be none")

        self.job_setting = {}
        self.reader_dict = {}
        self.writer_dict = {}

    # 返回json对象
    def parse_to_json(self):
        if not self.parse_reader():
            print >>sys.stderr, "parse reader error"
            return None

        if not self.parse_writer():
            print >>sys.stderr, "parse writer error"
            return None

        job_json = {}
        job_json["setting"] = self.job_setting
        job_json["content"] = [{"reader": self.reader_dict, "writer": self.writer_dict}]
        return json.dumps({"job": job_json}, sort_keys=True, indent=4)
    
    def parse_reader(self):
        reader_plugin = self.reader.find("plugin").text
        self.reader_dict["name"] = reader_plugin
        if reader_plugin == "streamreader":
            return self.parse_streamreader()
        else:
            print >>sys.stderr, "not suppotted reader plugin[%s]"%(reader_plugin)
            return False

    def parse_writer(self):
        writer_plugin = self.writer.find("plugin").text
        self.writer_dict["name"] = writer_plugin
        if writer_plugin == "streamwriter":
            return self.parse_streamwriter()
        else:
            print >>sys.stderr, "not suppotted writer plugin[%s]"%(writer_plugin)
            return False

    ############### 这里有问题，无法获取param的值  ############
    def get_value_from_xml(self, node_root, key):
        value = None
        try:
            for item in node_root.iter(tag="param"):
                if item.attrib["key"] == key:
                    value = item.attrib["value"]
                    break
        finally:
            return value

    def parse_streamreader(self):
        self.job_setting["speed"] = 10485760
        self.job_setting["errorLimit"] = 0.0

        parameter = {}
        self.reader_dict["parameter"] = parameter
        sliceRecordCount = self.get_value_from_xml(self.reader, "record-count")
        if sliceRecordCount:
            parameter["sliceRecordCount"] = sliceRecordCount
        else:
             parameter["sliceRecordCount"] = 100000
        
        if self.get_value_from_xml(self.reader, "column"):
            columns = []
            parameter["column"] = columns
            columns.append({"value":"filed", "type":"string"})
            columns.append({"value":100, "type":"int"})
            columns.append({"value":int(time.time()), "type":"date"})
            columns.append({"value":True, "type":"bool"})
            columns.append({"value":"byte string", "type":"byte"})
        else:
            field_delimiter = self.get_value_from_xml(self.reader, "field-delimiter")
            if not field_delimiter:
                field_delimiter = ","
            parameter["fieldDelimiter"] = field_delimiter

        return True

    def parse_streamwriter(self):
        parameter = {}
        self.writer_dict["parameter"] = parameter

        is_visible = self.get_value_from_xml(self.writer, "visible")
        if is_visible and "true"==is_visible.lower():
            parameter["visible"] = True
            encoding = self.get_value_from_xml(self.writer, "encoding")
            if not encoding:
                encoding = "UTF-8"
            parameter["encoding"] = encoding
            field_delimiter = self.get_value_from_xml(self.writer, "field-delimiter")
            if not field_delimiter:
                field_delimiter = ","
            parameter["fieldDelimiter"] = field_delimiter
        else:
            parameter["visible"] = False

        return True

if __name__ == "__main__":
    file = open("job.xml")
    context = file.read()
    file.close()

    convertor = XmlConvertor(context)
    print convertor.parse_to_json()
