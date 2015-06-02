# !/usr/bin/env python
# encoding: utf-8
import json
import os
import subprocess
import sys
import time
import urllib2
import uuid

__email__ = 'haosong.hhs@alibaba-inc.com'
__copyright__ = '2014 Alibaba Inc.'

__metaclass__ = type


class Util:
    def __init__(self):
        pass

    def execute(self, cmd, verbose=True, direct=True):
        """Execute shell

        Args:
            cmd (str): command

        Kwargs:
            verbose (bool): whether print stdoutdata or not
            direct (bool): whether print output to std or not

        Returns:
            tuple. contains returncode, stdoutdata, stderrdata
        """
        if verbose:
            self.log("Execute %s ." % cmd)

        if direct:
            p = subprocess.Popen(cmd, shell=True)
        else:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (stdoutdata, stderrdata) = p.communicate()

        if verbose:
            if stdoutdata is not None:
                self.log(stdoutdata)
            if p.returncode != 0:
                self.log(stderrdata)

        return p.returncode, stdoutdata, stderrdata

    def log(self, msg):
        print >> sys.stdout, "[%s] - %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), msg)
        sys.stdout.flush()
        return

    def header_length(self, msg):
        if msg is None or len(msg.strip()) == 0:
            return 0

        max_len = 0
        for line in msg.splitlines():
            if len(line) > max_len:
                max_len = len(line)
        return max_len

    def header(self, msg):
        n = self.header_length(msg)

        msg = ""
        for i in xrange(n):
            msg += "="
        return msg

    def notice(self, msg):
        print >> sys.stdout, "\n" + self.header(msg)
        print >> sys.stdout, msg
        print >> sys.stdout, self.header(msg) + "\n"
        return

    def log_phase(self, phase_name, is_end=False):
        if is_end:
            self.notice("%s Phase %s" % ("Finish", phase_name))
        else:
            self.notice("%s Phase %s" % ("Start", phase_name))

    def is_empty(self, s):
        return (s is None) or (s.strip() == "")

    def is_arr_contains_empty(self, arr):
        result = False
        for s in arr:
            if self.is_empty(s):
                result = True
                break
        return result

    def get_partitions(self, mutil_partition_str):
        partitions = []
        if self.is_empty(mutil_partition_str):
            return partitions
        for part in mutil_partition_str.split(r'/'):
            kv = part.split('=')
            partitions.append("%s='%s'" % (kv[0].strip(), kv[1].strip()))
        return partitions

    def get_partition_keys(self, mutil_partition_str):
        keys = []
        if self.is_empty(mutil_partition_str):
            return keys
        for part in mutil_partition_str.split(r'/'):
            kv = part.split('=')
            keys.append(kv[0].strip())
        return keys


class HBaseUtil(Util):
    def __init__(self, cluster_id):
        super(HBaseUtil, self).__init__()
        self.cluster_id = cluster_id
        self.conf_json_cache = {}

    def generate_conf(self, conf_name):
        result = self.get_conf_json(conf_name)
        p = """<property>\n<name>%s</name>\n<value>%s</value>\n</property>\n"""
        xml = ""
        for k in result.keys():
            xml += p % (k, result[k])
        xml = "<configuration>\n%s</configuration>" % xml
        path = "/tmp/%s_%s" % (conf_name, str(uuid.uuid1()))
        with open(path, "w") as fp:
            fp.write(xml)
        return path

    def get_conf_json(self, conf_name):
        if conf_name not in self.conf_json_cache:
            api = "http://hmc.alibaba-inc.com/hmcServer/service/clusterConfig/get?clusterId=%s&confName=%s"
            resp = json.loads(urllib2.urlopen(api % (self.cluster_id, conf_name)).read())
            assert resp["returnCode"] == 0, "Get conf failed: %s" % resp["errorMessage"]

            result = resp["result"]
            assert result.keys() != 0, "Get conf failed: %s is empty in HMC." % conf_name

            self.conf_json_cache[conf_name] = result

        return self.conf_json_cache[conf_name]

    def get_rootdir(self):
        result = self.get_conf_json("hbase-site.xml")
        rootdir = result["hbase.rootdir"]
        assert not self.is_empty(rootdir), "rootdir is empty."

        return rootdir


class OdpsUtil(Util):
    def __init__(self, project="", access_id="", access_key=""):
        super(OdpsUtil, self).__init__()
        self.delay_sqls = []
        self.column_json_cache = {}

        if os.getenv('SKYNET_SYSTEM_ENV', '').lower() == 'local':
            self.odpscmd = '''/home/admin/odps_clt/bin/odpscmd --project=%s -u %s -p %s ''' % (project, access_id, access_key)
            return

        if os.getenv('SKYNET_SYSTEM_ENV', '').lower() == 'test':
            self.odpscmd = '''/home/taobao/odps_clt/bin/odpscmd'''
            return

        odpscmd = '''/opt/taobao/tbdpapp/odpswrapper/odpswrapper.py'''
        #if self.is_empty(project):
        #    if os.getenv('SKYNET_SYSTEM_ENV', 'dev').lower() == 'dev':
        #        project = "${SKYNET_PACKAGEID}_${SKYNET_SYSTEMID}"
        #    else:
        #        project = "${SKYNET_PACKAGEID}"
        #self.odpscmd = "%s --project=%s " % (odpscmd, project)
        self.odpscmd = odpscmd

    def execute_sql(self, sql, be_json=False, verbose=True, direct=True, delay=True):
        """
            Execute ODPS SQL

        Args:
            odpscmd (str): odpscmd
            sql (str): sql

        Kwargs:
            be_json (bool): output as json or not
            verbose (bool): whether print stdoutdata or not
            direct (bool): whether print output to std or not

        Returns:
            tuple. contains returncode, stdoutdata, stderrdata

        """

        if delay:
            self.delay_sqls.append(sql)
            return

        # single sql command
        sql = '\"' + sql + ';\"'

        if be_json:
            cmd = self.odpscmd + " -j -e " + sql + ""  # background
        else:
            cmd = self.odpscmd + " -e " + sql + ""  # background

        self.log("Use SQL: %s ." % (sql.strip()))

        result = self.execute(cmd, verbose, direct)
        if result[0] != 0:
            raise RuntimeError("Execute %s Failed." % sql)

        return result

    def execute_delay_sqls(self, msg):
        sql = ";".join(self.delay_sqls)
        self.log("Delay SQLs:" + sql)
        self.execute_sql(sql, delay=False)
        self.notice(msg)

    def get_column_json(self, table, refresh=False):
        if table not in self.column_json_cache or refresh:
            sql = "desc %s" % table
            output = self.execute_sql(sql, be_json=True, direct=False, delay=False)
            column_json = json.loads(output[1])["columns"]
            self.column_json_cache[table] = column_json

        return self.column_json_cache[table]

    def get_column_dict(self, table, refresh=False):
        column_dict = {}
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_dict[i["name"]] = i["type"]
        return column_dict

    def get_column_tuple(self, table, refresh=False):
        column_tuples = []
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_tuples.append((i["name"], i["type"]))
        return column_tuples

    def get_mapping_hbase_column_str(self, family, table):
        hbase_columns = []
        column_json = self.get_column_json(table)
        i = 0
        for c in column_json:
            hbase_columns.append("%s|%s|%s:%s" % (i, c["type"], family, c["name"]))
            i += 1
        return ",".join(hbase_columns)

    def drop_table(self, table, delay=False):
        sql = "drop table if exists %s" % table
        self.execute_sql(sql, delay=delay)

    def rename_table(self, old_table, new_table, delay=False):
        self.drop_table(new_table, delay=delay)
        sql = "alter table %s rename to %s" % (old_table, new_table)
        self.execute_sql(sql, delay=delay)

    def add_udf_jar(self, udf_path, delay=False):
        sql = "add jar %s -f" % udf_path
        self.execute_sql(sql, delay=delay)

    def create_udf_fun(self, fun_name, class_name, udf_file, res_files=None, delay=False):
        if not res_files:
            res_files = []
        res_files.append(udf_file)

        sql = "drop function %s;create function %s as '%s' using '%s'" % (
            fun_name, fun_name, class_name, ",".join(res_files))
        self.execute_sql(sql, delay=delay)

    def drop_udf_fun(self, fun_name, delay=False):
        sql = "drop function %s" % fun_name
        self.execute_sql(sql, delay=delay)

    def drop_res(self, res_nanme, delay=False):
        sql = "drop resource %s" % res_nanme
        self.execute_sql(sql, delay=delay)

    def add_res_path(self, res_path, delay=False):
        sql = "add file %s -f" % res_path
        self.execute_sql(sql, delay=delay)

    def get_keys(self, table, refresh=False):
        column_keys = []
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_keys.append((i["name"]))
        return column_keys

    def get_normal_keys(self, table, mutil_partition_str):
        normal_keys = []
        keys = self.get_keys(table)
        partition_keys = self.get_partition_keys(mutil_partition_str)
        for k in keys:
            if k not in partition_keys:
                normal_keys.append(k)
        return normal_keys

class HiveUtil(Util):
    def __init__(self):
        super(HiveUtil, self).__init__()
        self.delay_sqls = ["set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.dynamic.partition=true"]
        self.column_json_cache = {}

        if os.getenv('SKYNET_SYSTEM_ENV', '').lower() == 'test':
            self.hivecmd = '''/home/haosong.hhs/develop/code/hbaselogreader/hive/bin/hive -u root -p root'''
            return

        self.hivecmd = '''/home/hive/hive/bin/hive'''

    def execute_sql(self, sql, be_json=False, verbose=True, direct=True, delay=True):
        """
            Execute ODPS SQL

        Args:
            odpscmd (str): odpscmd
            sql (str): sql

        Kwargs:
            be_json (bool): output as json or not
            verbose (bool): whether print stdoutdata or not
            direct (bool): whether print output to std or not

        Returns:
            tuple. contains returncode, stdoutdata, stderrdata

        """

        if delay:
            self.delay_sqls.append(sql)
            return

        # single sql command
        sql = '\"' + sql + ';\"'
        cmd = self.hivecmd + " -e " + sql + ""  # background

        self.log("Use SQL: %s ." % (sql.strip()))

        result = self.execute(cmd, verbose, direct)
        if result[0] != 0:
            raise RuntimeError("Execute %s Failed." % sql)

        # for compatible
        if be_json:
            output = result[1]
            assert "Table no does not exist" not in output, "Table isn't exist."
            json_output = []
            splits = output.split()
            for i in range(0, len(splits) / 2):
                column = splits[2 * i]
                typ = splits[2 * i + 1]
                json_output.append("{\"name\": \"%s\", \"type\": \"%s\"}" % (column, typ))
            result = (result[1], "{\"columns\":[%s]}" % ",".join(json_output), result[2])

        return result

    def execute_delay_sqls(self, msg):
        sql = ";".join(self.delay_sqls)
        self.log("Delay SQLs:" + sql)
        self.execute_sql(sql, delay=False)
        self.notice(msg)

    def get_column_json(self, table, refresh=False):
        if table not in self.column_json_cache or refresh:
            sql = "desc %s" % table
            output = self.execute_sql(sql, be_json=True, direct=False, delay=False)
            column_json = json.loads(output[1])["columns"]
            self.column_json_cache[table] = column_json

        return self.column_json_cache[table]

    def get_column_dict(self, table, refresh=False):
        column_dict = {}
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_dict[i["name"]] = i["type"]
        return column_dict

    def get_column_tuple(self, table, refresh=False):
        column_tuples = []
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_tuples.append((i["name"], i["type"]))
        return column_tuples

    def get_mapping_hbase_column_str(self, family, table):
        hbase_columns = []
        column_json = self.get_column_json(table)
        i = 0
        for c in column_json:
            hbase_columns.append("%s|%s|%s:%s" % (i, c["type"], family, c["name"]))
            i += 1
        return ",".join(hbase_columns)

    def drop_table(self, table, delay=False):
        sql = "drop table %s" % table
        self.execute_sql(sql, delay=delay)

    def rename_table(self, old_table, new_table, delay=False):
        self.drop_table(new_table, delay=delay)
        sql = "alter table %s rename to %s" % (old_table, new_table)
        self.execute_sql(sql, delay=delay)

    def add_udf_jar(self, udf_path, delay=False):
        sql = "add jar %s" % udf_path
        self.execute_sql(sql, delay=delay)

    def create_udf_fun(self, fun_name, class_name, delay=False):
        sql = "drop temporary function %s;create temporary function %s as '%s'" % (fun_name, fun_name, class_name)
        self.execute_sql(sql, delay=delay)

    def drop_udf_fun(self, fun_name, delay=False):
        sql = "drop temporary function %s" % fun_name
        self.execute_sql(sql, delay=delay)

    def get_keys(self, table, refresh=False):
        column_keys = []
        column_json = self.get_column_json(table, refresh=refresh)
        for i in column_json:
            column_keys.append((i["name"]))
        return column_keys

    def get_normal_keys(self, table, mutil_partition_str):
        normal_keys = []
        keys = self.get_keys(table)
        partition_keys = self.get_partition_keys(mutil_partition_str)
        for k in keys:
            if k not in partition_keys:
                normal_keys.append(k)
        return normal_keys