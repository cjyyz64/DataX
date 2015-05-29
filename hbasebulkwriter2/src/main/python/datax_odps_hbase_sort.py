# !/usr/bin/env python
# encoding: utf-8
import json
from optparse import OptionParser
import os
import shutil
import sys
import uuid
from datax_util import OdpsUtil

__email__ = 'haosong.hhs@alibaba-inc.com'
__copyright__ = '2014 Alibaba Inc.'


def get_hbase_regions_json(dst_table, hbase_config, cluster_id, datax_home):
    path = "/tmp/regions_%s" % str(uuid.uuid1())
    cmd = "java -jar %s/plugin/writer/hbasebulkwriter2/datax-odps-hbase-udf.jar -t '%s' -c '%s' -l '%s' -o '%s' " \
          % (datax_home, dst_table, hbase_config, cluster_id, path)
    result = odpsutil.execute(cmd)
    assert result[0] == 0, "ERROR: Get HBase regions failed: %s ." % result[2]

    with open(path, "r") as regions_file:
        regions_str = regions_file.read()
        regions_json = json.loads(regions_str)
    assert regions_json is not None, "ERROR: System error, HBase regions list illegal, contact WangWang 'askdatax' for help . \n" \
                            "Detail: \n%s" % regions_json
    print regions_json
    cmd = "rm -rf %s" % path
    odpsutil.execute(cmd)

    return regions_json


def register_udf(regions, fun_name, res_file, datax_home):
    res_path = "/tmp/%s" % res_file
    with open(res_path, "w") as fp:
        fp.write(json.dumps(regions))

    odpsutil.add_res_path(res_path)
    tmp_udf_file = "datax-odps-hbase-udf.%s.jar" % res_file
    tmp_udf_path = r"%s/tmp/%s" % (datax_home, tmp_udf_file)
    shutil.copy("%s/plugin/writer/hbasebulkwriter2/datax-odps-hbase-udf.jar" % datax_home, tmp_udf_path)
    odpsutil.add_udf_jar(tmp_udf_path)
    odpsutil.create_udf_fun(fun_name, 'com.alibaba.datax.plugin.writer.hbasebulkwriter2.tools.HBaseRegionRouter',
                            tmp_udf_file, res_files=[res_file])


def create_tmp_table(src_table, tmp_table):
    odpsutil.drop_table(tmp_table)
    columns = []
    for column_tuple in odpsutil.get_column_tuple(src_table):
        columns.append("%s %s" % (column_tuple[0], column_tuple[1]))
    sql = "create table %s (%s) partitioned by(datax_pt string) lifecycle 5" % (tmp_table, ",".join(columns))
    odpsutil.execute_sql(sql, delay=False)


def to_udf_params(src_table, sort_column):
    column_dict = odpsutil.get_column_dict(src_table)
    params = []
    for tmp_column in sort_column.split(","):
        if ":" in tmp_column:
            (typ, name) = tmp_column.split(":")
        else:
            name = tmp_column
            typ = column_dict[name]
        if typ == "boolean":
            name = "to_char(%s)" % name
        params.append(name)
        params.append("'%s'" % typ)
    return params


def sort(src_table, partition, sort_column, tmp_table, regions_num, res_file, fun_name, bucket_num, dynamic_qualifier, rowkey_type):
    sort_columns = to_udf_params(src_table, sort_column)

    if dynamic_qualifier == "true":
        sort_field = "%s,%s,9223372036854775807-%s" % (sort_columns[0], sort_columns[2], sort_columns[4])
        region_location_fun = "%s(2, '%s', %s(1, %s, '%s'))" % (fun_name, res_file, fun_name, sort_columns[0], rowkey_type)
    else:
        rowkey_columns = ",".join(sort_columns)
        if not odpsutil.is_empty(bucket_num):
            rowkey_fun = "%s(3, %s, %s)" % (fun_name, bucket_num, rowkey_columns)
        else:
            rowkey_fun = "%s(1, %s)" % (fun_name, rowkey_columns)
        sort_field = rowkey_fun
        region_location_fun = "%s(2, '%s', %s)" % (fun_name, res_file, rowkey_fun)

    columns = []
    for column_tuple in odpsutil.get_column_tuple(src_table):
        columns.append(column_tuple[0])

    if not odpsutil.is_empty(partition):
        partition = "where %s" % partition

    sql = "set odps.sql.reshuffle.dynamicpt=false;\n" \
          "set odps.task.merge.enabled=false;\n" \
          "set odps.sql.reducer.instances=%d;\n" \
          "insert overwrite table %s partition(datax_pt) " \
          "select %s,%s as datax_pt from %s %s distribute by cast (datax_pt as bigint) sort by %s" \
          % (regions_num, tmp_table, ",".join(columns), region_location_fun, src_table, partition, sort_field)
    odpsutil.execute_sql(sql, delay=False)


if __name__ == "__main__":
    usage = '''python %s [options]''' % sys.argv[0]
    parser = OptionParser(usage=usage)
    parser.add_option("--project", dest="project", help="[ODPS] project name", metavar="project")
    parser.add_option("--src_table", dest="src_table", help="[ODPS] source table", metavar="src_table")
    parser.add_option("--sort_column", dest="sort_column", help="[ODPS] sort column", metavar="sort_column")
    parser.add_option("--dst_table", dest="dst_table", help="[HBase] dest table", metavar="dst_table")
    parser.add_option("--hbase_config", dest="hbase_config", help="[HBase] hbase-site.xml location",
                      metavar="hbase_config")
    parser.add_option("--cluster_id", dest="cluster_id", help="[HBase] cluster id", metavar="cluster_id", default="")

    parser.add_option("--suffix", dest="suffix", help="[ODPS][option] suffix", metavar="suffix", default="")
    parser.add_option("--partition", dest="partition", help="[ODPS][option] partition", metavar="partition", default="")
    parser.add_option("--bucket_num", dest="bucket_num", help="[ODPS][option] bucket number", metavar="bucket_num",
                      default="")
    parser.add_option("--dynamic_qualifier", dest="dynamic_qualifier", help="[ODPS][option] dynamic qualifier",
                      metavar="dynamic_qualifier", default="false")
    parser.add_option("--rowkey_type", dest="rowkey_type", help="[HBase]rowkey_type", metavar="rowkey_type", default="string")
    parser.add_option("--access_id", dest="access_id", help="[ODPS][option] odps access id", metavar="access_id",
                      default="")
    parser.add_option("--access_key", dest="access_key", help="[ODPS][option] odps access key", metavar="access_key",
                  default="")
    parser.add_option("--datax_home", dest="datax_home", help="[ODPS][option] datax home path", metavar="datax_home",
                      default="")

    (options, args) = parser.parse_args(sys.argv)

    odpsutil = OdpsUtil(options.project, options.access_id, options.access_key)

    if odpsutil.is_arr_contains_empty(
            [options.src_table, options.sort_column, options.dst_table]):
        print parser.format_help()
        sys.exit(-1)

    if not odpsutil.is_empty(options.partition):
        options.partition = " and ".join(odpsutil.get_partitions(options.partition))
    if not odpsutil.is_empty(options.suffix):
        options.suffix = "_" + options.suffix

    if odpsutil.is_empty(options.datax_home):
        options.datax_home = "/home/admin/datax3"

    res_file = "t_dx3_o2h_res_%s%s" % (options.src_table, options.suffix)
    fun_name = "t_dx3_o2h_fun_%s%s" % (options.src_table, options.suffix)
    tmp_table = "t_dx3_o2h_tbl_%s%s" % (options.src_table, options.suffix)

    create_tmp_table(options.src_table, tmp_table)
    regions = get_hbase_regions_json(options.dst_table, options.hbase_config, options.cluster_id, options.datax_home)
    register_udf(regions, fun_name, res_file, options.datax_home)
    sort(options.src_table, options.partition, options.sort_column, tmp_table, len(regions), res_file, fun_name,
         options.bucket_num, options.dynamic_qualifier,options.rowkey_type)
