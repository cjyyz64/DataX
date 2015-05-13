# !/usr/bin/env python
# encoding: utf-8
__email__ = 'haosong.hhs@alibaba-inc.com'
__copyright__ = '2014 Alibaba Inc.'

from optparse import OptionParser
import sys
import os
from datax_util import OdpsUtil, HBaseUtil, Util


def init_constants(project, cluster_id, new_datax_home, new_datax_job_xml, access_id = "", access_key = ""):
    global odpsutil, hbaseutil
    odpsutil = OdpsUtil(project, access_id, access_key)
    hbaseutil = HBaseUtil(cluster_id)

    global datax_home, datax_run_scrpit, datax_hbasebulkwriter_home, datax_hbasebulkwriter_sort_script, datax_hbasebulkwriter_udf, datax_job_xml
    if not odpsutil.is_empty(new_datax_home):
        datax_home = new_datax_home
    else:
        datax_home = "/home/admin/datax3"

    datax_run_scrpit = "%s/bin/datax.py" % datax_home
    datax_hbasebulkwriter_home = "%s/plugin/writer/hbasebulkwriter" % datax_home
    datax_hbasebulkwriter_sort_script = "%s/datax_odps_hbase_sort.py" % datax_hbasebulkwriter_home
    datax_hbasebulkwriter_udf = "%s/tools/odps2hbase_cdh4/datax_odps_hbase_udf.jar" % datax_home

    if not odpsutil.is_empty(new_datax_job_xml):
        datax_job_xml = new_datax_job_xml
    else:
        datax_job_xml = "%s/datax_odps_hbase_job.xml" % datax_hbasebulkwriter_home


def get_odps_rowkey_column(hbase_rowkey, odps_column, rowkey_type):
    odps_columns = odps_column.split(",")
    rowkey_columns = []
    if not util.is_empty(hbase_rowkey):
        hbase_rowkeys = hbase_rowkey.split(",")
        for elem in hbase_rowkeys:
            items = elem.split("|")
            index = int(items[0])
            typ = items[1]
            if index != -1:
                rowkey_columns.append(typ + ":" + odps_columns[index])
            else:
                fixed = items[2]
                if typ == "string":
                    fixed = "'%s'" % fixed
                rowkey_columns.append(typ + ":" + fixed)
    else:
        assert len(odps_columns) == 4, "Odps column length should equal to 4 in dynamic mode."
        rowkey_columns.append(rowkey_type + ":" + odps_columns[0])
        rowkey_columns.append("string:" + odps_columns[1])
        rowkey_columns.append("bigint:" + odps_columns[2])
    return ",".join(rowkey_columns)


def exec_sort_script(project, src_table, odps_column,
                     dst_table, hbase_rowkey, hbase_config, cluster_id,
                     partition, suffix, bucket_num, dynamic_qualifier, rowkey_type, access_id="", access_key="", datax_home=""):
    phase_name = "Sort Script"
    util.log_phase(phase_name)

    rowkey_column = get_odps_rowkey_column(hbase_rowkey, odps_column, rowkey_type)
    cmd = "python %s --project '%s' --src_table '%s' --sort_column '%s' --partition '%s'" \
          " --dst_table '%s' --hbase_config '%s' --cluster_id '%s' --suffix '%s' --bucket_num '%s' --dynamic_qualifier '%s'" \
          " --access_id '%s' --access_key '%s' --datax_home '%s' --rowkey_type '%s'" \
          % (datax_hbasebulkwriter_sort_script, project, src_table, rowkey_column, partition,
             dst_table, hbase_config, cluster_id, suffix, bucket_num, dynamic_qualifier, access_id, access_key, datax_home, rowkey_type)

    result = util.execute(cmd)
    assert result[0] == 0, "Execute %s Failed." % cmd

    util.log_phase(phase_name, is_end=True)


def exec_datax_job(project, src_table, odps_column, suffix,
                   dst_table, hbase_rowkey, hbase_column, hdfs_config, hbase_config, cluster_id, hbase_output, concurrency,
                   bucket_num, time_col, start_ts, null_mode, dynamic_qualifier, rowkey_type, truncate,
                   access_id, access_key, tunnel_server, odps_server,compress):
    phase_name = "DataX: ODPSWriter -> HBaseBulkWriter"
    util.log_phase(phase_name)

    jobJSON = build_datax_job_config(project, src_table, odps_column, suffix,
                   dst_table, hbase_rowkey, hbase_column, hdfs_config, hbase_config, cluster_id, hbase_output, concurrency,
                   bucket_num, time_col, start_ts, null_mode, dynamic_qualifier, rowkey_type, truncate,
                   access_id, access_key, tunnel_server, odps_server,compress)
    jobConfigPath = 'datax_hbasebulkwriter_job.json'
    fileHandler = open(jobConfigPath,'w')
    fileHandler.write(jobJSON)
    fileHandler.close()

    cmd = "python %s %s" % (datax_run_scrpit, jobConfigPath)

    result = util.execute(cmd)
    assert result[0] == 0, "Execute %s Failed." % cmd

    odpsutil.drop_table(src_table)

    util.log_phase(phase_name, is_end=True)

def build_datax_job_config(project, src_table, odps_column, suffix,
                   dst_table, hbase_rowkey, hbase_column, hdfs_config, hbase_config, cluster_id, hbase_output, concurrency,
                   bucket_num, time_col, start_ts, null_mode, dynamic_qualifier, rowkey_type, truncate,
                   access_id, access_key, tunnel_server, odps_server,compress):
    if not odpsutil.is_empty(suffix):
        suffix = "_" + suffix
    src_table = "t_datax_odps2hbase_table_%s%s" % (src_table, suffix)

    accessIdJSON = accessKeyJSON = serverJSON = tunnelJSON = ''
    #build json config for odpsReader
    if not odpsutil.is_empty(access_id):
      accessIdJSON = '"accessId" : "' + access_id + '",'
    if not odpsutil.is_empty(access_key):
      accessKeyJSON = '"accessKey" : "' + access_key + '",'
    if os.environ.get('SKYNET_SYSTEMID', '') == '':
      projectName = os.environ.get('SKYNET_PACKAGEID', '')
    else:
      projectName = os.environ.get('SKYNET_PACKAGEID', '') + '_' + os.environ.get('SKYNET_SYSTEMID', '')
    ## 测试环境没有SKYNET环境变量
    if odpsutil.is_empty(projectName):
      projectName = project
    projectJSON = '"project" : "' + projectName + '",'
    tableJSON = '"table" : "' + src_table + '",'
    columnJSON = '"column" : [' 
    columnArray = odps_column.split(',')
    for columnElement in columnArray:
      columnJSON = columnJSON + '"' + columnElement + '", '
    columnJSON = columnJSON + '],'
    partitionJSON = '"partition" : ["datax_pt=*"],'
    splitJSON = '"splitMode" : "record",'
    compressJSON = '"isCompress" : "' + compress + '",'
    if not odpsutil.is_empty(odps_server):
      serverJSON = '"odpsServer" : "' + odps_server + '",'
    if not odpsutil.is_empty(tunnel_server):
      tunnelJSON = '"tunnelServer" : "' + tunnel_server + '"'
    parameterJSON = '"parameter" : {' + accessIdJSON + accessKeyJSON + projectJSON + tableJSON + columnJSON + partitionJSON + splitJSON + compressJSON + serverJSON + tunnelJSON + '}'
    readerJSON = '"reader" : { "name" : "odpsreader", ' + parameterJSON + '},' 
    
    #build json config for hbasebulkwriter
    if(dynamic_qualifier == "true"):
      #dynamiccolumn
      tableJSON = '"hbase_table" : "' + dst_table + '",'
      rowkeyJSON = '"rowkey_type" : "' + rowkey_type + '",'
      columnJSON = ''
      columnArray = hbase_column.split(',')
      for columnElement in columnArray:
        htype, pattern = columnElement.split('|',1)
        columnJSON = columnJSON + '{"pattern" : "' + pattern + '", "htype" : "' + htype + '"}, '
      columnJSON = '"hbase_column" : { "type" : "prefix", "rules" : [' + columnJSON + ']},'
      outputJSON = '"hbase_output" : "' + hbase_output + '",'
      hbaseJSON = '"hbase_config" : "' + hbase_config + '",'
      hdfsJSON = '"hdfs_config" : "' + hdfs_config + '"'
      dynamicJSON = '"dynamiccolumn" : {' + tableJSON + rowkeyJSON + columnJSON + outputJSON + hbaseJSON + hdfsJSON + '}'
      writerJSON = dynamicJSON
    else:
      #fixedcolumn
      tableJSON = '"hbase_table" : "' + dst_table + '",'
      rowkeyJSON = ''
      rowkeyArray = hbase_rowkey.split(',')
      for rowkeyElement in rowkeyArray:
        if(rowkeyElement.startswith("-1")):
          index, htype, constant = rowkeyElement.split('|',2)
          rowkeyJSON = rowkeyJSON + '{"index" : ' + index + ', "htype" : "' + htype + '", "constant" : "' + constant + '"},'
        else:
          index, htype = rowkeyElement.split('|',1)
          rowkeyJSON = rowkeyJSON + '{"index" : ' + index + ', "htype" : "' + htype + '"},'
      rowkeyJSON = '"hbase_rowkey" : [' + rowkeyJSON + '],'
      columnJSON = ''
      columnArray = hbase_column.split(',')
      for columnElement in columnArray:
        index, htype, hname = columnElement.split('|',2)
        columnJSON = columnJSON + '{"index" : "' + index + '", "hname" : "' + hname + '", "htype" : "' + htype + '"}, '
      columnJSON = '"hbase_column" : [' + columnJSON + '],'
      outputJSON = '"hbase_output" : "' + hbase_output + '",'
      hbaseJSON = '"hbase_config" : "' + hbase_config + '",'
      hdfsJSON = '"hdfs_config" : "' + hdfs_config + '",'
      if not odpsutil.is_empty(null_mode):
        null_mode = '"null_mode" : "' + null_mode + '",'
      if not odpsutil.is_empty(bucket_num):
        bucket_num = '"bucket_num" : "' + bucket_num + '",'
      if not odpsutil.is_empty(start_ts):
        start_ts = '"start_ts" : "' + start_ts + '",'
      if not odpsutil.is_empty(time_col):
        time_col = '"time_col" : "' + time_col + '"'
      optionJSON = '"optional" : {' + null_mode + bucket_num + start_ts + time_col + '}'
      fixedJSON = '"fixedcolumn" : {' + tableJSON + rowkeyJSON + columnJSON + outputJSON + hbaseJSON + hdfsJSON + optionJSON + '}' 
      writerJSON = fixedJSON
      
    writerJSON = '"writer" : { "name" : "hbasebulkwriter", "parameter" : {' + writerJSON + '}}'
    contentJSON = '"content" : [ { ' + readerJSON + writerJSON + ' } ]'
    settingJSON = '"setting" : { "speed" : { "channel" : 5 } }, '
    jobJSON = '{ "job" : {' + settingJSON + contentJSON + '}}'
    print 'job config in JSON : ' + jobJSON
    return jobJSON
      
def run(project, src_table, odps_column,
        dst_table, hbase_rowkey, hbase_column, hbase_output, concurrency,
        partition, suffix, bucket_num, time_col, start_ts, null_mode, dynamic_qualifier, rowkey_type, truncate,
        hdfs_config, hbase_config, cluster_id,
        access_id, access_key, tunnel_server, odps_server, datax_home, datax_job_xml,compress):
    init_constants(project, cluster_id, datax_home, datax_job_xml, access_id, access_key)
    if util.is_empty(hdfs_config):
        hdfs_config = hbaseutil.generate_conf("hdfs-site.xml")
    if util.is_empty(hbase_config):
        hbase_config = hbaseutil.generate_conf("hbase-site.xml")

    exec_sort_script(project, src_table, odps_column,
                     dst_table, hbase_rowkey, hbase_config, cluster_id,
                     partition, suffix, bucket_num, dynamic_qualifier, rowkey_type, access_id, access_key, datax_home)

    exec_datax_job(project, src_table, odps_column, suffix,
                   dst_table, hbase_rowkey, hbase_column, hdfs_config, hbase_config, cluster_id, hbase_output, concurrency,
                   bucket_num, time_col, start_ts, null_mode, dynamic_qualifier, rowkey_type, truncate,
                   access_id, access_key, tunnel_server, odps_server,compress)


if __name__ == "__main__":
    usage = '''python %s [options]''' % sys.argv[0]
    parser = OptionParser(usage=usage)
    parser.add_option("--project", dest="project", help="[ODPS] project name", metavar="project")
    parser.add_option("--src_table", dest="src_table", help="[ODPS] source table", metavar="src_table")
    parser.add_option("--odps_column", dest="odps_column", help="[ODPS] odps column", metavar="odps_column")
    parser.add_option("--dst_table", dest="dst_table", help="[HBase] dest table", metavar="dst_table")
    parser.add_option("--hbase_column", dest="hbase_column", help="[HBase] column rule", metavar="hbase_column")
    parser.add_option("--hbase_rowkey", dest="hbase_rowkey", help="[HBase] rowkey rule", metavar="hbase_rowkey", default="")
    parser.add_option("--hbase_output", dest="hbase_output", help="[HBase] hfile output dir", metavar="hbase_output")
    parser.add_option("--concurrency", dest="concurrency", help="concurrency", metavar="concurrency")

    parser.add_option("--partition", dest="partition", help="[HBase][option] partition", metavar="partition", default="")
    parser.add_option("--suffix", dest="suffix", help="[HBase][option] suffix", metavar="suffix", default="")
    parser.add_option("--bucket_num", dest="bucket_num", help="[HBase][option] bucket number", metavar="bucket_num", default="")
    parser.add_option("--time_col", dest="time_col", help="[HBase][option] time column", metavar="time_col", default="")
    parser.add_option("--start_ts", dest="start_ts", help="[HBase][option] start timestamp", metavar="start_ts", default="")
    parser.add_option("--null_mode", dest="null_mode", help="[HBase][option] how to deal with null", metavar="null_mode", default="EMPTY_BYTES")
    parser.add_option("--dynamic_qualifier", dest="dynamic_qualifier", help="[HBase][option] dynamic qualifier",
                      metavar="dynamic_qualifier", default="false")
    parser.add_option("--rowkey_type", dest="rowkey_type", help="[HBase][option] dynamic qualifier rowkey type",
                      metavar="rowkey_type", default="")
    parser.add_option("--truncate", dest="truncate", help="[HBase][option] truncate data in hbase",
                      metavar="truncate", default="false")

    parser.add_option("--hdfs_config", dest="hdfs_config", help="[HBase] hdfs-site.xml location", metavar="hdfs_config",
                      default="")
    parser.add_option("--hbase_config", dest="hbase_config", help="[HBase] hbase-site.xml location",
                      metavar="hbase_config", default="")
    parser.add_option("--cluster_id", dest="cluster_id", help="[HBase] cluster id", metavar="cluster_id", default="")

    parser.add_option("--access_id", dest="access_id", help="[ODPS][option] odps access id", metavar="access_id",
                      default="")
    parser.add_option("--access_key", dest="access_key", help="[ODPS][option] odps access key", metavar="access_key",
                      default="")
    parser.add_option("--tunnel_server", dest="tunnel_server", help="[ODPS][option] odps tunnel server",
                      metavar="tunnel_server", default="")
    parser.add_option("--odps_server", dest="odps_server", help="[ODPS][option] odps endpoint", metavar="odps_server",
                      default="")
    parser.add_option("--datax_home", dest="datax_home", help="[ODPS][option] datax home path", metavar="datax_home",
                      default="")
    parser.add_option("--datax_job_xml", dest="datax_job_xml", help="[ODPS][option] datax job xml path", metavar="datax_job_xml",
                      default="")
    parser.add_option("--compress", dest="compress", help="[ODPS][option] odps reader isCompress", metavar="compress",
                          default="false")

    (options, args) = parser.parse_args(sys.argv)

    util = Util()

    if util.is_arr_contains_empty([options.project, options.src_table, options.odps_column,
                                   options.dst_table, options.hbase_output]):
        print parser.format_help()
        sys.exit(-1)

    sys.exit(run(options.project, options.src_table, options.odps_column,
                 options.dst_table, options.hbase_rowkey, options.hbase_column, options.hbase_output, options.concurrency,
                 options.partition, options.suffix, options.bucket_num, options.time_col, options.start_ts, options.null_mode, options.dynamic_qualifier, options.rowkey_type, options.truncate,
                 options.hdfs_config, options.hbase_config, options.cluster_id,
                 options.access_id, options.access_key, options.tunnel_server, options.odps_server,
                 options.datax_home, options.datax_job_xml, options.compress))
