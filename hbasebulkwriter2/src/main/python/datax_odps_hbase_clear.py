# !/usr/bin/env python
# encoding: utf-8
__email__ = 'haosong.hhs@alibaba-inc.com'
__copyright__ = '2014 Alibaba Inc.'

from optparse import OptionParser
import sys
import os
from datax_util import OdpsUtil, HBaseUtil, Util

if __name__ == "__main__":
    usage = '''python %s [options]''' % sys.argv[0]
    parser = OptionParser(usage=usage)
    parser.add_option("--project", dest="project", help="[ODPS] project name", metavar="project")
    parser.add_option("--table", dest="table", help="[ODPS]  table", metavar="table")
    parser.add_option("--access_id", dest="access_id", help="[ODPS][option] odps access id", metavar="access_id",
                  default="")
    parser.add_option("--access_key", dest="access_key", help="[ODPS][option] odps access key", metavar="access_key",
                  default="")

    (options, args) = parser.parse_args(sys.argv)

    util = Util()

    if util.is_arr_contains_empty([options.project, options.table, options.access_id, options.access_key]):
        print parser.format_help()
        sys.exit(-1)

    odpsutil = OdpsUtil(options.project, options.access_id, options.access_key)

    odpsutil.drop_table(options.table)
    util.log_phase("HBaseBulkWriter2 Clear ODPS tmp Table", is_end=True)
    sys.exit()
