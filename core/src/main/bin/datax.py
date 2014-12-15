#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# author: jingxing
# python: 2.7

"""
/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

import os
import sys
import time
import re
import errno
import json
import signal
import subprocess
import urllib2
import base64
import socket
from optparse import OptionParser
from string import Template

########## 全局配置 ###########
RET_STATE = {
    "KILL": -2,
    "FAIL": -1,
    "OK": 0,
    "RUN": 1,
    "RETRY": 2
}

def enum(**enums):
    return type('Enum', (), enums)
STRING_TYPE = enum(JSON="json", XML="xml", YAML="yaml")

DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_JVM = "-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log"%(DATAX_HOME)
YUNTI_JAVA_HOME = r"/home/yunti/java-current"
TAOBAO_JAVA_HOME = r"/opt/taobao/java"
ENGINE_COMMAND = "${JAVA_HOME}/bin/java -server -Dfile.encoding=UTF-8 ${jvm} -Ddatax.home=%s -classpath %s/lib/*:. ${params} com.alibaba.datax.core.Engine -job ${job}"%(DATAX_HOME, DATAX_HOME)
DEBUG_JVM = "-Xms1024m -Xmx1024m  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log"%(DATAX_HOME)
child_process = None

########## 函数 #############

def get_usage():
    return """Usage: %prog [-g] [-d] [-j jvm] [-p params] [-J jobId] [-t taskGroupId] job.json"""

def show_usage():
    print get_usage()
    sys.stdout.flush()

def get_ip():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


def get_option_parser():
    parser = OptionParser(usage = get_usage())
    parser.add_option('-g', '--gen', action="store_true", dest="gen", help='generate job config file.')
    parser.add_option('-d', '--delete', action="store_true", dest="delete", help='delete tmp job config file.')
    parser.add_option('-j', '--jvm', default="", dest="jvm", help='set jvm parameters.')
    parser.add_option('-p', '--params', default="", help='add DataX runtime parameters.')
    parser.add_option('-D', '--remotedebug', action="store_true", dest="remotedebug", help='use remote debug mode.')
    parser.add_option('-t', '--taskgroup', default="", dest="taskgroup", help='task group id')
    parser.add_option('-J', '--jobid', default="", dest="jobid", help='job id')
    parser.add_option('-m', '--mode', default=None, dest="mode", help='execute mode')

    return parser

def get_run_context(options):
    context = {}
    context["params"] = options.params if options.params else ""
    context["jvm"] = options.jvm if options.jvm else ""

    return context

def is_url(path):
    if not path:
        return False

    assert(isinstance(path, str))
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False

# 输入job_path，输出json格式的job路径
def get_json_job_path(job_id, mode, task_group_id, job_path, auth_user, auth_pass):
    if not job_path:
        print >>sys.stderr, "not give file or http address for job"
        sys.exit(RET_STATE["FAIL"])

    job_content = None
    is_job_from_http = False
    # 从http获取配置文件
    if is_url(job_path):
        is_job_from_http = True
        counter = 0
        while counter < 4:
            job_content = get_job_from_http(job_path, auth_user, auth_pass)
            if job_content:
                break
            time.sleep(2**counter)
            counter += 1
        if not job_content:
            print >>sys.stderr, "can not get job from http %s"%(job_path)
            sys.exit(RET_STATE["FAIL"])
    # 从本地文件读取配置
    else:
        job_path = os.path.abspath(job_path)
        file = open(job_path)
        try:
            job_content = file.read()
        finally:
            file.close()
        if not job_content:
            print >>sys.stderr, "get nothing from file %s"%(job_path)
            sys.exit(RET_STATE["FAIL"])

    is_resaved_json = False
    job_new_path = job_path
    job_json_content = None
    # 将非json配置转为json
    try:
        json.loads(job_content)
        job_json_content = job_content
    except:
        string_type = get_string_type(job_content)
        convertor = None
        if string_type == STRING_TYPE.XML:
            from xml_convertor import XmlConverter
            convertor = XmlConverter(job_content)
        else:
            print >>sys.stderr, "not support string type[%s]"%(string_type)
            sys.exit(RET_STATE["FAIL"])
        job_json_content = convertor.parse_to_json()
        if not job_json_content:
            print >>sys.stderr, "can not parse job conf to json"
            sys.exit(RET_STATE["FAIL"])
        if not is_job_from_http:
            job_new_path = save_to_tmp_file(job_id, task_group_id, job_path, False, job_json_content)
            is_resaved_json = True

    if is_job_from_http:
        # 把jobId 和 reportAddress写入配置中
        job_json_content = add_core_config_for_http(job_id, mode, task_group_id, job_json_content, job_path)
        if not job_json_content:
            print >>sys.stderr, "add core config for http error"
            sys.exit(RET_STATE["FAIL"])
        job_new_path = save_to_tmp_file(job_id, task_group_id, job_path, True, job_json_content)
        is_resaved_json = True

    return job_new_path, is_resaved_json

def add_core_config_for_http(job_id, mode, task_group_id, job_json_content, job_path):
    if not job_id:
        job_id = get_jobId_from_http(job_path)
    if job_id:
        job_json_content = json.loads(job_json_content)

        if not job_json_content.has_key("core"):
            job_json_content["core"] = {"container":{"job":{}}}

        job_json_content["core"]["container"]["job"]["id"] = job_id
        job_json_content["core"]["container"]["job"]["mode"] = mode

        # if not job_json_content.has_key("core"):
        #     job_json_content["core"]["container"]["job"]["id"] = job_id
        #
        # core_container_job_content_map = job_json_content["core"]["container"]["job"]
        # if not core_container_job_content_map.has_key("id"):
        #     job_json_content["core"]["container"]["job"]["id"] = job_id
        #
        # if not core_container_job_content_map.has_key("mode"):
        #     job_json_content["core"]["container"]["job"]["mode"] = mode

    else:
        return None

    return json.dumps(job_json_content, sort_keys=True, indent=4)

def get_jobId_from_http(job_path):
    m = re.match(r"^http[s]?://\S+/(\d+)\w*", job_path)
    if m:
        return m.group(1)
    else:
        print >>sys.stderr, "can not get job id from url[%s]"%(job_path)
        return None

# 根据job配置字符串判断其类型
def get_string_type(job_content):
    if job_content.find('''<?xml version="1.0" encoding="UTF-8"?>''') > -1:
        return STRING_TYPE.XML
    else:
        print >>sys.stderr, "can not get supported string type for string:\n%s"%(job_content)
        sys.exit(RET_STATE["FAIL"])

def save_to_tmp_file(job_id, task_group_id, job_path, is_job_from_http, job_json_content):
    assert(isinstance(job_path, str))

    tmp_file_path = None
    if is_job_from_http:
        if job_id:
            tmp_file_path = job_id
        else:
            tmp_file_path = get_jobId_from_http(job_path)

        if task_group_id:
            tmp_file_path = tmp_file_path + "-" + task_group_id
        if not tmp_file_path:
            sys.exit(RET_STATE["FAIL"])
    else:
        tmp_file_path = os.path.basename(job_path)

    run_day_time = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    tmp_dir = os.path.join(DATAX_HOME, "job", run_day_time)
    if not os.path.exists(tmp_dir):
        try:
            os.mkdir(tmp_dir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    tmp_file_path = os.path.join(tmp_dir, tmp_file_path + ".json")

    file = open(tmp_file_path, "w")
    try:
        file.write(job_json_content)
    finally:
        file.close()

    return tmp_file_path

def get_job_from_http(job_path, username, password):
    job_conf = None
    response = None

    try:
        # 这里的参数可能需要处理
        if not (job_path.endswith("/config") or job_path.endswith("/config.xml")):
            job_path = "%s/config" % job_path

        request = urllib2.Request(job_path)
        base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        response = urllib2.urlopen(request)
        job_conf = response.read()
    except Exception, ex:
        print >>sys.stderr, str(ex)
    finally:
        if response:
            response.close()

        return job_conf

def process_entry(json_path, run_context):
    entry_json = None
    file = open(json_path)
    try:
        file_context = file.read()
        all_json = json.loads(file_context)
        if "entry" in all_json:
            entry_json = all_json["entry"]
    finally:
        file.close()

    # 如果entry_json为空或没有设置jvm，使用默认jvm+命令行jvm
    if not entry_json or len(entry_json)<=0:
        run_context["jvm"] = "%s %s"%(DEFAULT_JVM, run_context["jvm"])
        run_context["JAVA_HOME"] = get_default_java_home()
        return run_context

    # 优先顺序为：命令行jvm > job_json jvm > default jvm
    if run_context["jvm"]:
        run_context["jvm"] = "%s %s"%(DEFAULT_JVM, run_context["jvm"])
    elif entry_json["jvm"]:
        run_context["jvm"] = "%s %s"%(DEFAULT_JVM, entry_json["jvm"])
    else:
        run_context["jvm"] = DEFAULT_JVM

    # JAVA_HOME处理，没有指定
    if not entry_json.has_key("environment") or "JAVA_HOME" not in entry_json["environment"].keys():
        default_java_home =  get_default_java_home()
        run_context["JAVA_HOME"] = default_java_home
        if not entry_json.has_key("environment"):
            entry_json["environment"] = {}
        entry_json["environment"]["JAVA_HOME"] = default_java_home
    else:
        java_home = entry_json["environment"]["JAVA_HOME"]
        if os.access("%s/bin/java"%(java_home), os.X_OK):
            run_context["JAVA_HOME"] = java_home
        else:
            print >>sys.stderr, "can not access pointed %s/bin/java"%(java_home)
            sys.exit(RET_STATE["FAIL"])

    # 以下是环境变量处理
    for key, value in entry_json["environment"].items():
        os.environ[key] = value

    return run_context

def get_default_java_home():
    if "JAVA_HOME" in os.environ:
        return os.environ["JAVA_HOME"]
    elif os.access("%s/bin/java"%(YUNTI_JAVA_HOME), os.X_OK):
        return YUNTI_JAVA_HOME
    elif os.access("%s/bin/java"%(TAOBAO_JAVA_HOME), os.X_OK):
        return TAOBAO_JAVA_HOME
    else:
        print >>sys.stderr, "no java could be used"
        sys.exit(RET_STATE["FAIL"])

def register_signal():
    global child_process
    signal.signal(2, suicide)
    signal.signal(3, suicide)
    signal.signal(15, suicide)

def suicide(signum, e):
    global child_process
    print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide."%(signum)

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    sys.exit(RET_STATE["KILL"])

# start to go
def get_auth_info(file_name):
    import StringIO
    import os
    import ConfigParser
    if not os.path.exists(file_name):
        return [None, None]
    with open(file_name) as f:
        fp = StringIO.StringIO()
        fp.write('[default_section]\n')
        fp.write(f.read())
        fp.seek(0, os.SEEK_SET)
        conf = ConfigParser.ConfigParser()
        conf.readfp(fp)
        if conf.has_option('default_section', 'auth.user'):
            return [conf.get('default_section', 'auth.user'),
                    conf.get('default_section', 'auth.pass')]
        else:
            return [None, None]


if __name__ == "__main__":
    # 解析option参数，其余返回到args中
    options, args = get_option_parser().parse_args(sys.argv[1:])
    # 生成运行上文配置
    run_context = get_run_context(options)

    # 判断是否为远程调试模式
    if options.remotedebug:
       DEFAULT_JVM = DEBUG_JVM
       print 'ip: ',get_ip()

    # 没有给job参数，直接返回使用说明
    if len(args) == 0:
        show_usage()
        sys.exit(RET_STATE["OK"])

    # 尝试获取读取url的认证信息
    auth_user, auth_pass = get_auth_info('%s/conf/.security.properties' % DATAX_HOME)

    # 获取job配置文件
    job_path, is_resaved_json = get_json_job_path(options.jobid, options.mode, options.taskgroup, args[0].strip(),
                                                  auth_user, auth_pass)
    # 处理entry相关配置
    run_context = process_entry(job_path, run_context)
    run_context["job"] = job_path

    os.chdir(DATAX_HOME)
    command = Template(ENGINE_COMMAND).substitute(**run_context)

     # print command
    child_process = subprocess.Popen(command, shell=True)
    register_signal()
    (stdout, stderr) = child_process.communicate()

    # 按要求删除临时生成的json文件
    if options.delete and is_resaved_json:
        os.remove(job_path)

    sys.exit(child_process.returncode)

