#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
/**
 *  (C) 2010-2013 Alibaba Group Holding Limited.
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
import signal
import re
import subprocess
import os.path
import urllib2

#For python 2.4 & 2.6
try:
    from xml.etree import ElementTree as XmlTree
except:
    import elementtree.ElementTree as XmlTree

from optparse import OptionParser
from string import Template

RET_OK = 0

RET_FAIL_IN_RETRY = 2

RET_FAIL_IN_KILL = -1

GREP_SID_PATTERN = re.compile(".*sid=(\d+)")

YUNTI_JAVA_HOME = r'/home/yunti/java-current'

YUNTI_JAVA_BIN = YUNTI_JAVA_HOME + r'/bin/java'

dataXHome = "."

engineCmd='''java ${debug} -Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/taobao/datax/logs ${jvm} -Djava.ext.dirs=${libs} -Djava.library.path=${shared} ${params} -jar ${jar} -conf ${conf} -job ${job}'''

debug_config = '''-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address="9999"'''

genCmd='''java -Djava.ext.dirs=${libs} -jar ${jar} -edit'''

IS_DEBUG = os.environ.get('DATAX_DEBUG', None)

runMode = ''

childProcess = None

def getCopyRight(core):
    copyright = """
DataX V 3.0, Build %s. From Alibaba
Copyright (C) 2010-2014, Alibaba Group. All Rights Reserved.
"""
    return copyright % (getBuildVersion(core))

def showCopyRight(core):
    print(getCopyRight(core))
    sys.stdout.flush()
    return

#if we has yunti-jdk, use it as default
def setJavaPath():
    if os.access(YUNTI_JAVA_BIN, os.X_OK):
        os.environ[r"JAVA_HOME"] = YUNTI_JAVA_HOME
        os.environ[r"PATH"] = YUNTI_JAVA_HOME + "/bin:" + os.environ.get(r"PATH", "")
    return


def getUsage():
    usage = '''Usage: datax.py [-g] [-c conf] [-j jvm] [-p params] job.xml'''
    return usage

def showUsage():
    print(getUsage())
    sys.stdout.flush()
    return

def getOptionParser():
    op = OptionParser()
    op.add_option('-g', '--gen', action="store_true", dest="gen", help='generate job config file.')
    op.add_option('-e', '--edit', action="store_true", dest="gen", help='generate job config file.')
    op.add_option('-j', '--jvm', default="", dest="jvm", help='set jvm parameters.')
    op.add_option('-c', '--conf', dest="conf", help='set DataX core configuration.')
    op.add_option('-p', '--params', default="", help='add DataX runtime parameters.')
    op.add_option('-d', '--debug', default="", help='use remote debug mode.')

    op.set_usage(getUsage())
    return op

def registerSignal(process):
    global childProcess
    childProcess = process
    signal.signal(2, suicide)
    signal.signal(3, suicide)
    signal.signal(15, suicide)
    return

def suicide(signum, e):
    print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide ." % (signum)
    if childProcess is not None:
        childProcess.send_signal(signal.SIGQUIT)
        time.sleep(1)
        childProcess.kill()
    sys.exit(RET_FAIL_IN_KILL)

#NOTE: rewrite the url, just append file_version
def rewriteLocation(arg):
    '''
        >>> os.environ[r'FILE_VERSION'] = ''
        >>> os.environ.pop(r'FILE_VERSION')
        ''
        >>> rewriteLocation('bazhen')
        'bazhen'
        >>> os.environ[r'FILE_VERSION'] = '  '
        >>> rewriteLocation('bazhen')
        'bazhen'
        >>> os.environ[r'FILE_VERSION'] = '12345'
        >>> rewriteLocation('bazhen')
        'bazhen'
        >>> #测试sid 格式不符合 sid=123456
        >>> os.environ[r'FILE_VERSION'] = '12345'
        >>> rewriteLocation('bazhen')
        'bazhen'
        >>> #测试sid和天网id不匹配情况
        >>> os.environ[r'FILE_VERSION'] = '12345'
        >>> os.environ[r'SKYNET_ID'] = '54321'
        >>> rewriteLocation('sid=12345')
        'sid=12345'
        >>> #测试正确情况
        >>> os.environ[r'FILE_VERSION'] = '12345'
        >>> os.environ[r'SKYNET_ID'] = '54321'
        >>> rewriteLocation('sid=54321')
        'sid=54321&id=12345'
    '''
    #no version
    if r'FILE_VERSION' not in os.environ or \
        len(os.environ[r'FILE_VERSION'].strip()) == 0:
        return arg

    #has version but id is not correct
    matcher = GREP_SID_PATTERN.match(arg)
    if not matcher:
        return arg

    #如果下发的id和当前DataX获取的作业id不一致 不添加版本信息
    currentSID = matcher.group(1)
    dispatchSID = os.environ.get(r'SKYNET_ID', 'bazhen')
    if currentSID != dispatchSID:
        return arg

    return arg + "&id=" + os.environ[r'FILE_VERSION'].strip()

'''
    origin url like http://127.0.0.1/datax?sid=12345, it must has a '=' sign
    but job produced by master is like http://master/job/1234-5678-90
'''
def getJobName(url):
    '''
        >>> #测试Master生产的xml路径格式
        >>> getJobName('')
        ''
        >>> getJobName('bazhen')
        'bazhen'
        >>> getJobName('bazhen/ask/datax')
        'datax'
        >>> # 测试原始路径
        >>> getJobName('bazhen=')
        ''
        >>> getJobName('bazhen=datax')
        'datax'

    '''
    assert(isinstance(url, str))
    jobName = ""
    url = url.strip()
    isOrigin = (url.find(r'=') != -1)
    if isOrigin:
        jobName = url[url.find(r'=') + 1:]
    else:
        if url.endswith('/'):
            raise Exception('DataX url cannot ends with "/" .')
        jobName = url[url.rfind('/') + 1:]

    return jobName


def isUrl(arg):
    '''
        >>> isUrl('')
        False
        >>> isUrl(' ')
        False
        >>> isUrl('abs')
        False
        >>> isUrl('HTTP')
        True
        >>> isUrl(' Http ')
        True
    '''
    assert(isinstance(arg, str))
    return arg.strip().lower().find('http') == 0


def isJobXML(jobMsg):
    sflag = jobMsg.find('''<?xml version="1.0" encoding="UTF-8"?>''')
    return sflag != -1

#TODO if we generate same filename?
def genJobXml(jobMsg, jobName):
    '''
        >>> jobName = 'bazhen'
        >>> jobMsg = 'datax'
        >>> genJobXml(jobMsg, jobName)
        >>> open("./jobs/" + jobName + ".xml").read()
        'datax'
    '''
    assert(isinstance(jobName, str))
    assert(len(jobName) != 0)
    fileLocation = os.sep.join([dataXHome, 'jobs', jobName + '.xml'])
    fp = None
    try:
        fp = open(fileLocation, r'w')
        fp.write(jobMsg)
    finally:
        if fp:
            fp.close()
    return

def getExtLibs(libs):
    '''
        >>> os.environ['JAVA_HOME'] = ''
        >>> os.environ.pop('JAVA_HOME')
        ''
        >>> getExtLibs('libs')
        'libs'
        >>> os.environ['JAVA_HOME'] = 'bazhen'
        >>> getExtLibs('libs')
        'libs:bazhen/jre/lib/ext/'
    '''
    if r'JAVA_HOME' not in os.environ:
        return libs
    return os.pathsep.join([libs, os.environ[r'JAVA_HOME'] + "/jre/lib/ext/"])


def getCoreConf(options):
    coreConf = os.sep.join([dataXHome, "conf", "core.xml"])
    if options.conf:
        coreConf = os.path.abspath(options.conf)
    return coreConf

def getJvmParams(options):
    if options.jvm:
        return options.jvm
    return ""

def getJobParams(options):
    if options.params:
        return options.params
    return ""

def getDebug(options):
    if options.debug:
        return options.debug
    return ""

def getSharedLibraryPath():
    path = os.pathsep.join([dataXHome + "/libs", \
            dataXHome + "/plugins/reader/hdfsreader/libs", \
            dataXHome + "/plugins/writer/hdfswriter/libs", \
            dataXHome + "/plugins/writer/oraclewriter/libs", \
            dataXHome + "/plugins/writer/odpswriter/libs", \
             "/apsara/lib64", \
             os.environ.get("ORACLE_HOME", "") + "/lib"])
    if r'LD_LIBRARY_PATH' not in os.environ:
        os.environ['LD_LIBRARY_PATH'] = ''
    os.environ['LD_LIBRARY_PATH'] = os.pathsep.join([path, os.environ['LD_LIBRARY_PATH']])

    return path


def getBuildVersion(core):
    buildVersion = "Unknown"
    try:
        buildVersion = XmlTree.parse(core).getroot().find('core/version').text
    finally:
        return buildVersion

'''
def getJvmOptionFromJob(filename):
    return getOptionFromJob(filename, "core.java.opt")


def getOptionFromJob(filename, key):
    value = ""
    try:
        for item in XmlTree.parse(filename).getroot.find("job/config"):
            if item.attrib[r'key'] == key:
                value = item.attrib['value']
                break;
    finally:
        return value
'''

def readFile(fileName):
    lineList = []
    f = open(fileName,'r')
    for line in f:
        if line is not None and len(line)!=0:
            line = line.strip()
        if len(line)!=0:
            lineList.append(line)
    f.close()
    return lineList

def getJobFromHttp(url):
    '''
        >>> try:
        ...     getJobFromHttp('')
        ... except Exception, ex:
        ...     print str(ex)

    '''
    job = None
    response = None

    try:
        try:
            response = urllib2.urlopen(rewriteLocation(url))
            jobMsg = response.read()
            if isJobXML(jobMsg):
                genJobXml(jobMsg, getJobName(url))
                job = os.sep.join([dataXHome, "jobs", getJobName(url) + ".xml"])
            else:
                raise Exception("[Warn] Illegal response mesage :\n" + jobMsg)

        except Exception, ex:
            print >>sys.stderr, str(ex)

    finally:
        if response:
            response.close()
        return job


def getJob(jobPath,jvmOption):
    job = None
    jvmOptionInJobXML = None

    if not isUrl(jobPath):
        job = os.path.abspath(jobPath)
    else:
        counter = 0
        while counter < 4:
            job = getJobFromHttp(jobPath)
            if job:
                break
            time.sleep(2**counter)
            counter += 1
    if job:
        strTime = str(time.time())
        job,jvmOptionInJobXML = addJVMOptionToJob(job,strTime,jvmOption)
    return job,jvmOptionInJobXML


def addJVMOptionToJob(jobPath,strTime,jvmOption):
    '''
    >>> addJVMOptionToJob("/home/taobao/datax/jobs/example.xml","1","-Xms888m -Xmx888m")
    ('/home/taobao/datax/jobs/example.1.xml', '-Xms888m -Xmx888m')

    >>> addJVMOptionToJob("/home/taobao/datax/jobs/example.hasConfig.xml","2","")
    ('/home/taobao/datax/jobs/example.hasConfig.xml', '-Xms10m')

    >>> addJVMOptionToJob("/home/taobao/datax/jobs/example.hasConfig.xml","2","-Xms999m -Xmx999m")
    ('/home/taobao/datax/jobs/example.hasConfig.2.xml', '-Xms999m -Xmx999m')

    '''
    hasJvmOptionInCMD = False
    if(jvmOption is not None and len(jvmOption) > 0):
        hasJvmOptionInCMD = True

    tree = XmlTree.parse(jobPath)
    root = tree.getroot()
    config = root.find('job/config')
    if config is None:
        if hasJvmOptionInCMD:
            config = XmlTree.Element("config")
            jvmElement = XmlTree.Element("param",attrib={"key":"core.jvm.option","value":jvmOption})
            config.append(jvmElement)
            root.find('job').append(config)
            fileLocation = getNewFileLocation(jobPath,strTime)
            tree.write(fileLocation)
            return fileLocation,jvmOption
        else:
            return jobPath,None
    try:
        jvmOptionInJobXML = None

        for child in config:
            tempMap = child.attrib
            key = tempMap["key"]
            value = tempMap["value"]
            if(key == 'core.jvm.option'):
                if hasJvmOptionInCMD:
                    child.set("value",jvmOption)
                    jvmOptionInJobXML = jvmOption
                else:
                    jvmOptionInJobXML = value
            if(key == 'core.model'):
                if len(value.strip())!=0:
                    global runMode
                    runMode = value
        if jvmOptionInJobXML is None:
            if hasJvmOptionInCMD:
                jvmElement = XmlTree.Element("param",attrib={"key":"core.jvm.option","value":jvmOption})
                config.append(jvmElement)
                fileLocation = getNewFileLocation(jobPath,strTime)
                tree.write(fileLocation)
                return fileLocation,jvmOptionInJobXML
            else:
                return jobPath,None
        else:
            if hasJvmOptionInCMD:
                fileLocation = getNewFileLocation(jobPath,strTime)
                tree.write(fileLocation)
                return fileLocation,jvmOptionInJobXML
            else:
                return jobPath,jvmOptionInJobXML
    except:
        return jobPath,None

def getNewFileLocation(jobPath,strTime):
    '''
    >>> dataXHome = '/home/taobao/datax'
    >>> getNewFileLocation("/home/taobao/datax/jobs/example.xml","222")
    './jobs/example.222.xml'

    >>> getNewFileLocation("/home/taobao/datax/jobs/example.xml","123478954.45")
    './jobs/example.123478954.45.xml'

    '''
    jobName = getJobName(jobPath)
    jobNameNew = jobName[:-3]+strTime+jobName[-4:]
    fileLocation = os.sep.join([dataXHome, 'jobs', jobNameNew])
    return fileLocation


if __name__ == '__main__':
    setJavaPath()

    if IS_DEBUG:
        import doctest
        doctest.testmod()
        sys.exit(0)

    dataXHome = os.environ.get('DATAX_HOME', '').strip()
    isSetDataXEnv = (len(dataXHome) == 0)
    if isSetDataXEnv:
        dataXHome = os.sep.join([sys.path[0], '..', '/'])
    os.environ['DATAX_HOME'] = dataXHome

    options, args = getOptionParser().parse_args(sys.argv[1:])

    ctxt={}
    ctxt['jar'] = "engine/datax-engine-1.0.0-SNAPSHOT.jar"
    ctxt['conf'] = getCoreConf(options)
    ctxt['params'] = getJobParams(options)
    ctxt['jvm'] = getJvmParams(options)
    ctxt['libs'] = getExtLibs('libs')
    ctxt['shared'] = getSharedLibraryPath()
    ctxt['debug'] = getDebug()

    showCopyRight(ctxt['conf'])

    if options.gen:
        os.chdir(dataXHome)
        cmd = Template(genCmd).substitute(**ctxt)
        sys.exit(os.system(cmd))

    if len(args) == 0:
        showUsage()
        sys.exit(0)
    try:
        job,jvmOptionInJobXML = getJob(args[0].strip(),ctxt['jvm'])
        if not job:
            print >>sys.stderr, "[Error] DataX query job failed !"
            sys.exit(RET_FAIL_IN_RETRY)
    except Exception, e:
        print >>sys.stderr, e
        sys.exit(RET_FAIL_IN_RETRY)
    except:
        print >>sys.stderr, "[Error] DataX job xml not well-formed !"
        sys.exit(RET_FAIL_IN_RETRY)

    ctxt['job'] = job

    if jvmOptionInJobXML is not None:
        ctxt['jvm'] = jvmOptionInJobXML

    if len(runMode.strip())!=0:
        ctxt['conf'] = runMode
    os.chdir(dataXHome)
    cmd = Template(engineCmd).substitute(**ctxt)
    #print cmd
    p = subprocess.Popen(cmd, shell=True)
    registerSignal(p)
    (stdo, stde) = p.communicate()

    retCode = p.returncode
    if 0 != retCode:
        sys.exit(RET_FAIL_IN_RETRY)
    else:
        sys.exit(RET_OK)
