Name: t_dp_dw_datax_3_core_all
Packager:xiafei.qiuxf
Version:201410271850
Release: %(echo $RELEASE)%{?dist}

Summary: datax 3 core
URL: http://gitlab.alibaba-inc.com/datax/datax
Group: t_dp
License: Commercial
BuildArch: noarch


%define __os_install_post %{nil}

%description
CodeUrl: http://gitlab.alibaba-inc.com/datax/datax
datax core
%{_svn_path}
%{_svn_revision}

%define _prefix /home/taobao/datax3

%prep
export LANG=zh_CN.UTF-8

%pre
grep -q "^cug-tbdp:" /etc/group &>/dev/null || groupadd -g 508 cug-tbdp &>/dev/null || true
grep -q "^taobao:" /etc/passwd &>/dev/null || useradd -u 503 -g cug-tbdp taobao &>/dev/null || true
find %{_prefix}/logs -type f -mtime +7 -exec rm -rf {} \;
rm -rf %{_prefix}/plugins/
rm -rf %{_prefix}/spi/


%build
cd ${OLDPWD}/../
#(cd datax-engine/src/main/configs; cat core.xml | sed "s/{version}/`date +%Y%m%d`/g" > core.new; mv core.new core.xml)
#(cd datax-engine/src/main/configs; cat core.hadoop.xml | sed "s/{version}/`date +%Y%m%d`/g" > core.hadoop.new; mv core.hadoop.new core.hadoop.xml)
#(cd datax-engine/src/main/configs; cat core.pseudo.xml | sed "s/{version}/`date +%Y%m%d`/g" > core.pseudo.new; mv core.pseudo.new core.pseudo.xml)
#(cd datax-engine/src/main/configs; cat core.alisa.xml | sed "s/{version}/`date +%Y%m%d`/g" > core.alisa.new; mv core.alisa.new core.alisa.xml)
#(cd datax-engine/src/main/configs; cat core.alisa.p2p.xml | sed "s/{version}/`date +%Y%m%d`/g" > core.alisa.p2p.new; mv core.alisa.p2p.new core.alisa.p2p.xml)

/home/ads/tools/apache-maven-3.0.3/bin/mvn clean package -DskipTests assembly:assembly
#mvn clean package assembly:assembly -DskipTests=true

#(cd datax-common; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-official-router; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-plugin-database-utils; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-engine; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-hdfsreader; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-hdfswriter; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-hbasewriter-0.94; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-hbasereader-0.94; /home/ads/tools/apache-maven-3.0.3/bin/mvn install -U -Dmaven.test.skip=true)
#(cd datax-odpsreader; /home/ads/tools/apache-maven-3.0.3/bin/mvn assembly:assembly -U -Dmaven.test.skip=true)
#(cd datax-otsbulkwriter; /home/ads/tools/apache-maven-3.0.3/bin/mvn assembly:assembly -U -Dmaven.test.skip=true)
#(cd datax-hbasebulkwriter-cdh4; /home/ads/tools/apache-maven-3.0.3/bin/mvn assembly:assembly install -U -Dmaven.test.skip=true)
#(cd datax-hbasebulkwriter-cdh3; /home/ads/tools/apache-maven-3.0.3/bin/mvn assembly:assembly install -U -Dmaven.test.skip=true)



#/home/ads/tools/apache-maven-3.0.3/bin/mvn -Dmaven.test.skip=true -U assembly:assembly

%install
mkdir -p .%{_prefix}
#(cd $OLDPWD/../target/; tar -xzf datax.tar.gz)
cp -rf $OLDPWD/../target/datax/datax/* .%{_prefix}/

%post
chmod -R 0755 %{_prefix}/bin
chmod -R 0757 %{_prefix}/conf
chmod -R 0757 %{_prefix}/jobs
chmod -R 0757 %{_prefix}/lib
chmod -R 0757 %{_prefix}/logs
chmod -R 0757 %{_prefix}/plugin
chmod -R 0757 %{_prefix}/scripts

#ln -sf /home/yunti/hadoop-current/lib/ %{_prefix}/plugins/reader/hdfsreader/libs/hadoop_libs
#ln -sf /home/yunti/hadoop-current/lib/ %{_prefix}/plugins/writer/hdfswriter/libs/hadoop_libs
#ln -sf /home/yunti/hadoop-current/lib/ %{_prefix}/plugins/reader/hivereader/libs/hadoop_libs
#ln -sf /home/yunti/hadoop-current/lib/ %{_prefix}/plugins/writer/hivewriter/libs/hadoop_libs

## for DataX TairBulkWriter
#rm -rf %{_prefix}/tools/odps2tair && mkdir -p %{_prefix}/tools/odps2tair && mv %{_prefix}/plugins/writer/tairbulkwriter/tools/* %{_prefix}/tools/odps2tair/ || true
#
## for tair log bug
#test -d /home/taobao/logs/ || mkdir /home/taobao/logs/ || true
#chmod -R 0777 /home/taobao/logs/ || true
#
## for DataX OTSBulkWriter
#rm -rf %{_prefix}/tools/odps2ots && mkdir -p %{_prefix}/tools/odps2ots && mv %{_prefix}/plugins/writer/otsbulkwriter/datax_odps_ots_sort.py %{_prefix}/tools/odps2ots/ && mv %{_prefix}/plugins/writer/otsbulkwriter/datax-jar-with-dependencies.jar %{_prefix}/tools/odps2ots/datax_odps_ots_udf.jar || true
#
## for DataX hbasebulkwriter-cdh4
#rm -rf %{_prefix}/tools/odps2hbase_cdh4 && mkdir -p %{_prefix}/tools/odps2hbase_cdh4 && mv %{_prefix}/plugins/writer/hbasebulkwriter-cdh4/datax_odps_hbase_cdh4_sort.py %{_prefix}/tools/odps2hbase_cdh4/ && mv %{_prefix}/plugins/writer/hbasebulkwriter-cdh4/datax-jar-with-dependencies.jar %{_prefix}/tools/odps2hbase_cdh4/datax_odps_hbase_udf.jar || true
#
## for DataX hbasebulkwriter-cdh3
#rm -rf %{_prefix}/tools/odps2hbase_cdh3 && mkdir -p %{_prefix}/tools/odps2hbase_cdh3 && mv %{_prefix}/plugins/writer/hbasebulkwriter-cdh3/datax_odps_hbase_cdh3_sort.py %{_prefix}/tools/odps2hbase_cdh3/ && mv %{_prefix}/plugins/writer/hbasebulkwriter-cdh3/datax-jar-with-dependencies.jar %{_prefix}/tools/odps2hbase_cdh3/datax_odps_hbase_udf.jar || true

chmod +rx /home/taobao/


%files
%defattr(755,taobao,cug-tbdp)
%config(noreplace) %{_prefix}/conf/core.json
%config(noreplace) %{_prefix}/conf/logback.xml
%{_prefix}
