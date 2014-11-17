Name: t_dp_dw_datax_3_core_all
Packager:xiafei.qiuxf
Version:2014111412.4
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

%define _prefix /home/admin/datax3

%prep
export LANG=zh_CN.UTF-8

%pre
grep -q "^cug-tbdp:" /etc/group &>/dev/null || groupadd -g 508 cug-tbdp &>/dev/null || true
grep -q "^taobao:" /etc/passwd &>/dev/null || useradd -u 503 -g cug-tbdp taobao &>/dev/null || true
find %{_prefix}/log -type f -mtime +7 -exec rm -rf {} \;
rm -rf %{_prefix}/plugins/
rm -rf %{_prefix}/spi/


%build
cd ${OLDPWD}/../

/home/ads/tools/apache-maven-3.0.3/bin/mvn clean package -DskipTests assembly:assembly

%install
mkdir -p .%{_prefix}
#(cd $OLDPWD/../target/; tar -xzf datax.tar.gz)
cp -rf $OLDPWD/../target/datax/datax/* .%{_prefix}/

%post
chmod -R 0755 %{_prefix}/bin
chmod -R 0757 %{_prefix}/conf
chmod -R 0757 %{_prefix}/job
chmod -R 0757 %{_prefix}/lib
chmod -R 0757 %{_prefix}/log
chmod -R 0757 %{_prefix}/plugin
chmod -R 0757 %{_prefix}/script


%files
%defattr(755,admin,cug-tbdp)
%config(noreplace) %{_prefix}/conf/core.json
%config(noreplace) %{_prefix}/conf/logback.xml
%{_prefix}
