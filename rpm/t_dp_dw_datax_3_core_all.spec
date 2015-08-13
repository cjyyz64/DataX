Name: t_dp_dw_datax_3_core_all
Packager:xiafei.qiuxf
Version:201508131001
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
rm -rf %{_prefix}/plugin/
rm -rf %{_prefix}/lib/


%build
cd ${OLDPWD}/../

export MAVEN_OPTS="-Xms256m -Xmx1024m -XX:MaxPermSize=128m"
/home/ads/tools/apache-maven-3.0.3/bin/mvn clean package -DskipTests assembly:assembly

%install
mkdir -p .%{_prefix}
cp -rf $OLDPWD/../target/datax/datax/* .%{_prefix}/
# make dir for hook
mkdir .%{_prefix}/hook

%post
chmod -R 0755 %{_prefix}/bin
chmod -R 0755 %{_prefix}/conf
chmod -R 0755 %{_prefix}/job
chmod -R 0755 %{_prefix}/lib
chmod -R 0777 %{_prefix}/log
chmod -R 0755 %{_prefix}/plugin
chmod -R 0755 %{_prefix}/script
chmod -R 0755 %{_prefix}/hook


%files
%defattr(755,admin,cug-tbdp)
%config(noreplace) %{_prefix}/conf/core.json
%config(noreplace) %{_prefix}/conf/logback.xml
%config(noreplace) %{_prefix}/conf/.secret.properties

%{_prefix}
