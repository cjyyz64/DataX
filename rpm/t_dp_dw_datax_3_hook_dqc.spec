Name: t_dp_dw_datax_3_hook_dqc
Packager:xiafei.qiuxf
Version:201412221040
Release: %(echo $RELEASE)%{?dist}

Summary: datax 3 dqc hook
URL: http://gitlab.alibaba-inc.com/datax/datax
Group: t_dp
License: Commercial
BuildArch: noarch


%define __os_install_post %{nil}

%description
CodeUrl: http://gitlab.alibaba-inc.com/datax/datax
datax dqc hook
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


%build
cd ${OLDPWD}/../

/home/ads/tools/apache-maven-3.0.3/bin/mvn install -N
/home/ads/tools/apache-maven-3.0.3/bin/mvn install -pl common -DskipTests
cd dqchook
/home/ads/tools/apache-maven-3.0.3/bin/mvn clean package -DskipTests assembly:assembly
cd -

%install
mkdir -p .%{_prefix}
cp -rf $OLDPWD/../target/datax/datax/* .%{_prefix}/

%post
chmod -R 0757 %{_prefix}/hook


%files
%defattr(755,admin,cug-tbdp)
%config(noreplace) %{_prefix}/hook/dqc/dqc.properties
%{_prefix}
