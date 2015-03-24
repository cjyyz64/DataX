/**
	MySQL 相关插件使用到的数据库脚本准备。
*/

CREATE DATABASE datax_mysql_test_db CHARACTER SET utf8;

USE datax_mysql_test_db;

CREATE TABLE `db_info` (
  `db_id` bigint(20) NOT NULL auto_increment COMMENT '主键id',
  `db_type` int(11) NOT NULL COMMENT '数据库类型:1-mysql,2-oracle,3-sqlserver,4-oceanbase',
  `db_ip` varchar(64) default NULL COMMENT '数据库ip',
  `db_port` int(11) default NULL COMMENT '数据库端口',
  `db_role` varchar(8) default NULL COMMENT '数据库主从关系(主-master,备-slave)',
  `db_name` varchar(64) default NULL COMMENT '数据库名称',
  `db_username` varchar(128) default NULL COMMENT '数据库登录用户名',
  `db_password` varchar(128) default NULL COMMENT '数据库登录密码',
  `db_modify_time` datetime default NULL COMMENT '该数据库最近修改时间',
  `db_modify_user` varchar(64) default NULL COMMENT '最近修改人',
  `db_description` varchar(256) default NULL COMMENT '数据库描述信息',
  `db_tddl_info` varchar(256) default NULL COMMENT '数据库tddl相关信息',
  `on_line_flag` bool COMMENT '数据库是否可用',
  PRIMARY KEY  (`db_id`),
  KEY `idx_db_name` (`db_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='仅供于测试使用';

insert into `db_info`(`db_id`,`db_type`,`db_ip`,`db_port`,`db_role`,`db_name`,`db_username`,
`db_password`,`db_modify_time`,`db_modify_user`,`db_description`,`db_tddl_info`,`on_line_flag`)
values(1,1,'127.0.0.1','3306','master','hello_db00','user','pass','2014-09-01 12:00:01','jack','让天下没有难做的生意，这个事情离不开数据库.','{"hello":"world"}',true);


insert into `db_info`(`db_id`,`db_type`,`db_ip`,`db_port`,`db_role`,`db_name`,`db_username`,
`db_password`,`db_modify_time`,`db_modify_user`,`db_description`,`db_tddl_info`,`on_line_flag`)
values(2,1,'1.2.3.4','3306','master','hello_db01','user','pass','2014-09-01 13:00:02','jack','不是为了输赢，老罗是认真的。.','{"world":"changed"}',false);

insert into `db_info`(`db_id`,`db_type`,`db_ip`,`db_port`,`db_role`,`db_name`,`db_username`,
`db_password`,`db_modify_time`,`db_modify_user`,`db_description`,`db_tddl_info`,`on_line_flag`)
values(3,2,'8.8.8.8','1521','slave','hello_db','tiger','tiger','2014-09-01 14:00:03','Tom','中国好声音.','{"world":"changed"}',false);

insert into `db_info`(`db_id`,`db_type`,`db_ip`,`db_port`,`db_role`,`db_name`,`db_username`,
`db_password`,`db_modify_time`,`db_modify_user`,`db_description`,`db_tddl_info`,`on_line_flag`)
values(4,3,'100.100.100.100','8765','master','world_hello','user','pass','2014-09-01 15:01:04','rose','真人CS好玩吗？','{"world":"changed"}',true);

