--odps_write_test00_no_partition
create table odps_write_test00_no_partition(
  id bigint,
  name string,
  birthday datetime,
  at_school boolean,
  money double);


--odps_write_test00_partitioned
create table odps_write_test00_partitioned(
  id bigint,
  name string,
  birthday datetime,
  at_school boolean,
  money double)
  partitioned by(school string,class bigint);

