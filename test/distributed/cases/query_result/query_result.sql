set global save_query_result = on;
drop table if exists tt;
create table tt (a int);
insert into tt values(1), (2);
/* save_result */select * from tt;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from tt;
select count(*) from meta_scan(last_query_id()) as u;
set global save_query_result = off;

select * from tt;
-- @bvt:issue#9886
select * from result_scan(last_query_id()) as u;
-- @bvt:issue
set global save_query_result = on;
drop table if exists t2;
create table t2 (a int, b int, c int);
insert into t2 values(1, 2, 3), (1, 2, 3);
/* save_result */select c from tt, t2 where tt.a = t2.a;
select * from result_scan(last_query_id()) as u;
/* save_result */select c from tt, t2 where tt.a = t2.a;
/* save_result */select t2.b from result_scan(last_query_id()) as u, t2 where u.c = t2.c;
select * from result_scan(last_query_id()) as u;
/* save_result */select c from tt, t2 where tt.a = t2.a;
select * from result_scan(last_query_id()) as u, result_scan(last_query_id()) as v limit 1;
set global save_query_result = off;

set global save_query_result = on;
/* save_result */select tt.a from tt, t2;
select tables from meta_scan(last_query_id()) as u;
set global query_result_maxsize = 0;
/* save_result */select tt.a from tt, t2;
select char_length(result_path) from meta_scan(last_query_id()) as u;
/* save_result */select tt.a from tt, t2;
select result_size = 0 from meta_scan(last_query_id()) as u;
set global save_query_result = off;

set global save_query_result = on;
set global query_result_maxsize = 100;
create role rrrqqq;
grant rrrqqq to dump;
/* save_result */select * from tt;
set role rrrqqq;
select * from meta_scan(last_query_id(-2)) as u;
set role moadmin;
create database db111;
create table db111.tt1 (a int);
insert into db111.tt1 values(1), (2);
create table db111.tt2 (a int);
insert into db111.tt2 values(1), (2);
grant select on table db111.tt1 to rrrqqq;
/* save_result */select * from db111.tt1;
/* save_result */select * from db111.tt2;
set role rrrqqq;
select * from result_scan(last_query_id(-3)) as u;
select * from meta_scan(last_query_id(-3)) as u;
set role moadmin;
drop role rrrqqq;
select * from result_scan('d8fb97e7-e30e-11ed-8d80-d6aeb943c8b4') as u;
--need to clean database db111
drop database if exists db111;
set global save_query_result = off;

create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
set global save_query_result = on;
create database test;
/* save_result */show databases;
select * from result_scan(last_query_id()) as u;
use test;
drop table if exists tt;
create table tt (a int);
insert into tt values(1), (2);
/* save_result */select * from tt;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from tt;
select count(*) from meta_scan(last_query_id()) as u;
/* save_result */show tables;
select * from result_scan(last_query_id()) as u;
-- @bvt:issue#12083
/* save_result */show variables like 'tx_isolation';
select * from result_scan(last_query_id()) as u;
-- @bvt:issue
/* save_result */show columns from tt;
select * from result_scan(last_query_id()) as u;
/* save_result */show grants;
select * from result_scan(last_query_id()) as u;
/* save_result */show create table tt;
select * from result_scan(last_query_id()) as u;
alter table tt add unique index id(a);
/* save_result */show index from tt;
select * from result_scan(last_query_id()) as u;
-- @bvt:issue#12083
/* save_result */show node list;
select * from result_scan(last_query_id()) as u;
-- @bvt:issue
create sequence seq_an_03  increment 10 start with 1 no cycle;
/* save_result */show sequences;
select * from result_scan(last_query_id()) as u;
CREATE TABLE t1 (S1 INT);
CREATE TABLE t2 (S1 INT);
INSERT INTO t1 VALUES (1),(3),(4),(6);
INSERT INTO t2 VALUES (2),(4),(5);
/* save_result */SELECT * FROM t1 JOIN t2 on t1.S1=t2.S1;
select * from result_scan(last_query_id()) as u;
/* save_result */select t2.S1 from t2 left join t1 on t1.S1 =t2.S1;
select * from result_scan(last_query_id()) as u;
/* save_result */select t2.S1 from t2 right join t1 on t1.S1 =t2.S1;
select * from result_scan(last_query_id()) as u;
/* save_result */(select s1 from t1 union select s1 from t2) order by s1 desc;
select * from result_scan(last_query_id()) as u;
/* save_result */(select s1 from t1 union all select s1 from t2) order by s1 desc;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from t1 where t1.s1 > (select t2.s1 from t2 where s1<3);
select * from result_scan(last_query_id()) as u;
/* save_result */select * from t1 where s1 <> any (select s1 from t2);
select * from result_scan(last_query_id()) as u;
/* save_result */select * from t1 where s1 = some (select s1 from t2);
select * from result_scan(last_query_id()) as u;
--/* save_result */explain select * from t1;
--select * from result_scan(last_query_id()) as u;
--/* save_result */explain select * from t1 where t1.s1 > (select t2.s1 from t2 where s1<3);
--select * from result_scan(last_query_id()) as u;
drop table if exists time_window01;
create table time_window01 (ts timestamp primary key, col2 int);
insert into time_window01 values ('2021-01-12 00:00:00.000', 12);
insert into time_window01 values ('2020-01-12 12:00:12.000', 24);
insert into time_window01 values ('2021-01-12 01:00:00.000', 34);
insert into time_window01 values ('2020-01-12 12:01:12.000', 20);
select * from time_window01;
/* save_result */select _wstart, _wend, max(col2), min(col2) from time_window01 where ts > '2020-01-11 12:00:12.000' and ts < '2021-01-13 00:00:00.000' interval(ts, 100, day) fill(prev);
select * from result_scan(last_query_id()) as u;
create view v1 as SELECT 1 IN (SELECT 1);
/* save_result */select * from v1;
select * from result_scan(last_query_id()) as u;
drop table time_window01;
set global save_query_result = off;
-- @session
drop account abc;
