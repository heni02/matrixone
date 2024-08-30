drop table if exists t1(a int);
create table t1 (a int);
insert into t1 values (1),(1),(1),(1),(1);
select * from t1 limit 0,18446744073709551615;
select * from t1 limit 18446744073709551615,18446744073709551615;
select * from t1 limit 18446744073709551615,0;
select * from t1 limit -1,18446744073709551615;
select * from t1 limit 0,-1;
select * from t1 limit 0,0;
select * from t1 limit 0,3;
select * from t1 limit 1,3;
select * from t1 limit 2,4;
select * from t1 order by a limit 0,18446744073709551615;
select * from t1 order by a limit 18446744073709551615,18446744073709551615;
select * from t1 order by a limit 18446744073709551615,0;
select * from t1 order by a limit -1,18446744073709551615;
select * from t1 order by a limit 0,-1;
select * from t1 order by a limit 0,0;
drop table if exists t1;
create table t1 (a int primary key, b int);
insert into t1 select result, 1 from generate_series (1, 800000)g;
select * from t1 order by a limit 700000, 2;
drop table if exists t1;
create table t1 (a int primary key, b varchar);
insert into t1 select result, repeat("abcdefg",500) from generate_series (1, 30000)g;
select a, left(b,3) from t1 order by a desc limit 32000, 2;