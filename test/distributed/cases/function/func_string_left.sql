select left('abcde', 3) from dual;
select left('abcde', 0) from dual;
select left('abcde', 10) from dual;
select left('abcde', -1) from dual;
select left('abcde', null) from dual;
select left(null, 3) from dual;
select left(null, null) from dual;
select left('foobarbar', 5) from dual;
select left('qwerty', 1.2) from dual;
select left('qwerty', 1.5) from dual;
select left('qwerty', 1.8) from dual;
select left("是都方式快递费",3) from dual;
select left("あいうえお",3) from dual;
select left("あいうえお ",3) from dual;
select left("あいうえお  ",3) from dual;
select left("あいうえお   ",3) from dual;
select left("龔龖龗龞龡",3) from dual;
select left("龔龖龗龞龡 ",3) from dual;
select left("龔龖龗龞龡  ",3) from dual;
select left("龔龖龗龞龡   ",3) from dual;

drop table if exists t1;
CREATE TABLE t1 (str VARCHAR(100), len INT);
insert into t1 values('abcdefghijklmn',3);
insert into t1 values('ABCDEFGH123456', 3);
insert into t1 values('ABCDEFGHIJKLMN', 20);
insert into t1 values('ABCDEFGHijklmn', -1);
insert into t1 values('ABCDEFGH123456', 7);
insert into t1 values('', 3);

select left(str, len) from t1;
select * from t1 where left(str, len) = 'ABC';
select left(str, 3) from t1;
select left('sdfsdfsdfsdf', len) from t1;
drop table t1;

ccc