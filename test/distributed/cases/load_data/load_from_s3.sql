--load data from s3
create table table_s3_csv(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.csv', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} into table table_s3_csv fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from table_s3_csv;

--terminated by | enclosed ''
create table table_s3_csv2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char_new.csv', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} into table table_s3_csv2 fields terminated by '|' enclosed by '' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from table_s3_csv2;
truncate table table_s3_csv2;
--ignore rows
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char_ignore.csv', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} into table table_s3_csv2 fields terminated by '|' enclosed by '' lines terminated by '\n' ignore 1 rows;
select char_1,char_2,date_1,date_2 from table_s3_csv2;

--s3 file is log format
create table table_s3_log(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime(6),date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.log', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} into table table_s3_log fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from table_s3_log;

--s3 file compression gzip
create table table_s3_gz(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_gzip.gz', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='gzip'} into table table_s3_gz fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_gz;

--s3 file compression bzip2
create table table_s3_bzip2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_bzip.bz2', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='bz2'} into table table_s3_bzip2 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_bzip2;

--s3 file compression lz4
create table table_s3_lz4(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_lz4.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='lz4'} into table table_s3_lz4 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_lz4;

--s3 file compression auto
create table table_s3_auto(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_lz4.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='auto'} into table table_s3_auto fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_auto;

--s3 file compression none
create table table_s3_none(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.csv', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='none'} into table table_s3_none fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_none;

--default compression
create table table_s3_lz4_nocomp(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_lz4.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} into table table_s3_lz4_nocomp fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_lz4_nocomp;
create table table_s3_lz4_02(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_lz4.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='auto'} into table table_s3_lz4_02 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_lz4_02;

--Abnormal test: The compression format does not correspond to the uncompressed file
create table table_s3_an1(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_gzip.gz', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='bz2'} into table table_s3_an1 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_an1;
create table table_s3_an2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.log', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='bz2'} into table table_s3_an2 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from table_s3_an2;

--Abnormal test:  parameter invalid
create table table_s3_an3(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data1', 'filepath'='ex_table_gzip.gz', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='gzip'} into table table_s3_an3 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_an3;
create table table_s3_an4(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_gzip.gz', 'role_arn'='arn:aws:iam::141549:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='gzip'} into table table_s3_an4 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_an4;
create table table_s3_an5(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.log', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf'} into table table_s3_an5 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_an5;
create table table_s3_an6(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp);
load data URL s3option{'bucket'='bvtcase-data', 'filepath'='ex_table_char2222.log', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'}into table table_s3_an6 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from table_s3_an6;

--external s3 file
--csv
-- @bvt:issue#7738
create external table ex_table_s3_csv(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option {'bucket'='bvtcase-data', 'filepath'='ex_table_char.csv', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'}  fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from ex_table_s3_csv;

--log
create external table ex_table_s3_log(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', 'filepath'='ex_table_char.log', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'}  fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from ex_table_s3_log;

--gzip
create external table ex_table_s3_gz(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19)) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_gzip.gz','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='gz'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_gz;

--bzip2
create external table ex_table_s3_bzip2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_bzip.bz2','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='bz2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_bzip2;

--lz4
create external table ex_table_s3_lz4(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19))URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_lz4.lz4','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111' ,"compression"='lz4'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_lz4;

--auto
create external table ex_table_s3_auto(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19))URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_lz4.lz4','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='auto'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_auto;

--none
create external table ex_table_s3_none(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_char.csv','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111',"compression"='none'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_none;

--缺省compression
create external table ex_table_s3_lz4_nocomp(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19))URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_lz4.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_lz4_nocomp;
create external table ex_table_s3_lz4_02(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double,num_col11 decimal(38,19))URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_lz4.lz4','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111' ,'compression'='auto'};
select * from ex_table_s3_lz4_02;

--异常：压缩格式不对应，未压缩文件
create external table ex_table_s3_an1(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_gzip.gz','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='bz2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_an1;
create external table ex_table_s3_an2(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_char.log','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','compression'='bz2'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select char_1,char_2,date_1,date_2 from ex_table_s3_an2;

-- information valid
create external table ex_table_s3_an3(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_char.log','role_arn'='arn:aws:iam::141549464311:role/cross-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_an3;
create external table ex_table_s3_an4(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_char.log','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_an4;
create external table ex_table_s3_an5(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data-1', "filepath"='ex_table_char.log','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_an5;
create external table ex_table_s3_an6(char_1 char(20),char_2 varchar(10),date_1 date,date_2 datetime,date_3 timestamp) URL s3option{'bucket'='bvtcase-data', "filepath"='ex_table_char1.log','role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from ex_table_s3_an6;
-- @bvt:issue

--load jsonline from s3
create table jsonline_s3_01(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='integer_numbers_1.jl', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='object'} into table jsonline_s3_01 fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select * from jsonline_s3_01;
truncate table jsonline_s3_01;
load data url s3option {'bucket'='bvtcase-data', 'filepath'='integer_numbers_1_array.jl', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='array'} into table jsonline_s3_01;
select * from jsonline_s3_01;
create table jsonline_s3_02(col1 char(225),col2 varchar(225),col3 text,col4 varchar(225));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='char_varchar_1_array.json', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='array'} into table jsonline_s3_02;
select * from jsonline_s3_02;
truncate table jsonline_s3_02;
load data url s3option {'bucket'='bvtcase-data', 'filepath'='char_varchar_1.jl', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='object'}into table jsonline_s3_02;
select * from jsonline_s3_02;
create table jsonline_s3_03(col1 float,col2 double,col3 decimal(38,16),col4 decimal(38,16));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='float_1.json', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='object'}into table jsonline_s3_03;
select * from jsonline_s3_03;
truncate table jsonline_s3_03;
load data url s3option {'bucket'='bvtcase-data', 'filepath'='float_1_array.jl', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'jsondata'='array'}into table jsonline_s3_03;
select * from jsonline_s3_03;
create table jsonline_s3_gzip(col1 char(225),col2 varchar(225),col3 text,col4 varchar(225));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='char_varchar_1_array.json.gz', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'compression'='gzip','jsondata'='array'}into table jsonline_s3_gzip;
select * from jsonline_s3_gzip;
truncate table jsonline_s3_gzip;
create table jsonline_s3_bzip2(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='integer_numbers_1.jl.bz2', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'compression'='bz2','jsondata'='object'}into table jsonline_s3_bzip2;
select * from jsonline_s3_bzip2;
create table jsonline_s3_lz4(col1 float,col2 double,col3 decimal(38,16),col4 decimal(38,16));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='float_1.json.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'compression'='lz4','jsondata'='object'}into table jsonline_s3_lz4;
select * from jsonline_s3_lz4;
create table jsonline_s3_auto(col1 float,col2 double,col3 decimal(38,16),col4 decimal(38,16));
load data url s3option {'bucket'='bvtcase-data', 'filepath'='float_1.json.lz4', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline', 'compression'='auto','jsondata'='object'}into table jsonline_s3_auto;
select * from jsonline_s3_auto;
create table jsonline_s3_none(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned);
load data url s3option {'bucket'='bvtcase-data', 'filepath'='integer_numbers_1.jl', 'role_arn'='arn:aws:iam::141549464311:role/cross-account-s3', 'external_id'='30ffee3b_08c2_484c_b2cf_daf28147b111','format'='jsonline','compression'='none', 'jsondata'='object'} into table jsonline_s3_none;
select * from jsonline_s3_none;