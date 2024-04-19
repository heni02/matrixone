-- parquet type to mo type :int64->bigint binary->varchar
create table parquet_01(price bigint,area bigint,bedrooms bigint,bathrooms bigint,stories bigint,mainroad varchar(25),guestroom varchar(25),basement varchar(50),hotwaterheating varchar(50),airconditioning varchar(50),parking bigint,prefarea varchar(50),furnishingstatus varchar(50));
load data infile {'filepath'='/Users/heni/Downloads/price.parquet','format'='parquet'} into table parquet_01;
select * from parquet_01;

-- parquet type to mo type :int96->timestamp int32->int binary->varchar double->double
-- int96 not support
create table parquet_02 (registration_dttm timestamp, id int,first_name varchar(50),last_name varchar(50),email varchar(50),gender varchar(50),ip_address varchar(50),cc varchar(50),country varchar(50),birthdate varchar(50),salary double, title varchar(50),comment varchar(50));
load data infile {'filepath'='/Users/heni/test_data/parquet_data/kylo/samples/sample-data/parquet/userdata1.parquet','format'='parquet'} into table parquet_02;

-- parquet type to mo type : ?->datetime int16->smallint float->float
create table parquet_03(FL_DATE datetime,DEP_DELAY smallint,ARR_DELAY smallint,AIR_TIME smallint,DISTANCE smallint,DEP_TIME float,ARR_TIME float);
load data infile {'filepath'='/Users/heni/test_data/parquet_data/Flights_1m.parquet','format'='parquet'} into table parquet_03;

-- parquet type to mo type :string->double
create table parquet_04(`sepal.length` double,`sepal.width` double, `petal.length` double,`petal.width` double,`variety` double);
load data infile {'filepath'='/Users/heni/test_data/parquet_data/Iris.parquet','format'='parquet'} into table parquet_04;

--
create table parquet_05(model varchar(50),mpg double, cyl int,disp double,hp int,drat double,wt double,qsec double,vs int,am int,gear int,carb int);
load data infile {'filepath'='/Users/heni/test_data/parquet_data/mt_cars.parquet','format'='parquet'} into table parquet_05;

--
create table parquet_06(PassengerId bigint,Survived bigint, Pclass bigint,Name varchar(50),Sex varchar(50),Age double,SibSp bigint,Parch bigint,Ticket varchar(50),Fare double,Cabin varchar(50),Embarked varchar(50));
load data infile {'filepath'='/Users/heni/test_data/parquet_data/Titanic.parquet','format'='parquet'} into table parquet_06;



