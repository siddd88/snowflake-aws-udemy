use role sysadmin;
use database ecommerce_db;
 
create  or replace schema streams_test;

--- Create a raw table to test the streams  ---
CREATE OR REPLACE TABLE members_raw (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);

--- Create a production table which will consume the streams data  ---
CREATE OR REPLACE TABLE members_prod (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);


--- Create a standard (delta) stream on the raw table :members_raw---
CREATE OR REPLACE STREAM members_std_stream ON TABLE members_raw;

--- Check the streams ---- 
select * from members_std_stream

--- Check the stream offset ---- 
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream') as members_table_st_offset;

SELECT to_timestamp(SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream')) as members_table_st_offset;

--- Insert some data into the raw table : members_raw --- 

INSERT INTO members_raw (id,name,fee)
VALUES
(1,'Joe',0),
(2,'Jane',0),
(3,'George',0),
(4,'Betty',0),
(5,'Sally',0)


--- Check the streams ---- 
select * from members_std_stream

--- Check the stream offset ---- 
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream') as members_table_st_offset;

--- Query the streams data by ingesting the CDC streams data into the production table ---- 
SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT';

--- Consume the streams data by ingesting the CDC streams data into the production table : DML Operation ---- 
INSERT INTO members_prod(id,name,fee) 
SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT';

--- Check the production table -----
select * from members_prod;


--- Check the offset ---- 
SELECT to_timestamp(SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream')) as members_table_st_offset;



--- Insert some more data into the raw table ---
INSERT INTO members_raw (id,name,fee) VALUES (6,'sid',0),(7,'Billy',0),(8,'Katie',0);

--- Check the streams -----
select * from members_std_stream;

--- Update the raw table ----
update members_raw set fee=10 where id=7;


--- Check the streams -----
select * from members_std_stream;

--- Consume the streams ---
INSERT INTO members_prod(id,name,fee) SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT';


--- Lets update the raw table again ----

update members_raw set fee=20 where id=7;


--- Consume both (inserts and updates) from the streams --- 
merge into members_prod as mp 
using (select id,name,fee from members_std_stream mstr where metadata$action='INSERT' ) as mstr
on mp.id = mstr.id
when matched then update set mp.fee = mstr.fee,mp.name = mstr.name
when not matched then insert (id,name,fee) values (mstr.id,mstr.name,cast(mstr.fee as numeric));