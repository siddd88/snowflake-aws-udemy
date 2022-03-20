use role sysadmin;
use database ecommerce_db;
 
create  or replace schema streams_test;

--- Create a raw table to test the streams  ---
CREATE OR REPLACE TABLE members_raw (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL,
  member_type varchar(10) not null
);

--- Create a production table which will consume the streams data  ---
CREATE OR REPLACE TABLE members_music_prod (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL
);

CREATE OR REPLACE TABLE members_games_prod (
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

INSERT INTO members_raw (id,name,fee,member_type)
VALUES
(1,'Joe',0,'games'),
(2,'Jane',0,'music'),
(3,'George',0,'games'),
(4,'Betty',0,'games'),
(5,'Sally',0,'music');


--- Check the streams ---- 
select * from members_std_stream

--- Check the stream offset ---- 
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream') as members_table_st_offset;

--- Query the streams data by ingesting the CDC streams data into the production table ---- 
SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT';

--- Beging the transaction & consume the streams data  ---- 

begin ;

insert into members_games_prod
SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT' and member_type='games';

insert into members_music_prod
SELECT id, name, fee FROM members_std_stream WHERE METADATA$ACTION = 'INSERT' and member_type='music';


commit;

--- Check the production tables -----
select * from members_games_prod;
select * from members_music_prod;


--- Check the offset ---- 
SELECT to_timestamp(SYSTEM$STREAM_GET_TABLE_TIMESTAMP('members_std_stream')) as members_table_st_offset;


