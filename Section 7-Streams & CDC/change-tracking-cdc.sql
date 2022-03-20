use database ecommerce_db;

create or replace schema streams_test;

CREATE OR REPLACE TABLE members_raw (
  id number(8) NOT NULL,
  name varchar(255) default NULL,
  fee number(3) NULL,
  member_type varchar(10) not null
);

--- Enable change tracking ---- 
ALTER TABLE members_raw SET CHANGE_TRACKING = TRUE;

SET ts1 = (SELECT CURRENT_TIMESTAMP());

INSERT INTO members_raw (id,name,fee,member_type)
VALUES
(1,'Joe',0,'games'),
(2,'Jane',0,'music'),
(3,'George',0,'games');

update members_raw set fee=20 where id=3;

SELECT * FROM members_raw CHANGES(INFORMATION => default) at(TIMESTAMP =>$ts1);

SET ts2 = (SELECT CURRENT_TIMESTAMP());

INSERT INTO members_raw (id,name,fee,member_type)
VALUES
(4,'Betty',0,'games'),
(5,'Sally',0,'music');


SELECT * FROM members_raw CHANGES(INFORMATION => default) at(TIMESTAMP =>$ts1);

SELECT * FROM members_raw CHANGES(INFORMATION => default) at(TIMESTAMP => $ts2);



