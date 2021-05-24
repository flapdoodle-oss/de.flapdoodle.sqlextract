create table SAMPLE
(
    ID   NUMBER(2)   not null primary key,
    NAME VARCHAR(32) not null
);

comment on column SAMPLE.ID is 'comment';

INSERT INTO SAMPLE (ID, NAME)
VALUES (1, 'Klaus');
