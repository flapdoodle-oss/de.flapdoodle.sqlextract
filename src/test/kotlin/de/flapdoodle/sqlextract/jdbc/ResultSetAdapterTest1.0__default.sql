create table SAMPLE (
    ID NUMBER(2) not null,
    NAME VARCHAR(32) not null
--     constraint PK_ID
--         primary key (ID)
);

comment on column SAMPLE.ID is 'comment';

INSERT INTO SAMPLE (ID,NAME)
VALUES   (1,'Klaus');
