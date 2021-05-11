create table WITH_PK
(
    ID   NUMBER(2)   not null primary key,
    NAME VARCHAR(32) not null,
    AGE INTEGER null
);

comment on column WITH_PK.ID is 'comment';
