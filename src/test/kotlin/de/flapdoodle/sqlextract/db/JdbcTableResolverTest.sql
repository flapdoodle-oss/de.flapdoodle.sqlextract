create table WITH_PK
(
    ID   NUMBER(2)   not null primary key,
    NAME VARCHAR(32) not null,
    AGE INTEGER null
);

comment on column WITH_PK.ID is 'comment';

create table WITH_FK
(
    ID   NUMBER(2)   not null primary key,
    NAME VARCHAR(32) not null,
    REF NUMBER(2) null,
--     foreign key (WITH_PK) references WITH_PK(ID)
    CONSTRAINT FK_CONSTRAINT FOREIGN KEY (REF) REFERENCES WITH_PK(ID)
);