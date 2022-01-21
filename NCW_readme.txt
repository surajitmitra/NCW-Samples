use role SYSADMIN
use database NCW_TEST

-- create internal stage & table
create or replace stage cell1  file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1)
CREATE TABLE cell1 (
    softsectorId    varchar(100),
    bbuId    varchar(100),
    entityId    int,
    cellNum    int,
    channel    varchar(100),
    carrierOrdinal    int,
    tech    varchar(100),
    sector    varchar(100),
    freq    varchar(100),
    hatchLocProp    varchar(100),
    rcn    int,
    carrierType    varchar(100)
)

-- create "meta" tables for config data; "job" tables for run status
create table meta_job (jobid int, jobtype varchar(20), filename varchar(100), is_header boolean, active boolean default false );
create table meta_job_fldmap (jobid int, src_fld varchar(100), tgt_fld varchar(100), tgt_type varchar(10), maxlen int);
create table job_status (runid int, jobid int, run_status varchar(20), err_msg varchar(1000), numrow int, start_at timestamp, end_at timestamp );

-- insert few rows into config tables
insert into meta_job values (22, 'CSV', 'cell1', 'Y', 'Y');

insert into meta_job_fldmap values (22, 'softsectorId', 'softsectorId', 'STR', 25);
insert into meta_job_fldmap values (22, 'bbuId', 'bbuId', 'STR', 20);
insert into meta_job_fldmap values (22, 'entityId', 'entityId', 'INT', 0);
insert into meta_job_fldmap values (22, 'cellNum', 'cellNum', 'INT', 0);
insert into meta_job_fldmap values (22, 'channel', 'channel', 'STR', 30);
insert into meta_job_fldmap values (22, 'carrierOrdinal', 'carrierOrdinal', 'INT', 0);
insert into meta_job_fldmap values (22, 'tech', 'tech', 'STR', 5);
insert into meta_job_fldmap values (22, 'sector', 'sector', 'STR', 1);
insert into meta_job_fldmap values (22, 'freq', 'freq', 'STR', 3);
insert into meta_job_fldmap values (22, 'hatchLocProp', 'hatchLocProp', 'STR', 10);
insert into meta_job_fldmap values (22, 'rcn', 'rcn', 'INT', 0);
insert into meta_job_fldmap values (22, 'carrierType', 'carrierType', 'STR', 10);


