
select *
from spatial.nhdplus_erom 
limit 100;

select *
from spatial.nhdplus_eromqa
limit 100;

select avg(temp0001), avg(ppt0001)
from spatial.nhdplus_erom 
limit 100;

select *
from spatial.nhdplus_hr
limit 100;

alter table spatial.nhdplus_hr drop constraint nhdplus_hr_pkey;
alter table spatial.nhdplus_hr add constraint pk_nhdplus_hr primary key (id);
















