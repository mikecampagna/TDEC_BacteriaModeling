
select a.comid, b.nhdplusid
from spatial.nhdplus_maregion as a
left join spatial.nhdplus_precip_mm01 as b
on a.nhdplusid = b.nhdplusid
where a.huc12 like '0204%'
and b.hydroseq is not null;



SELECT * FROM spatial.nhdplus_maregion LIMIT 100;
select * from spatial.nhdplus_precip_mm01 limit 100;

select * from spatial.nhdplus_0204 limit 100;

alter table spatial.nhdplus_0204 add column comid int;
alter table spatial.nhdplus_0204 add column nord int;
alter table spatial.nhdplus_0204 add column nordstop int;

update spatial.nhdplus_0204 a set comid = b.comid
from spatial.nhdplus_maregion as b
where st_intersects(st_centroid(a.geom), b.catchment)
;

update spatial.nhdplus_0204 a set nord = b.nord, nordstop = b.nordstop
from spatial.nhdplus_maregion as b
where st_intersects(st_centroid(a.geom), b.catchment)
;

create index nhdplus_0204_nhdplus_id_idx
on spatial.nhdplus_0204
using btree(nhdplusid)
;

create index nhdplus_0204_comid_idx
on spatial.nhdplus_0204
using btree(comid)
;

create index nhdplus_0204_nord_idx
on spatial.nhdplus_0204
using btree(nord, nordstop)
;

select a.comid, b.nhdplusid
from spatial.nhdplus_0204 as a
left join spatial.nhdplus_precip_mm01 as b
on a.nhdplusid = b.nhdplusid
where a.huc12 like '0204%'
and b.hydroseq is not null;

-- CREATE VIEW FOR PRECIP IN THE NHDPLUS HR TABLE

drop view if exists spatial.nhdplushr_precip_ma;
create view spatial.nhdplushr_precip_ma
as
select distinct nhdplusid, 
sum(precipmm) over (partition by nhdplusid) as precipmm_ma, 
hydroseq, vpuid
from (
select id, 01::int as month	
, objectid, nhdplusid, precipmm01 as precipmm, hydroseq, vpuid
from spatial.nhdplus_precip_mm01
union all 
select id, 02::int as month	
, objectid, nhdplusid, precipmm02, hydroseq, vpuid
from spatial.nhdplus_precip_mm02
union all 
select id, 03::int as month	
, objectid, nhdplusid, precipmm03, hydroseq, vpuid
from spatial.nhdplus_precip_mm03
union all 
select id, 04::int as month	
, objectid, nhdplusid, precipmm04, hydroseq, vpuid
from spatial.nhdplus_precip_mm04
union all 
select id, 05::int as month	
, objectid, nhdplusid, precipmm05, hydroseq, vpuid
from spatial.nhdplus_precip_mm05
union all 
select id, 06::int as month	
, objectid, nhdplusid, precipmm06, hydroseq, vpuid
from spatial.nhdplus_precip_mm06
union all 
select id, 07::int as month	
, objectid, nhdplusid, precipmm07, hydroseq, vpuid
from spatial.nhdplus_precip_mm07
union all 
select id, 08::int as month	
, objectid, nhdplusid, precipmm08, hydroseq, vpuid
from spatial.nhdplus_precip_mm08
union all 
select id, 09::int as month	
, objectid, nhdplusid, precipmm09, hydroseq, vpuid
from spatial.nhdplus_precip_mm09
union all 
select id, 10::int as month	
, objectid, nhdplusid, precipmm10, hydroseq, vpuid
from spatial.nhdplus_precip_mm10
union all 
select id, 11::int as month	
, objectid, nhdplusid, precipmm11, hydroseq, vpuid
from spatial.nhdplus_precip_mm11
union all 
select id, 12::int as month	
, objectid, nhdplusid, precipmm12, hydroseq, vpuid
from spatial.nhdplus_precip_mm12
) as t1
order by nhdplusid
;

-- ADD IN THE COLUMN TO THE NHDPLUS TABLE

alter table spatial.nhdplus_0204 add column precipmm_ma double precision;
update spatial.nhdplus_0204 a set precipmm_ma = b.precipmm_ma
from  spatial.nhdplus_precip_ma as b
where a.nhdplusid = b.nhdplusid
;

alter table spatial.nhdplus_maregion add column precipmm_ma double precision;
update spatial.nhdplus_maregion a set precipmm_ma = b.precipmm_ma
from  (
	select distinct comid, avg(precipmm_ma) as precipmm_ma from spatial.nhdplus_0204 group by comid order by comid
) as b
where a.comid = b.comid
;

-- WHERE THE VALUS IS NULL, GET THE AVERAGE VALUE FROM ALL NEIGHBORS

update spatial.nhdplus_maregion a set precipmm_ma = b.precipmm_ma
from  (
	select distinct t1.comid, avg(t2.precipmm_ma) over (partition by t1.comid) as precipmm_ma
			--t1.comid, t1.precipmm_ma, t2.comid, t2.precipmm_ma
	from (
		select a.comid, a.precipmm_ma, catchment
		from spatial.nhdplus_maregion as a 
		where a.precipmm_ma is null and a.huc12 like '0204%'
	) as t1
	left join (select comid, precipmm_ma, catchment from spatial.nhdplus_maregion where precipmm_ma is not null) as t2
	on st_intersects(t1.catchment, t2.catchment)
	where t1.comid != t2.comid
	order by t1.comid --, t2.comid
) as b
where a.comid = b.comid and a.precipmm_ma is null
;


-- Get a table just for the AOI

drop table if exists spatial.census_state_de_buff;
create table spatial.census_state_de_buff
as
select state_fips, ST_MakePolygon(ST_ExteriorRing(st_buffer(geom,100)))::geometry(Polygon,32618) as geom, st_area(geom) as area from (
select state_fips, ST_GeometryN(geom, generate_series(1, ST_NumGeometries(geom)))::geometry(Polygon,32618) as geom from spatial.census_state where state_fips = '10'
) as t1
order by area desc
limit 1;

create index census_state_de_buff_geom_idx
on spatial.census_state_de_buff
using gist(geom);



drop table if exists spatial.nhdplus_tdec;
create table spatial.nhdplus_tdec
as
select distinct t1.comid, t1.nord, t1.nordstop, catchment, geom, lineroutedwn,
gnis_id, gnis_name, huc12, streamorde, areasqkm, totdasqkm, temp0001, pop10, pop10upstrm, kffact, upskffact, maflowv, mavelv, slope, precipmm_ma
--select distinct t2.nord
from spatial.nhdplus_maregion as t1
join (
--create table spatial.nhdplus_tdec_temp as
select distinct comid, nord, nordstop--, catchment
from spatial.nhdplus_maregion as nhd
join spatial.census_state_de_buff as st
on st_intersects(st_centroid(nhd.catchment), st.geom)
--on st_intersects(nhd.catchment, st.geom)
--where nord is not null --and gnis_name not like 'Delaware River'
) as t2
on t1.nord between t2.nord and t2.nordstop
where huc12 like '0204%'
;

alter table spatial.nhdplus_tdec add constraint pk_nhdplus_tdec primary key (comid);

insert into spatial.nhdplus_tdec
select t1.*
from (
select distinct comid, nord, nordstop, catchment, nhd.geom, lineroutedwn,
gnis_id, gnis_name, huc12, streamorde, areasqkm, totdasqkm, temp0001, pop10, pop10upstrm, kffact, upskffact, maflowv, mavelv, slope, precipmm_ma
from spatial.nhdplus_maregion as nhd
join spatial.census_state_de_buff as st
on st_intersects(st_centroid(nhd.catchment), st.geom)
where huc12 like '0204%'
) as t1
left join (select comid from spatial.nhdplus_tdec) as t2
on t1.comid = t2.comid
where t2.comid is null
;

insert into spatial.nhdplus_tdec
select t1.*
from (
select distinct comid, nord, nordstop, catchment, nhd.geom, lineroutedwn,
gnis_id, gnis_name, huc12, streamorde, areasqkm, totdasqkm, temp0001, pop10, pop10upstrm, kffact, upskffact, maflowv, mavelv, slope, precipmm_ma
from spatial.nhdplus_maregion as nhd
join spatial.census_state_de_buff as st
on st_intersects(nhd.catchment, st.geom)
where huc12 like '0204%'
) as t1
left join (select comid from spatial.nhdplus_tdec) as t2
on t1.comid = t2.comid
where t2.comid is null
;

--------

select * from spatial.aoi;

alter table spatial.aoi add column geom_outer geometry(polygon,32618);
update spatial.aoi a set geom_outer = b.geom
from (
select id, ST_MakePolygon(ST_ExteriorRing(ST_Buffer(ST_GeometryN(geom, generate_series(1, ST_NumGeometries(geom))),50)))::geometry(Polygon,32618) as geom
from spatial.aoi
) b
where a.id = b.id
;

drop table if exists spatial.wbdhuc12_de;
create table spatial.wbdhuc12_de
as
select a.*
from spatial.wbdhuc12 as a
join spatial.aoi as b
on st_intersects(st_centroid(a.geom), b.geom_outer)
where a.states not like '%NJ%'
;

insert into spatial.wbdhuc12_de
select t1.*
from (
select distinct h12.*
from spatial.wbdhuc12 as h12
join spatial.census_state_de_buff as st
on st_intersects(h12.geom, st.geom)
where h12.huc12 like '0204%'
) as t1
left join (select huc12 from spatial.wbdhuc12_de) as t2
on t1.huc12 = t2.huc12
where t2.huc12 is null and t1.states not like '%NJ%'
;



























