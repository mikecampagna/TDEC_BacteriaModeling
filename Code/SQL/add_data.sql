
create table dnrec.construction_projects
(
noi_permit_number int
,project_name varchar(256)
,latitude numeric(10,8)
,longitude numeric(10,8)
,receiving_waters varchar(256)
,total_land_area numeric(10,3)
,estimated_area numeric(10,3)
,construct_start date
,construct_end date
);

select * from dnrec.construction_projects;

alter table dnrec.construction_projects add constraint pk_construction_projects primary key (noi_permit_number);

alter table dnrec.construction_projects add column geom geometry(point,32618);

update dnrec.construction_projects set geom = st_transform(st_setsrid(st_makepoint(longitude, latitude),4326),32618);



create table dnrec.usgs_gages
as select * from ml_stream_temp.usgs_gages

CREATE INDEX usgs_gages_geom_idx
  ON dnrec.usgs_gages
  USING gist
  (geom);

alter table dnrec.usgs_gages add constraint pk_usgs_gages primary key (site_no);


create table dnrec.flow_gaged
(
	year	int
,	site_id	int
,	agency	varchar(32)
,	flow_code	int
,	ts_id	int
,	site_name	varchar(256)
,	drainage_area_sqmi	numeric(10,3)
,	annus_mean_cfs	numeric(10,3)
,	provisional	varchar(12)
,	annual_runoff_rate_cfs_per_sqmi	numeric(10,3)
);

alter table dnrec.flow_gaged
add constraint pk_flow_gaged
primary key (site_id, year);

select distinct site_id from dnrec.flow_gaged;

alter table dnrec.flow_gaged add column latitude numeric(12,8);
alter table dnrec.flow_gaged add column longitude numeric(12,8);
alter table dnrec.flow_gaged add column geom geometry(point,32618);

update dnrec.flow_gaged a set latitude = b.lat_gage, longitude = b.lng_gage, geom = b.geom
from (
select distinct site_id, lat_gage, lng_gage, b.geom
from dnrec.flow_gaged as a
join dnrec.usgs_flowgages as b
on a.site_id = b.staid::bigint
) as b
where a.site_id = b.site_id
;

update dnrec.flow_gaged a set geom = b.geom
from (
select distinct site_id, b.geom
from dnrec.flow_gaged as a
join dnrec.nwis_gages as b
on a.site_id = b.staid::bigint
) as b
where a.site_id = b.site_id and a.geom is null
;

update dnrec.flow_gaged set latitude = st_y(st_transform(geom,4326)), longitude = st_x(st_transform(geom,4326))
where geom is not null and latitude is null
;

select * 
from dnrec.flow_gaged 
where latitude is null
order by site_id, year;

update dnrec.flow_gaged
set latitude = case
					when site_id = 1483153 then 39.433352 else latitude end
,    longitude = case
					when site_id = 1483153 then -75.683448 else latitude end
;

update dnrec.flow_gaged set geom = st_transform(st_setsrid(st_makepoint(longitude, latitude),4326),32618) where geom is null;

select * 
from dnrec.flow_gaged 
order by site_id, year;

-- KILLME THEY DIDNT INCLUDE THE SITE ID
drop table if exists dnrec.flow_watershed;
create table dnrec.flow_watershed
(
	year	int
,	site_id int
,	watershed_name	varchar(256)
,	watershed_area_sqmi	numeric(10,3)
,	flow_gage_name	varchar(256)
,	gaged_area_sqmi	numeric(10,3)
,	gaged_annual_mean_cfs	numeric(10,3)
,	area_ratio_watershed_to_gaged	numeric(10,3)
,	watershed_annual_mean_cfs	numeric(10,3)
,	watershed_annual_runoff_cfs_per_sqmi	numeric(10,3)
);

select * from dnrec.flow_watershed;
--14
select distinct lower(flow_gage_name) as flow_gage_name from dnrec.flow_watershed order by flow_gage_name;

select distinct lower(staname) as staname from dnrec.usgs_flowgages;

CREATE INDEX idx_usgs_flowgages_name
  ON dnrec.usgs_flowgages
  USING btree
  (staname);

CREATE INDEX idx_flow_watershed_name
  ON dnrec.flow_watershed
  USING btree
  (flow_gage_name);

alter table dnrec.flow_watershed add column latitude numeric(12,8);
alter table dnrec.flow_watershed add column longitude numeric(12,8);
alter table dnrec.flow_watershed add column geom geometry(point,32618);

update dnrec.flow_watershed a set latitude = b.lat, longitude = b.lng, geom = b.geom, site_id = b.staid
from (
select 
distinct lower(flow_gage_name) as flow_gage_name, staid::int as staid, st_x(st_transform(b.geom,4326)) as lat, st_y(st_transform(b.geom,4326)) as lng, b.geom
from dnrec.flow_watershed as a
join dnrec.usgs_flowgages as b
on lower(flow_gage_name) like lower(staname)
order by flow_gage_name
) as b
where lower(a.flow_gage_name) like b.flow_gage_name
and a.site_id is null
;

select distinct flow_gage_name
,length(flow_gage_name), * 
from dnrec.flow_watershed 
where site_id is null
order by flow_gage_name, watershed_name, year;


select * from dnrec.usgs_flowgages where lower(staname) like '%jones%'
order by staname;

select * from dnrec.nwis_gages where lower(staname) like '%noxon%'
order by staname;

--'NOXONTOWN LAKE OUTLET NEAR MIDDLETOWN, DE'

update dnrec.flow_watershed
set flow_gage_name = 'BEAVERDAM DITCH NEAR MILLVILLE, DE'
where flow_gage_name like 'BEAVERDAM DITCH NEAR MILLVILLE'

update dnrec.flow_watershed a set latitude = b.lat, longitude = b.lng, geom = b.geom, site_id = b.staid
from (
select 
distinct lower(flow_gage_name) as flow_gage_name, staid::int as staid, st_x(st_transform(b.geom,4326)) as lat, st_y(st_transform(b.geom,4326)) as lng, b.geom
from dnrec.flow_watershed as a
join dnrec.nwis_gages as b
on lower(flow_gage_name) like lower(staname)
order by flow_gage_name
) as b
where lower(a.flow_gage_name) like b.flow_gage_name
and a.site_id is null
;

update dnrec.flow_watershed a set latitude = b.lat, longitude = b.lng, geom = b.geom, site_id = b.staid
from (
select 
distinct lower(flow_gage_name) as flow_gage_name, b.site_id::int as staid, st_y(st_transform(b.geom,4326)) as lat, st_y(st_transform(b.geom,4326)) as lng, b.geom
from dnrec.flow_watershed as a
join dnrec.flow_gaged as b
on lower(flow_gage_name) like lower(site_name)
order by flow_gage_name
) as b
where lower(a.flow_gage_name) like b.flow_gage_name
and a.site_id is null
;


---------------------------------------------------
-- ALL 	DISTINCT ROWS --
---------------------------------------------------


update dnrec.flow_gaged set latitude = st_y(st_transform(geom,4326)), longitude = st_x(st_transform(geom,4326));
update dnrec.flow_watershed set latitude = st_y(st_transform(geom,4326)), longitude = st_x(st_transform(geom,4326));

drop table if exists dnrec.usgs_gages;
create table dnrec.usgs_gages
as
select distinct staid, staname, count(staid) over (partition by staid) as n_years, outfall_bay, latitude, longitude, 
geom, geom_snap
from (
select distinct * from (
select site_id as staid, site_name as staname, year, latitude, longitude, geom, outfall_bay, geom_snap
from dnrec.flow_gaged
--order by staid, year
union all
select site_id as staid, flow_gage_name, year, latitude, longitude, geom, outfall_bay, geom_snap
from dnrec.flow_watershed
--order by staid, year
) as a
order by staid, staname, year
) as b
order by n_years desc, staname;

select * from dnrec.usgs_gages;

-- NOT ALL IN DRB
alter table dnrec.flow_gaged add column outfall_bay varchar(36);
alter table dnrec.flow_watershed add column outfall_bay varchar(36);

update dnrec.flow_watershed a set outfall_bay = 'Delaware'
from spatial.drbbounds as b
where st_intersects(a.geom, b.geom);

update dnrec.flow_watershed a set outfall_bay = 'Chesapeake'
from spatial.cbbbounds as b
where st_intersects(a.geom, b.geom);

update dnrec.flow_watershed a set outfall_bay = 'Other'
where outfall_bay is null;

select * from dnrec.flow_watershed;

-- Snap geom to FDR
select * from public.usgs_gages_snapped;
select * from dnrec.flow_gaged;
select * from dnrec.flow_watershed;

alter table dnrec.flow_gaged add column geom_snap geometry(point,32618);
alter table dnrec.flow_watershed add column  geom_snap geometry(point,32618);

update dnrec.flow_gaged a set geom_snap = b.geom
from public.usgs_gages_snapped b
where a.site_id = b.staid;

update dnrec.flow_watershed a set geom_snap = b.geom
from public.usgs_gages_snapped b
where a.site_id = b.staid;

-- add in WS

alter table dnrec.usgs_gages add column geom_ws geometry(polygon,32618);

select * from dnrec.usgs_gages;

-- RUN THE WATERSHED DELINEATION ALGORITHM AND ADD NEW GEOMS TO TABLE

-- NHDPLUS VERSION

select * from dnrec.usgs_gages;
select nord from spatial.nhdplus_maregion;

alter table dnrec.usgs_gages add column nord int;
update dnrec.usgs_gages a set nord = b.nord
from spatial.nhdplus_maregion b
where st_intersects(a.geom, b.catchment);

alter table dnrec.usgs_gages add column geom_nhd geometry(MultiPolygon,32618);

update dnrec.usgs_gages a set geom_nhd = b.watershed_geom
from (
 SELECT t3.name, st_multi(st_buffer(st_collect(w4.catchment), 0.000001::double precision))::geometry(MultiPolygon,32618) AS watershed_geom
   FROM ( SELECT t2.name, t2.bsenord, w3.gid, w3.nord, w3.nordstop, w3.mavelv, w3.lengthkm
           FROM ( SELECT t1.name, max(w2.nord) AS bsenord
                   FROM ( SELECT f.name, min(w.nord) AS minnord, max(w.nord) AS mxnord
                           FROM ( SELECT fi.nme AS name, st_buffer(st_collect(fi.geom), (-25)::double precision) AS geom
                                   FROM ( SELECT a.staid AS nme, catchment as geom
                                           FROM dnrec.usgs_gages as a -- INSERT NEW DATAPRODUCT TABLE HERE
											left join spatial.nhdplus_maregion as b
											on a.nord = b.nord ) fi
                                  GROUP BY fi.nme) f
                      JOIN spatial.nhdplus_maregion w ON st_intersects(f.geom, w.catchment)
                     GROUP BY f.name) t1
              JOIN spatial.nhdplus_maregion_idx w2 ON w2.nord between t1.minnord and t1.mxnord
             GROUP BY t1.name) t2
      JOIN spatial.nhdplus_maregion_idx w3 ON t2.bsenord = w3.nord) t3
   JOIN spatial.nhdplus_maregion w4 ON w4.nord between t3.nord and t3.nordstop
  GROUP BY t3.name
) b
where a.staid = b.name
;

-----------------------------------------------------
-- LINK CONSTRUCTION PROJECTS TO GAGES

select * from dnrec.construction_projects
order by usgs_gage_staid ;

dnrec.usgs_gages
geom_nhd

alter table dnrec.construction_projects add column usgs_gage_staid int;

update dnrec.construction_projects a set usgs_gage_staid = b.staid
from (
select b.staid, a.*
from dnrec.construction_projects as a
left join dnrec.usgs_gages as b
on st_intersects(a.geom, b.geom_nhd)
order by b.staid
) as b
where a.noi_permit_number = b.noi_permit_number
;

alter table dnrec.construction_projects add column in_gage_ws boolean default 'f';
update dnrec.construction_projects set in_gage_ws = 't' where usgs_gage_staid is not null;

select noi_permit_number			
,project_name			
,latitude			
,longitude			
,receiving_waters			
,total_land_area			
,estimated_area			
,construct_start			
,construct_end					
,usgs_gage_staid			
,in_gage_ws
from dnrec.construction_projects order by usgs_gage_staid;

----------
-- Construction Projects outside of DE

alter table dnrec.construction_projects add column state_fips varchar(2);
alter table dnrec.construction_projects add column state_abbr varchar(2);

update dnrec.construction_projects a set state_fips = b.state_fips, state_abbr = b.state_abbr
from (
select a.noi_permit_number, b.state_fips, b.state_abbr 
from dnrec.construction_projects as a
join spatial.census_state as b
on st_intersects(a.geom, b.geom)
) as b
where a.noi_permit_number = b.noi_permit_number;

select noi_permit_number			
,project_name			
,latitude			
,longitude			
,receiving_waters			
,total_land_area			
,estimated_area			
,construct_start			
,construct_end						
,usgs_gage_staid			
,in_gage_ws			
,state_fips			
,state_abbr 
from dnrec.construction_projects
where state_fips not like '10' or state_fips is null
order by state_fips, noi_permit_number;

-----------------------------------------------------------------

-- GAGED AVERAGE FLOW
select * from dnrec.flow_gaged order by site_id, year;
select * from dnrec.usgs_gages;

create or replace view  dnrec.flow_gaged_final
as
select distinct a.staid, a.staname, a.n_years, a.outfall_bay, a.latitude, a.longitude, 
avg(annus_mean_cfs) over (partition by staid)::numeric(8,3) as maflow_cfs, 
avg(drainage_area_sqmi) over (partition by staid)::numeric(8,3) as drainage_area_sqmi, 
avg(annual_runoff_rate_cfs_per_sqmi) over (partition by staid)::numeric(8,3) as marunoff_cfs_per_sqmi,
a.geom, a.geom_nhd
from dnrec.usgs_gages as a
join dnrec.flow_gaged as b
on a.staid = b.site_id
order by staid, outfall_bay, n_years
;

select staid			
,staname			
,n_years			
,outfall_bay			
,latitude			
,longitude			
,maflow_cfs			
,drainage_area_sqmi			
,marunoff_cfs_per_sqmi			
from dnrec.flow_gaged_final
;

-----------------------------------------------------------------

-- WATERSHED AVERAGE FLOW
select * from dnrec.flow_watershed order by site_id, watershed_name, year;
select * from dnrec.usgs_gages;

-- Find geometries for the different watersheds
alter table dnrec.flow_watershed add column gnis_name varchar(256);

update dnrec.flow_watershed a set gnis_name = b.gnis_name
from (
select distinct watershed_name, b.gnis_name
from dnrec.flow_watershed as a
join spatial.nhdplus_maregion as b
on a.watershed_name like b.gnis_name
) as b
where a.watershed_name like b.watershed_name and a.gnis_name is null
;

select distinct watershed_name from dnrec.flow_watershed where gnis_name is null order by watershed_name;
select distinct watershed_name from dnrec.flow_watershed where gnis_name is not null order by watershed_name;

--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Assawoman%';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Brandywine%';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Christina%';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Dragon%Creek';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Naaman%';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Red Clay Creek%';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'S%Jones%River';
--select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'White Clay Creek%';

update dnrec.flow_watershed set gnis_name = 
	case
		when watershed_name like 'Assawoman Bay (in DE)' then 'Assawoman Creek'
		when watershed_name like 'Brandywine River in DE' then 'Brandywine Creek'
		when watershed_name like 'Christina River in DE' then 'Christina River'
		when watershed_name like 'Dragon Run Creek' then 'Dragon Creek'
		when watershed_name like 'Naamans Creek in DE' then 'Naaman Creek'
		when watershed_name like 'Red Clay Creek in DE' then 'Red Clay Creek'
		when watershed_name like 'St. Jones River' then 'Saint Jones River'
		when watershed_name like 'White Clay Creek in DE' then 'White Clay Creek'
		--when watershed_name like 'Indian Riv&Bay and Behoboth Bay' then 
		--when watershed_name like 'Little Assawoman Bay' then 
		--when watershed_name like 'Delaware Bay' then 
		--when watershed_name like 'C & D Canal East' then 
else gnis_name end
;

select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'C & D Canal East';
select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Delaware Bay';
select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Indian Riv&Bay and Behoboth Bay';
select distinct gnis_name from spatial.nhdplus_maregion where gnis_name like 'Little Assawoman Bay';

alter table dnrec.flow_watershed add column minnord int;
update dnrec.flow_watershed a set minnord = b.minnord
from (
select distinct a.gnis_name, min(nord) over (partition by a.gnis_name) as minnord
from dnrec.flow_watershed as a
join spatial.nhdplus_maregion as b
on a.gnis_name like b.gnis_name
) as b
where a.gnis_name like b.gnis_name
;

CREATE INDEX idx_flow_watershed_name_2
  ON dnrec.flow_watershed
  USING btree
  (watershed_name);

alter table dnrec.flow_watershed add column geom_flow_watershed geometry(MultiPolygon,32618);

update dnrec.flow_watershed a set geom_flow_watershed = b.watershed_geom
from (
 SELECT t3.name, st_multi(st_buffer(st_collect(w4.catchment), 0.000001::double precision))::geometry(MultiPolygon,32618) AS watershed_geom
   FROM ( SELECT t2.name, t2.bsenord, w3.gid, w3.nord, w3.nordstop, w3.mavelv, w3.lengthkm
           FROM ( SELECT t1.name, max(w2.nord) AS bsenord
                   FROM ( SELECT f.name, min(w.nord) AS minnord, max(w.nord) AS mxnord
                           FROM ( SELECT fi.nme AS name, st_buffer(st_collect(fi.geom), (-25)::double precision) AS geom
                                   FROM ( SELECT a.watershed_name AS nme, catchment as geom
                                           FROM dnrec.flow_watershed as a -- INSERT NEW DATAPRODUCT TABLE HERE
											left join spatial.nhdplus_maregion as b
											on a.minnord = b.nord ) fi
                                  GROUP BY fi.nme) f
                      JOIN spatial.nhdplus_maregion w ON st_intersects(f.geom, w.catchment)
                     GROUP BY f.name) t1
              JOIN spatial.nhdplus_maregion_idx w2 ON w2.nord between t1.minnord and t1.mxnord
             GROUP BY t1.name) t2
      JOIN spatial.nhdplus_maregion_idx w3 ON t2.bsenord = w3.nord) t3
   JOIN spatial.nhdplus_maregion w4 ON w4.nord between t3.nord and t3.nordstop
  GROUP BY t3.name
) b
where a.watershed_name = b.name
;

alter table dnrec.flow_watershed add column flow_watershed_geom_area numeric(10,3);
update dnrec.flow_watershed a set flow_watershed_geom_area = (st_area(geom_flow_watershed)::numeric/1000000.0) * 0.386102;

select * from dnrec.flow_watershed order by site_id, watershed_name, year;


-- USING THE DNREC TMDL WATERSHEDS

select distinct watershed_name from dnrec.flow_watershed; --26
select * from spatial.dnrec_watersheds; --47

alter table dnrec.flow_watershed add column geom_dnrec geometry(MultiPolygon,32618);

update dnrec.flow_watershed a set geom_dnrec = b.geom
from (
select distinct a.watershed_name, b.watershed, b.geom 
from dnrec.flow_watershed as a
join spatial.dnrec_watersheds as b
on a.watershed_name like b.watershed
) as b
where a.watershed_name like b.watershed_name and a.geom_dnrec is null
;

select distinct watershed_name from dnrec.flow_watershed where geom_dnrec is null;

--'Assawoman Bay (in DE)'
--'Brandywine River in DE'
--'Little Assawoman Bay'
--'Christina River in DE'
--'White Clay Creek in DE'
'Indian Riv&Bay and Behoboth Bay'
--'Red Clay Creek in DE'
--'Naamans Creek in DE'

select * from spatial.dnrec_watersheds where watershed like 'Indian Riv&Bay and Behoboth Bay';

-- THIS IS A VERY WEIRD ONE! Xie said to combine 38 - 42, but I think it should just be 39 - 42...
select st_multi(st_buffer(st_union(geom),-0.0001))::geometry(multipolygon, 32618) as geom from (
select st_buffer(geom, 0.0001) as geom from spatial.dnrec_watersheds where watershed in ('Rehoboth Bay', 'Indian River', 'Iron Branch', 'Indian River Bay')
) as t1
;

update dnrec.flow_watershed a set geom_dnrec = b.geom
from (
select st_multi(st_buffer(st_union(geom),-0.0001))::geometry(multipolygon, 32618) as geom from (
select st_buffer(geom, 0.0001) as geom from spatial.dnrec_watersheds where watershed in ('Rehoboth Bay', 'Indian River', 'Iron Branch', 'Indian River Bay')
) as t1
) as b
where a.geom_dnrec is null and a.watershed_name like 'Indian Riv&Bay and Behoboth Bay'
;

alter table dnrec.flow_watershed add column dnrec_ws_area_sqmi numeric(10,3);
update dnrec.flow_watershed a set dnrec_ws_area_sqmi = (st_area(geom_dnrec)::numeric/1000000.0) * 0.386102;

select * from dnrec.flow_watershed order by site_id, watershed_name, year;

------------------------------------------------------------------------------------------
-- create the final table

drop table if exists dnrec.flow_watershed_final;
create table  dnrec.flow_watershed_final
as
select distinct a.staid, a.staname, a.n_years, a.outfall_bay, a.latitude, a.longitude, 
b.watershed_name,
c.drainage_area_sqmi as gaged_area_sqmi, 
avg(watershed_area_sqmi) over (partition by a.staid, watershed_name)::numeric(8,3) as watershed_area_sqmi, 
avg(area_ratio_watershed_to_gaged) over (partition by a.staid, watershed_name)::numeric(8,3) as area_ratio_watershed_to_gaged, 
c.maflow_cfs as gaged_maflow_cfs, 
avg(watershed_annual_mean_cfs) over (partition by a.staid, watershed_name)::numeric(8,3) as watershed_maflow_cfs, 
avg(watershed_annual_runoff_cfs_per_sqmi) over (partition by a.staid, watershed_name)::numeric(8,3) as watershed_marunoff_cfs_per_sqmi,
avg(flow_watershed_geom_area) over (partition by a.staid, watershed_name)::numeric(8,3) as nhdplus_chk_watershed_area_sqmi,
avg(dnrec_ws_area_sqmi) over (partition by a.staid, watershed_name)::numeric(8,3) as dnrec_chk_watershed_area_sqmi,
a.geom, a.geom_nhd, b.geom_flow_watershed, b.geom_dnrec
from dnrec.usgs_gages as a
join dnrec.flow_watershed as b
on a.staid = b.site_id
join dnrec.flow_gaged_final as c
on a.staid = c.staid
order by a.staid, a.outfall_bay, a.n_years, b.watershed_name
;

select * from dnrec.flow_watershed_final;

select staid			
,staname			
,n_years			
,outfall_bay			
,latitude			
,longitude			
,watershed_name			
,gaged_area_sqmi			
,watershed_area_sqmi			
,area_ratio_watershed_to_gaged			
,gaged_maflow_cfs			
,watershed_maflow_cfs			
,watershed_marunoff_cfs_per_sqmi			
,nhdplus_chk_watershed_area_sqmi			
from dnrec.flow_watershed_final
order by staid, watershed_name, n_years;





