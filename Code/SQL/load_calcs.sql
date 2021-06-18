
drop table if exists dnrec.septicsystems;
create table dnrec.septicsystems
(
	pild	int
,	flow_galday	int
,	systemclass	varchar(64)
,	facid	int
,	facname	varchar(256)
,	piname	varchar(256)
,	lat	numeric(11,8)
,	lng	numeric(11,8)

);

alter table dnrec.septicsystems add constraint pk_septicsystems primary key (pild, facid);
alter table dnrec.septicsystems add column geom geometry(point,32618);
update dnrec.septicsystems set geom = st_transform(st_setsrid(st_makepoint(lng,lat),4326),32618)::geometry(point,32618);
create index septicsystems_geom_idx
on dnrec.septicsystems
using gist(geom);

select sum("B00002e1") 
from spatial.acs_2018_bg_counts 
; --35070

select sum(b25001e1) 
from spatial.acs_2018_bg_housing 
; --428251

select sum(B25002e2) 
from spatial.acs_2018_bg_housing 
; --357765


select sum(b01001e1) 
from spatial.acs_2018_bg_totpop 
; --949495

drop table if exists spatial.temp_du;
create table spatial.temp_du
as
select a."GEOID" as geoid
,b.b25001e1 as dwelling_units
,a.geom
from spatial.acs_2018_bg as a
left join spatial.acs_2018_bg_housing as b
on a."OBJECTID" = b.objectid
;


select distinct comid, sum(nhd_du) over (partition by comid) as nhd_du
from (
	select id, comid, int_area/bg_area as p_bg, (int_area/bg_area) * tot_du as nhd_du, tot_du
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.comid
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.catchment) as nhd_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.nhdplus_tdec as d
		on st_intersects(a.geom, d.catchment)
		order by a.id, d.comid
	) t1
	order by comid
) as t2
;


select * from spatial.nhdplus_tdec limit 1000;

select * from spatial.wbdhuc12_de;

alter table spatial.nhdplus_tdec add column acs_bg_id bigint;
update spatial.nhdplus_tdec a set acs_bg_id = b.id
from spatial.acs_2018_bg as b
where st_intersects(st_centroid(a.geom), b.geom);

alter table spatial.nhdplus_tdec drop column acs_pop;
alter table spatial.nhdplus_tdec drop column acs_du;

alter table spatial.nhdplus_tdec add column acs_pop int;
alter table spatial.nhdplus_tdec add column acs_du int;

-- UPDATE POPULATION
update spatial.nhdplus_tdec a set acs_pop = b.nhd_pop::int
from (
select distinct comid, sum(nhd_pop) over (partition by comid) as nhd_pop
from (
	select id, comid, int_area/bg_area as p_bg, (int_area/bg_area) * tot_pop as nhd_pop, tot_pop
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.comid
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.catchment) as nhd_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.nhdplus_tdec as d
		on st_intersects(a.geom, d.catchment)
		order by a.id, d.comid
	) t1
	order by comid
) as t2
) as b
where a.comid = b.comid

-- UPDATE DWELLING UNITS
update spatial.nhdplus_tdec a set acs_du = b.nhd_du::int
from (
select distinct comid, sum(nhd_du) over (partition by comid) as nhd_du
from (
	select id, comid, int_area/bg_area as p_bg, (int_area/bg_area) * tot_du as nhd_du, tot_du
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.comid
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.catchment) as nhd_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.catchment)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.nhdplus_tdec as d
		on st_intersects(a.geom, d.catchment)
		order by a.id, d.comid
	) t1
	order by comid
) as t2
) as b
where a.comid = b.comid
;

-- SEPTIC LOAD
-- avg person per du is the number of bedrooms, 120 gallons of septic per bedroom, 21% of DU are unsewered

alter table spatial.nhdplus_tdec add column septic_galyear numeric(12,2);
update spatial.nhdplus_tdec a set septic_galyear = b.septic_gal_yr
from (
	select t1.comid, septic_gal_yr + coalesce(septic_gal_yr_pt,0) as septic_gal_yr
	from (
		select comid, (acs_pop::numeric / acs_du::numeric * 0.21) * 120.0 * 365.25 as septic_gal_yr
		from spatial.nhdplus_tdec
		where acs_du > 0.0
	) as t1
	left join (
		-- SHOULD WE TREAT COMMERCIAL vs COMMUNITY systems differently?
		select b.comid, 
			case when systemclass = 'Community' then (a.flow_galday * 365.25)* 1.0
			else a.flow_galday * 365.25
			end as septic_gal_yr_pt
		from dnrec.septicsystems as a
		join spatial.nhdplus_tdec as b
		on st_intersects(a.geom, b.catchment)
	) as t2
	on t1.comid = t2.comid
	order by t1.comid
) as b
where a.comid = b.comid
;

select * from spatial.nhdplus_tdec where septic_galyear < 0

-- NUMBER OF SYSTEMS == NUMBE OF UNSEWERED DWELLING UNITS
alter table spatial.nhdplus_tdec add column n_septic_systems bigint;
update spatial.nhdplus_tdec a set n_septic_systems = b.n_septic_systems
from (
	select t1.comid, t1.n_septic_systems + coalesce(t2.n_septic_systems_pt,0) as n_septic_systems
	from (
		select comid, (acs_du::numeric * 0.21)::int as n_septic_systems
		from spatial.nhdplus_tdec
		where acs_du > 0.0
		) as t1
	left join (
		-- SHOULD WE TREAT COMMERCIAL vs COMMUNITY systems differently?
		select b.comid, count(a.facid) over (partition by b.comid) as n_septic_systems_pt
		from dnrec.septicsystems as a
		join spatial.nhdplus_tdec as b
		on st_intersects(a.geom, b.catchment)
	) as t2
	on t1.comid = t2.comid
	order by t1.comid
) as b
where a.comid = b.comid
;

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------
-- GET OSDS SEPTIC LOAD, bnMPN/yr
-- Untreated Sewage Delivered to Septics (billions) = Unsewered dwelling units * Avg. Person/DU * Water Use (GPCD) * constant * FC (MPN/100ml)
-- Bacteria (bn/yr) = (untreated sewage delivered to septics * failure rates * normal delivery ratio * % septics not near waterway * normal bacteria decay %) + (delivery ratio adjacent to waterway * % septics near waterway * bacteria decay adjacent to waterway)

alter table spatial.nhdplus_tdec add column septic_bnmpn_yr numeric(12,2);
alter table spatial.nhdplus_tdec add column petwaste_bnmpn_yr numeric(12,2);
alter table spatial.nhdplus_tdec add column illicitconn_bnmpn_yr numeric(12,2);

update spatial.nhdplus_tdec a 
set septic_bnmpn_yr = b.septic_bnmpn_yr,
	petwaste_bnmpn_yr = b.petwaste_bnmpn_yr,
	illicitconn_bnmpn_yr = b.illicitconn_bnmpn_yr
from (
	select comid, 
	(coalesce(n_septic_systems,0) * 2.514 * 70.0 * 0.0000138 * 10000000.0) * 0.2 * (0.5 * (1 - 0.0483) * 0.002 + 1.0 * 0.0483 *0.13 ) as septic_bnmpn_yr,
	-- Pet Waste Load
	(((coalesce(acs_du,0.0) * 0.4) * (1 - 0.0) * 0.05) + ((coalesce(acs_du,0.0) * 0.4) * 0.0 * 1.0)) * 0.5 * 0.4 * 0.32 * 10.0 * 365.0 as petwaste_bnmpn_yr,
	-- Illicit Connections Load
	(2.514 * 70.0 * 10000000.0 * (coalesce(acs_du,0.0) * 0.001)) * 0.0000138 as illicitconn_bnmpn_yr

	from spatial.nhdplus_tdec
	order by septic_bnmpn_yr desc
) as b
where a.comid = b.comid
;

alter table bridges."bridges.nhdxuvmlc" rename to nhdxuvmlc

---------------------------------------------------------------
---------------------------------------------------------------
-- Watershed Water Volume and ACRES OF URBAN
alter table spatial.nhdplus_tdec add column urban_tot_ac numeric(12,2);
alter table spatial.nhdplus_tdec add column urban_imp_ac numeric(12,2);
alter table spatial.nhdplus_tdec add column urban_turf_ac numeric(12,2);

update spatial.nhdplus_tdec a
set urban_tot_ac = b.urban_tot_ac,
	urban_imp_ac = b.urban_imp_ac,
	urban_turf_ac = b.urban_turf_ac
from (
	select comid
	, urban_tot_ac
	, urban_imp_ac
	, case
		when urban_tot_ac > 0.0 and (urban_imp_ac + urban_turf_ac) > urban_tot_ac then urban_tot_ac - urban_imp_ac
		else urban_turf_ac end as urban_turf_ac
	from (
		select a.comid, 
		(b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86 as urban_tot_ac,

		case
			when ((c.histo_7 + c.histo_8 + c.histo_9 + c.histo_10 + c.histo_11 + c.histo_12) / 4046.86) > (b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86
				then (b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86
			else ((c.histo_7 + c.histo_8 + c.histo_9 + c.histo_10 + c.histo_11 + c.histo_12) / 4046.86)
			end as urban_imp_ac,

		-- coefficients in metadata for fractional turf use classes
		(d.histo_9 + d.histo_11*0.7 + d.histo_12*0.5 + d.histo_13*0.3 + d.histo_15) / 4046.86 as urban_turf_ac
		from spatial.nhdplus_tdec as a
		left join bridges.nhdxnlcd2016 as b
		on a.comid = b.comid
		left join bridges.nhdxuvmlc as c
		on a.comid = c.comid
		left join bridges.nhdxuvmlu as d
		on a.comid = d.comid
	) as t1
) as b
where a.comid = b.comid
;

alter table spatial.nhdplus_tdec add column watershed_wqv_cf numeric(12,2);

update spatial.nhdplus_tdec a set watershed_wqv_cf = b.watershed_wqv_cf
from (
	select comid, 
	1.0 * ( urban_tot_ac * 0.950 + urban_turf_ac * 0.202) * 3630 as watershed_wqv_cf
	from spatial.nhdplus_tdec
	) as b
where a.comid = b.comid

---------------------------------------------------------------
-- ADD IN TOTAL ACRES OF AG, FOREST and WATER
alter table spatial.nhdplus_tdec add column ag_tot_ac numeric(12,2);
alter table spatial.nhdplus_tdec add column nat_tot_ac numeric(12,2);
alter table spatial.nhdplus_tdec add column water_tot_ac numeric(12,2);

update spatial.nhdplus_tdec a
set ag_tot_ac = b.ag_tot_ac,
	nat_tot_ac = b.nat_tot_ac,
	water_tot_ac = b.water_tot_ac
from (
	select a.comid,
	(b.histo_13*0.1 + b.histo_16) / 4046.86 as ag_tot_ac,
	(b.histo_5 + b.histo_6 + b.histo_7 + b.histo_8 + b.histo_10 + b.histo_11*0.3 + b.histo_12*0.5 + b.histo_13*0.6) / 4046.86 as nat_tot_ac,
	(b.histo_4) / 4046.86 as water_tot_ac
	from spatial.nhdplus_tdec as a
	left join bridges.nhdxuvmlu as b
	on a.comid = b.comid
) as b
where a.comid = b.comid
;

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------------------------
-- GET THE URBAN, AG, NATURAL, AND WATER LOADS. USED MEDIAN FC EMC FOR DEVELOPED CLASSES (7772.5). THIS ALL SEEMS RATHER SUBJECTIVE...
alter table spatial.nhdplus_tdec add column precip_inyr numeric(7,4);

alter table spatial.nhdplus_tdec add column runoff_urb_inyr numeric(7,4);
alter table spatial.nhdplus_tdec add column runoff_urb_acftyr numeric(8,4);
alter table spatial.nhdplus_tdec add column urbanloading_bnmpn_acyr numeric(8,4);
alter table spatial.nhdplus_tdec add column urbanload_bnmpn_yr numeric(12,4);

alter table spatial.nhdplus_tdec add column runoff_ag_inyr numeric(7,4);
alter table spatial.nhdplus_tdec add column runoff_ag_acftyr numeric(8,4);
alter table spatial.nhdplus_tdec add column agloading_bnmpn_acyr numeric(8,4);
alter table spatial.nhdplus_tdec add column agload_bnmpn_yr numeric(12,4);

alter table spatial.nhdplus_tdec add column runoff_nat_inyr numeric(7,4);
alter table spatial.nhdplus_tdec add column runoff_nat_acftyr numeric(8,4);
alter table spatial.nhdplus_tdec add column natloading_bnmpn_acyr numeric(8,4);
alter table spatial.nhdplus_tdec add column natload_bnmpn_yr numeric(12,4);

alter table spatial.nhdplus_tdec add column runoff_water_inyr numeric(7,4);
alter table spatial.nhdplus_tdec add column runoff_water_acftyr numeric(8,4);
alter table spatial.nhdplus_tdec add column waterloading_bnmpn_acyr numeric(8,4);
alter table spatial.nhdplus_tdec add column waterload_bnmpn_yr numeric(12,4);

update spatial.nhdplus_tdec a
set precip_inyr = b.precip_inyr,
	runoff_urb_inyr = b.runoff_urb_inyr,
	runoff_urb_acftyr = b.runoff_urb_acftyr,
	urbanloading_bnmpn_acyr = b.urbanloading_bnmpn_acyr,
	urbanload_bnmpn_yr = b.urbanload_bnmpn_yr,

	runoff_ag_inyr = b.runoff_ag_inyr,
	runoff_ag_acftyr = b.runoff_ag_acftyr,
	agloading_bnmpn_acyr = b.agloading_bnmpn_acyr,
	agload_bnmpn_yr = b.agload_bnmpn_yr,

	runoff_nat_inyr = b.runoff_nat_inyr,
	runoff_nat_acftyr = b.runoff_nat_acftyr,
	natloading_bnmpn_acyr = b.natloading_bnmpn_acyr,
	natload_bnmpn_yr = b.natload_bnmpn_yr,

	runoff_water_inyr = b.runoff_water_inyr,
	runoff_water_acftyr = b.runoff_water_acftyr,
	waterloading_bnmpn_acyr = b.waterloading_bnmpn_acyr,
	waterload_bnmpn_yr = b.waterload_bnmpn_yr

from (
	select comid, 
	urban_tot_ac, 
	precip_inyr, 

	runoff_urb_inyr,
	runoff_urb_inyr*urban_tot_ac/12.0 as runoff_urb_acftyr,
	0.00103 * runoff_urb_inyr * 7772.5 as urbanloading_bnmpn_acyr,
	(0.00103 * runoff_urb_inyr * 7772.5) * urban_tot_ac as urbanload_bnmpn_yr,

	precip_inyr * 0.90 * 0.033 as runoff_ag_inyr,
	(precip_inyr * 0.90 * 0.033) * ag_tot_ac / 12.0 runoff_ag_acftyr,
	39.0 as agloading_bnmpn_acyr,
	39.0 * ag_tot_ac as agload_bnmpn_yr,

	precip_inyr * 0.90 * 0.033 as runoff_nat_inyr,
	(precip_inyr * 0.90 * 0.033) * nat_tot_ac / 12.0 runoff_nat_acftyr,
	12.0 as natloading_bnmpn_acyr,
	12.0 * nat_tot_ac as natload_bnmpn_yr,

	precip_inyr * 0.90 * 1.0 as runoff_water_inyr,
	(precip_inyr * 0.90 * 1.0) * water_tot_ac / 12.0 runoff_water_acftyr,
	0.0 as waterloading_bnmpn_acyr,
	0.0 * water_tot_ac as waterload_bnmpn_yr


	from (
		select comid, 
		urban_tot_ac, 
		ag_tot_ac,
		nat_tot_ac,
		water_tot_ac,
		(precipmm_ma/25.4) as precip_inyr,
		case 
			when urban_tot_ac = 0.0 then 0.0
			else ((urban_imp_ac/urban_tot_ac * 0.950) + (urban_turf_ac/urban_tot_ac * 0.202) + ((urban_tot_ac - (urban_imp_ac+urban_turf_ac))/urban_tot_ac * 0.033)) * 0.9 * (precipmm_ma/25.4) 
			end as runoff_urb_inyr
		from spatial.nhdplus_tdec
	) as t1
) as b
where a.comid = b.comid
;


--------------------------------------------------------------------------------
-- FINAL NHD TABLE!!!
drop table if exists dnrec.nhdplus_tdec_bacterialoading;
create table dnrec.nhdplus_tdec_bacterialoading
as

select comid, nord, nordstop	
,acs_pop			
,acs_du			
,n_septic_systems	
------------------------	
,septic_bnmpn_yr			
,petwaste_bnmpn_yr			
,illicitconn_bnmpn_yr
------------------------
,urban_tot_ac			
,urban_imp_ac			
,urban_turf_ac
,ag_tot_ac			
,nat_tot_ac			
,water_tot_ac

,watershed_wqv_cf

,precipmm_ma as precip_mmyr
,precip_inyr

,runoff_urb_inyr	
,runoff_ag_inyr
,runoff_nat_inyr
,(runoff_urb_inyr + runoff_ag_inyr + runoff_nat_inyr) as runoff_tot_inyr

		
,runoff_urb_acftyr
,runoff_ag_acftyr
,runoff_nat_acftyr
,(runoff_urb_acftyr + runoff_ag_acftyr + runoff_nat_acftyr) as runoff_tot_acftyr

		
,urbanloading_bnmpn_acyr
,agloading_bnmpn_acyr
,natloading_bnmpn_acyr

-----------------------------------------------------------------------------
,urbanload_bnmpn_yr
,agload_bnmpn_yr
,natload_bnmpn_yr
-----------------------------------------------------------------------------
-- TOTALS
,(urbanload_bnmpn_yr + agload_bnmpn_yr + natload_bnmpn_yr) as primaryload_bnmpn_yr
,(septic_bnmpn_yr + petwaste_bnmpn_yr + illicitconn_bnmpn_yr) as secondaryload_bnmpn_yr

,(urbanload_bnmpn_yr + agload_bnmpn_yr + natload_bnmpn_yr + septic_bnmpn_yr + petwaste_bnmpn_yr + illicitconn_bnmpn_yr) as totalload_bnmpn_yr
,catchment as geom
--, countyfp10
from spatial.nhdplus_tdec as a
--left join (select * from spatial.census_county /*where countyfp10 != '029' and countyfp10 != '045'*/) as b
--on st_intersects(st_centroid(a.geom),b.geom)
--where countyfp10 is null or countyfp10 not like '029'
where (urban_imp_ac			
+urban_turf_ac
+ag_tot_ac			
+nat_tot_ac			
+water_tot_ac) > 0.0

;

alter table dnrec.nhdplus_tdec_bacterialoading add constraint pk_nhdplus_tdec_bacterialoading primary key(comid);
create index nhdplus_tdec_bacterialoading_geom_idx
on dnrec.nhdplus_tdec_bacterialoading
using gist(geom);

select * from dnrec.nhdplus_tdec_bacterialoading;

---------------------------------------------------------------------------------------------------
-- WATERSHED LOADS

create table dnrec.nhdplus_tdec_bacterialoading_ws
as

select t1.*, t2.geom
from (
	select idx.nord, 
	sum(urbanload_bnmpn_yr) as urbanload_bnmpn_yr,
	sum(agload_bnmpn_yr) as agload_bnmpn_yr,
	sum(natload_bnmpn_yr) as natload_bnmpn_yr
	--geom
	from dnrec.nhdplus_tdec_bacterialoading as a
	join (select nord, nordstop from dnrec.nhdplus_tdec_bacterialoading) as idx
	on a.nord between idx.nord and idx.nordstop
	group by idx.nord
) as t1
left join dnrec.nhdplus_tdec_bacterialoading as t2
on t1.nord = t2.nord
;



















