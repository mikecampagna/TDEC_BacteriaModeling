﻿--
select * from dnrec.flow_watershed_final;


-- CREATE THE HUC12 TDEC Table
drop table if exists spatial.huc12_tdec;
create table spatial.huc12_tdec
as
select distinct a.huc12
,a.geom
,a.gid
,a.objectid
,a.tnmid
,a.metasource
,a.sourcedata
,a.sourceorig
,a.sourcefeat
,a.loaddate
,a.gnis_id
,a.areaacres
,a.areasqkm
,a.states
,a.name
,a.hutype
,a.humod
,a.tohuc
,a.noncontrib
,a.noncontr_1
,a.globalid
,a.shape_leng
,a.shape_area

,avg(b.precipmm_ma) over (partition by a.huc12) as precipmm_ma

from spatial.wbdhuc12_de as a

left join spatial.nhdplus_tdec as b
on st_intersects(a.geom, st_centroid(b.catchment))

order by huc12;

alter table spatial.huc12_tdec add constraint pk_huc12_tdec primary key (huc12);

create index huc12_tdec_geom_idx on spatial.huc12_tdec using gist(geom);

select * from spatial.huc12_tdec;

-- 84 HUC12s

-----------------------------


select distinct huc12, sum(huc12_du) over (partition by huc12) as huc12_du
from (
	select id, huc12, int_area/bg_area as p_bg, (int_area/bg_area) * tot_du as huc12_du, tot_du
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.huc12
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.geom) as huc12_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.geom)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.geom)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.huc12_tdec as d
		on st_intersects(a.geom, d.geom)
		order by a.id, d.huc12
	) t1
	order by huc12
) as t2
;


select * from spatial.huc12_tdec limit 1000;

select * from spatial.wbdhuc12_de;

-- This works for NHDplus catchments, but not really for HUC12s. I never use the column though and I would rather just include it for now.
alter table spatial.huc12_tdec add column acs_bg_id bigint;
update spatial.huc12_tdec a set acs_bg_id = b.id
from spatial.acs_2018_bg as b
where st_intersects(st_centroid(a.geom), b.geom);

alter table spatial.huc12_tdec drop column acs_pop;
alter table spatial.huc12_tdec drop column acs_du;

alter table spatial.huc12_tdec add column acs_pop int;
alter table spatial.huc12_tdec add column acs_du int;

-- UPDATE POPULATION
update spatial.huc12_tdec a set acs_pop = b.huc12_pop::int
from (
select distinct huc12, sum(huc12_pop) over (partition by huc12) as huc12_pop
from (
	select id, huc12, int_area/bg_area as p_bg, (int_area/bg_area) * tot_pop as huc12_pop, tot_pop
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.huc12
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.geom) as huc12_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.geom)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.geom)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.huc12_tdec as d
		on st_intersects(a.geom, d.geom)
		order by a.id, d.huc12
	) t1
	order by huc12
) as t2
) as b
where a.huc12 = b.huc12;

-- UPDATE DWELLING UNITS
update spatial.huc12_tdec a set acs_du = b.huc12_du::int
from (
select distinct huc12, sum(huc12_du) over (partition by huc12) as huc12_du
from (
	select id, huc12, int_area/bg_area as p_bg, (int_area/bg_area) * tot_du as huc12_du, tot_du
	from (
		select a.id
		,a."OBJECTID" as objid
		,d.huc12
		,b.B25002e2 as tot_du
		,c.b01001e1 as tot_pop
		,st_area(a.geom) as bg_area
		,st_area(d.geom) as huc12_area
		,st_area(st_makevalid(st_multi(st_intersection(a.geom, d.geom)))) as int_area
		,st_makevalid(st_multi(st_intersection(a.geom, d.geom)))::geometry(multipolygon,32618) as geom_int
		from spatial.acs_2018_bg as a
		left join spatial.acs_2018_bg_housing as b
		on a."OBJECTID" = b.objectid
		left join spatial.acs_2018_bg_totpop as c
		on a."OBJECTID" = c.objectid
		left join spatial.huc12_tdec as d
		on st_intersects(a.geom, d.geom)
		order by a.id, d.huc12
	) t1
	order by huc12
) as t2
) as b
where a.huc12 = b.huc12
;

-- SEPTIC LOAD
-- avg person per du is the number of bedrooms, 120 gallons of septic per bedroom, 21% of DU are unsewered

alter table spatial.huc12_tdec add column septic_galyear numeric(12,2);
update spatial.huc12_tdec a set septic_galyear = b.septic_gal_yr
from (
	select t1.huc12, septic_gal_yr + coalesce(septic_gal_yr_pt,0) as septic_gal_yr
	from (
		select huc12, (acs_pop::numeric / acs_du::numeric * 0.21) * 120.0 * 365.25 as septic_gal_yr
		from spatial.huc12_tdec
		where acs_du > 0.0
	) as t1
	left join (
		-- SHOULD WE TREAT COMMERCIAL vs COMMUNITY systems differently?
		select b.huc12, 
			case when systemclass = 'Community' then (a.flow_galday * 365.25)* 1.0
			else a.flow_galday * 365.25
			end as septic_gal_yr_pt
		from dnrec.septicsystems as a
		join spatial.huc12_tdec as b
		on st_intersects(a.geom, b.geom)
	) as t2
	on t1.huc12 = t2.huc12
	order by t1.huc12
) as b
where a.huc12 = b.huc12
;

select * from spatial.huc12_tdec where septic_galyear < 0

-- NUMBER OF SYSTEMS == NUMBER OF UNSEWERED DWELLING UNITS
alter table spatial.huc12_tdec add column n_septic_systems bigint;
update spatial.huc12_tdec a set n_septic_systems = b.n_septic_systems
from (
	select t1.huc12, t1.n_septic_systems + coalesce(t2.n_septic_systems_pt,0) as n_septic_systems
	from (
		select huc12, (acs_du::numeric * 0.21)::int as n_septic_systems
		from spatial.huc12_tdec
		where acs_du > 0.0
		) as t1
	left join (
		-- SHOULD WE TREAT COMMERCIAL vs COMMUNITY systems differently?
		select b.huc12, count(a.facid) over (partition by b.huc12) as n_septic_systems_pt
		from dnrec.septicsystems as a
		join spatial.huc12_tdec as b
		on st_intersects(a.geom, b.geom)
	) as t2
	on t1.huc12 = t2.huc12
	order by t1.huc12
) as b
where a.huc12 = b.huc12
;

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------
-- GET OSDS SEPTIC LOAD, bnMPN/yr
-- Untreated Sewage Delivered to Septics (billions) = Unsewered dwelling units * Avg. Person/DU * Water Use (GPCD) * constant * FC (MPN/100ml)
-- Bacteria (bn/yr) = (untreated sewage delivered to septics * failure rates * normal delivery ratio * % septics not near waterway * normal bacteria decay %) + (delivery ratio adjacent to waterway * % septics near waterway * bacteria decay adjacent to waterway)

alter table spatial.huc12_tdec add column septic_bnmpn_yr numeric(12,2);
alter table spatial.huc12_tdec add column petwaste_bnmpn_yr numeric(12,2);
alter table spatial.huc12_tdec add column illicitconn_bnmpn_yr numeric(12,2);

update spatial.huc12_tdec a 
set septic_bnmpn_yr = b.septic_bnmpn_yr,
	petwaste_bnmpn_yr = b.petwaste_bnmpn_yr,
	illicitconn_bnmpn_yr = b.illicitconn_bnmpn_yr
from (
	select huc12, 
	(coalesce(n_septic_systems,0) * 2.514 * 70.0 * 0.0000138 * 10000000.0) * 0.2 * (0.5 * (1 - 0.0483) * 0.002 + 1.0 * 0.0483 *0.13 ) as septic_bnmpn_yr,
	-- Pet Waste Load
	(((coalesce(acs_du,0.0) * 0.4) * (1 - 0.0) * 0.05) + ((coalesce(acs_du,0.0) * 0.4) * 0.0 * 1.0)) * 0.5 * 0.4 * 0.32 * 10.0 * 365.0 as petwaste_bnmpn_yr,
	-- Illicit Connections Load
	(2.514 * 70.0 * 10000000.0 * (coalesce(acs_du,0.0) * 0.001)) * 0.0000138 as illicitconn_bnmpn_yr

	from spatial.huc12_tdec
	order by septic_bnmpn_yr desc
) as b
where a.huc12 = b.huc12
;

alter table bridges."bridges.huc12xuvmlc" rename to huc12xuvmlc;

select * from spatial.huc12_tdec;

---------------------------------------------------------------
---------------------------------------------------------------
-- Watershed Water Volume and ACRES OF URBAN
alter table spatial.huc12_tdec add column urban_tot_ac numeric(12,2);
alter table spatial.huc12_tdec add column urban_imp_ac numeric(12,2);
alter table spatial.huc12_tdec add column urban_turf_ac numeric(12,2);

update spatial.huc12_tdec a
set urban_tot_ac = b.urban_tot_ac,
	urban_imp_ac = b.urban_imp_ac,
	urban_turf_ac = b.urban_turf_ac
from (
	select huc12
	, urban_tot_ac
	, urban_imp_ac
	, case
		when urban_tot_ac > 0.0 and (urban_imp_ac + urban_turf_ac) > urban_tot_ac then urban_tot_ac - urban_imp_ac
		else urban_turf_ac end as urban_turf_ac
	from (
		select a.huc12, 
		--(b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86 as urban_tot_ac,
		(d.histo_1 + d.histo_2 + d.histo_3 + d.histo_9 + d.histo_11*0.7 + d.histo_12*0.5 + d.histo_13*0.3 + d.histo_14 + d.histo_15) / 4046.86 as urban_tot_ac,

		case
			when ((c.histo_7 + c.histo_8 + c.histo_9 + c.histo_10 + c.histo_11 + c.histo_12) / 4046.86) > (b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86
				then (b.histo_21 + b.histo_22 + b.histo_23 + b.histo_24)*900.0 / 4046.86
			else ((c.histo_7 + c.histo_8 + c.histo_9 + c.histo_10 + c.histo_11 + c.histo_12) / 4046.86)
			end as urban_imp_ac,

		-- coefficients in metadata for fractional turf use classes
		(d.histo_9 + d.histo_11*0.7 + d.histo_12*0.5 + d.histo_13*0.3 + d.histo_15) / 4046.86 as urban_turf_ac

		from spatial.huc12_tdec as a
		left join bridges.huc12xnlcd2016 as b
		on a.huc12 = b.huc12
		left join bridges.huc12xuvmlc as c
		on a.huc12 = c.huc12
		left join bridges.huc12xuvmlu as d
		on a.huc12 = d.huc12
	) as t1
) as b
where a.huc12 = b.huc12
;

alter table spatial.huc12_tdec add column watershed_wqv_cf numeric(12,2);

update spatial.huc12_tdec a set watershed_wqv_cf = b.watershed_wqv_cf
from (
	select huc12, 
	1.0 * ( urban_tot_ac * 0.950 + urban_turf_ac * 0.202) * 3630 as watershed_wqv_cf
	from spatial.huc12_tdec
	) as b
where a.huc12 = b.huc12
;

---------------------------------------------------------------
-- ADD IN TOTAL ACRES OF AG, FOREST and WATER
alter table spatial.huc12_tdec add column ag_tot_ac numeric(12,2);
alter table spatial.huc12_tdec add column nat_tot_ac numeric(12,2);
alter table spatial.huc12_tdec add column water_tot_ac numeric(12,2);

update spatial.huc12_tdec a
set ag_tot_ac = b.ag_tot_ac,
	nat_tot_ac = b.nat_tot_ac,
	water_tot_ac = b.water_tot_ac
from (
	select a.huc12,
	-- The dataset does not have pasture in DE?
	--(b.histo_13*0.1 + b.histo_16 + b.histo_17) / 4046.86 as ag_tot_ac,
	(b.histo_13*0.1 + b.histo_16) / 4046.86 as ag_tot_ac,
	(b.histo_5 + b.histo_6 + b.histo_7 + b.histo_8 + b.histo_10 + b.histo_11*0.3 + b.histo_12*0.5 + b.histo_13*0.6) / 4046.86 as nat_tot_ac,
	(b.histo_4) / 4046.86 as water_tot_ac
	from spatial.huc12_tdec as a
	left join bridges.huc12xuvmlu as b
	on a.huc12 = b.huc12
) as b
where a.huc12 = b.huc12
;

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------------------------
-- GET THE URBAN, AG, NATURAL, AND WATER LOADS. USED MEDIAN FC EMC FOR DEVELOPED CLASSES (7772.5). THIS ALL SEEMS RATHER SUBJECTIVE...

alter table spatial.huc12_tdec DROP column precip_inyr;

alter table spatial.huc12_tdec DROP column runoff_urb_inyr;
alter table spatial.huc12_tdec DROP column runoff_urb_acftyr;
alter table spatial.huc12_tdec DROP column urbanloading_bnmpn_acyr;
alter table spatial.huc12_tdec DROP column urbanload_bnmpn_yr;

alter table spatial.huc12_tdec DROP column runoff_ag_inyr;
alter table spatial.huc12_tdec DROP column runoff_ag_acftyr;
alter table spatial.huc12_tdec DROP column agloading_bnmpn_acyr;
alter table spatial.huc12_tdec DROP column agload_bnmpn_yr;

alter table spatial.huc12_tdec DROP column runoff_nat_inyr;
alter table spatial.huc12_tdec DROP column runoff_nat_acftyr;
alter table spatial.huc12_tdec DROP column natloading_bnmpn_acyr;
alter table spatial.huc12_tdec DROP column natload_bnmpn_yr;

alter table spatial.huc12_tdec DROP column runoff_water_inyr;
alter table spatial.huc12_tdec DROP column runoff_water_acftyr;
alter table spatial.huc12_tdec DROP column waterloading_bnmpn_acyr;
alter table spatial.huc12_tdec DROP column waterload_bnmpn_yr;

---

alter table spatial.huc12_tdec add column precip_inyr numeric(7,4);

alter table spatial.huc12_tdec add column runoff_urb_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_urb_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column urbanloading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column urbanload_bnmpn_yr numeric(14,4);

alter table spatial.huc12_tdec add column runoff_imp_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_imp_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column imploading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column impload_bnmpn_yr numeric(14,4);

alter table spatial.huc12_tdec add column runoff_turf_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_turf_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column turfloading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column turfload_bnmpn_yr numeric(14,4);

alter table spatial.huc12_tdec add column runoff_ag_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_ag_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column agloading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column agload_bnmpn_yr numeric(14,4);

alter table spatial.huc12_tdec add column runoff_nat_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_nat_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column natloading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column natload_bnmpn_yr numeric(14,4);

alter table spatial.huc12_tdec add column runoff_water_inyr numeric(7,4);
alter table spatial.huc12_tdec add column runoff_water_acftyr numeric(10,4);
alter table spatial.huc12_tdec add column waterloading_bnmpn_acyr numeric(10,4);
alter table spatial.huc12_tdec add column waterload_bnmpn_yr numeric(14,4);

update spatial.huc12_tdec a
set precip_inyr = b.precip_inyr,
	runoff_urb_inyr = b.runoff_urb_inyr,
	runoff_urb_acftyr = b.runoff_urb_acftyr,
	urbanloading_bnmpn_acyr = b.urbanloading_bnmpn_acyr,
	urbanload_bnmpn_yr = b.urbanload_bnmpn_yr,

	runoff_imp_inyr = b.runoff_imp_inyr,
	runoff_imp_acftyr = b.runoff_imp_acftyr,
	imploading_bnmpn_acyr = b.imploading_bnmpn_acyr,
	impload_bnmpn_yr = b.impload_bnmpn_yr,

	runoff_turf_inyr = b.runoff_turf_inyr,
	runoff_turf_acftyr = b.runoff_turf_acftyr,
	turfloading_bnmpn_acyr = b.turfloading_bnmpn_acyr,
	turfload_bnmpn_yr = b.turfload_bnmpn_yr,

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
	select huc12, 
	urban_tot_ac, 
	precip_inyr, 

	runoff_urb_inyr,
	runoff_urb_inyr*urban_tot_ac/12.0 as runoff_urb_acftyr,
	0.00103 * runoff_urb_inyr * 7772.5 as urbanloading_bnmpn_acyr,
	(0.00103 * runoff_urb_inyr * 7772.5) * urban_tot_ac as urbanload_bnmpn_yr,

	-- ADD IN IMPERVIOUS LOADING
	runoff_imp_inyr,
	runoff_imp_inyr*urban_tot_ac/12.0 as runoff_imp_acftyr,
	0.00103 * runoff_imp_inyr * 7772.5 as imploading_bnmpn_acyr,
	(0.00103 * runoff_imp_inyr * 7772.5) * urban_tot_ac as impload_bnmpn_yr,

	-- ADD IN TURF LOADING
	runoff_turf_inyr,
	runoff_turf_inyr*urban_tot_ac/12.0 as runoff_turf_acftyr,
	0.00103 * runoff_turf_inyr * 7772.5 as turfloading_bnmpn_acyr,
	(0.00103 * runoff_turf_inyr * 7772.5) * urban_tot_ac as turfload_bnmpn_yr,

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
		select huc12, 
		urban_tot_ac, 
		urban_imp_ac,
		urban_turf_ac,
		ag_tot_ac,
		nat_tot_ac,
		water_tot_ac,
		(precipmm_ma/25.4) as precip_inyr,
		case 
			when urban_tot_ac = 0.0 then 0.0
			when urban_tot_ac - (urban_imp_ac+urban_turf_ac) > 0.0
				then ((urban_imp_ac/urban_tot_ac * 0.950) + (urban_turf_ac/urban_tot_ac * 0.202) + ((urban_tot_ac - (urban_imp_ac+urban_turf_ac))/urban_tot_ac * 0.033)) * 0.9 * (precipmm_ma/25.4) 
			else ((urban_imp_ac/urban_tot_ac * 0.950) + (urban_turf_ac/urban_tot_ac * 0.202)) * 0.9 * (precipmm_ma/25.4)
			end as runoff_urb_inyr,

		-- ADD IN IMPERVIOUS RUNOFF
		case 
			when urban_tot_ac = 0.0 then 0.0
			else ((urban_imp_ac/urban_tot_ac * 0.950)) * 0.9 * (precipmm_ma/25.4) 
			end as runoff_imp_inyr,

		-- ADD IN TURF RUNOFF
		case 
			when urban_tot_ac = 0.0 then 0.0
			else ((urban_turf_ac/urban_tot_ac * 0.202)) * 0.9 * (precipmm_ma/25.4) 
			end as runoff_turf_inyr

		from spatial.huc12_tdec
	) as t1
) as b
where a.huc12 = b.huc12
;

select urban_tot_ac - (urban_imp_ac+urban_turf_ac) as test,
urban_tot_ac,
urban_imp_ac,
urban_turf_ac, 
acs_pop,
urbanload_bnmpn_yr,
impload_bnmpn_yr,
turfload_bnmpn_yr,
runoff_urb_inyr,
runoff_imp_inyr,
runoff_turf_inyr
from spatial.huc12_tdec


--------------------------------------------------------------------------------
-- FINAL huc12 TABLE!!!
drop table if exists dnrec.huc12_tdec_bacterialoading;
create table dnrec.huc12_tdec_bacterialoading
as

select huc12
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
,runoff_imp_inyr
,runoff_turf_inyr
,runoff_ag_inyr
,runoff_nat_inyr
,(runoff_urb_inyr + runoff_ag_inyr + runoff_nat_inyr) as runoff_tot_inyr

		
,runoff_urb_acftyr
,runoff_imp_acftyr
,runoff_turf_acftyr
,runoff_ag_acftyr
,runoff_nat_acftyr
,(runoff_urb_acftyr + runoff_ag_acftyr + runoff_nat_acftyr) as runoff_tot_acftyr

		
,urbanloading_bnmpn_acyr
,imploading_bnmpn_acyr
,turfloading_bnmpn_acyr
,agloading_bnmpn_acyr
,natloading_bnmpn_acyr

-----------------------------------------------------------------------------
,urbanload_bnmpn_yr
,impload_bnmpn_yr
,turfload_bnmpn_yr
,agload_bnmpn_yr
,natload_bnmpn_yr
-----------------------------------------------------------------------------
-- TOTALS
,(urbanload_bnmpn_yr + agload_bnmpn_yr + natload_bnmpn_yr) as primaryload_bnmpn_yr
,(septic_bnmpn_yr + petwaste_bnmpn_yr + illicitconn_bnmpn_yr) as secondaryload_bnmpn_yr

,(urbanload_bnmpn_yr + agload_bnmpn_yr + natload_bnmpn_yr + septic_bnmpn_yr + petwaste_bnmpn_yr + illicitconn_bnmpn_yr) as totalload_bnmpn_yr
,geom as geom
--, countyfp10
from spatial.huc12_tdec as a
--left join (select * from spatial.census_county /*where countyfp10 != '029' and countyfp10 != '045'*/) as b
--on st_intersects(st_centroid(a.geom),b.geom)
--where countyfp10 is null or countyfp10 not like '029'
where (urban_imp_ac			
+urban_turf_ac
+ag_tot_ac			
+nat_tot_ac			
+water_tot_ac) > 0.0

;

alter table dnrec.huc12_tdec_bacterialoading add constraint pk_huc12_tdec_bacterialoading primary key(huc12);
create index huc12_tdec_bacterialoading_geom_idx
on dnrec.huc12_tdec_bacterialoading
using gist(geom);

select * from dnrec.huc12_tdec_bacterialoading where "acs_pop" >= 0;

---------------------------------------------------------------------------------------------------
-- ONLY USE FOR NHDPLUS LAYER
-- WATERSHED LOADS

create table dnrec.huc12_tdec_bacterialoading_ws
as

select t1.*, t2.geom
from (
	select idx.nord, 
	sum(urbanload_bnmpn_yr) as urbanload_bnmpn_yr,
	sum(agload_bnmpn_yr) as agload_bnmpn_yr,
	sum(natload_bnmpn_yr) as natload_bnmpn_yr
	--geom
	from dnrec.huc12_tdec_bacterialoading as a
	join (select nord, nordstop from dnrec.huc12_tdec_bacterialoading) as idx
	on a.nord between idx.nord and idx.nordstop
	group by idx.nord
) as t1
left join dnrec.huc12_tdec_bacterialoading as t2
on t1.nord = t2.nord
;


















