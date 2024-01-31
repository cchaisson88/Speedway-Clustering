--RETAILER STORES
with stores as
(
  SELECT dx_store_attr.dx_id as dx_id, dx_store_attr.btg_zip5 as btg_zip5
  FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."DX_STORE_ATTR" dx_store_attr
  WHERE {account_condition}
),

--HOUSEHOLDS BY STORE
store_hh as
(
  SELECT stores.dx_id as dx_id, stores.btg_zip5 as btg_zip5,
  COUNT(DISTINCT mvh_hh_all_store.cv_living_unit_id) as households
  FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_HH_ALL_STORE" mvh_hh_all_store
  RIGHT JOIN stores
    ON mvh_hh_all_store.dx_id = stores.dx_id
  WHERE mvh_hh_all_store.is_avbl_decile >= 8 
  GROUP BY stores.dx_id, stores.btg_zip5
  HAVING COUNT(DISTINCT mvh_hh_all_store.cv_living_unit_id) >= 100
),

--HOUSEHOLDS BY ZIP5
zip5_hh as
(
  SELECT mvh_data_store.cv_zipcode_5 as btg_zip5, COUNT(DISTINCT mvh_data_store.cv_living_unit_id) as households
  FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
  WHERE {postal_retailer_decile} >= 8
  AND mvh_data_store.cv_zipcode_5 IN
      (
          select distinct btg_zip5
          from stores
          where dx_id not in (select distinct dx_id from store_hh)
      )
  GROUP BY mvh_data_store.cv_zipcode_5
  HAVING COUNT(DISTINCT mvh_data_store.cv_living_unit_id) >= 100
)

--RETAILER STORE XREF DESIGNATING THE TYPE OF DATA PULL NEEDED
select dx_id, btg_zip5 as zip5, SUBSTR(btg_zip5, 1, 3) as zip3, lod
from
(
select distinct dx_id, btg_zip5, 'dx_id' as lod
from store_hh

union

select distinct dx_id, btg_zip5, 'zip5' as lod
from stores
where dx_id not in (select distinct dx_id from store_hh)
and btg_zip5 in (select distinct btg_zip5 from zip5_hh)

union

select distinct dx_id, btg_zip5, 'zip3' as lod from stores
where dx_id not in (select distinct dx_id from store_hh)
and btg_zip5 not in (select distinct btg_zip5 from zip5_hh)
)