SELECT dx_store_attr.dx_id as dx_id, 'dx_id' as lod, {proscore_string}
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_HH_ALL_STORE" mvh_hh_all_store
  RIGHT JOIN "DP_SDNA_US"."HOUSEHOLD_RELEASE"."DX_STORE_ATTR" dx_store_attr ON
    mvh_hh_all_store.dx_id = dx_store_attr.dx_id 
  JOIN "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store ON
    mvh_hh_all_store.cv_living_unit_id = mvh_data_store.cv_living_unit_id 
WHERE
  mvh_hh_all_store.dx_id IN {dx_ids}
  AND mvh_hh_all_store.is_avbl_decile >= 8 
GROUP BY dx_store_attr.dx_id