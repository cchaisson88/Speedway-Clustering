SELECT mvh_data_store.cv_zipcode_5 as zip, 'zip5' as lod, {proscore_string}
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
WHERE
  {postal_retailer_decile} >= 8
  AND mvh_data_store.cv_zipcode_5 IN {zip5s} 
GROUP BY mvh_data_store.cv_zipcode_5

union

SELECT SUBSTR(mvh_data_store.cv_zipcode_5,1,3) as zip, 'zip3' as lod, {proscore_string}
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
WHERE
  {postal_retailer_decile} >= 8
  AND SUBSTR(mvh_data_store.cv_zipcode_5,1,3) IN {zip3s}
GROUP BY SUBSTR(mvh_data_store.cv_zipcode_5,1,3)