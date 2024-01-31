SELECT zip, demos, description, sum(households) as households, lod
FROM
(
SELECT mvh_data_store.cv_zipcode_5 as zip,
mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME,
COUNT(DISTINCT mvh_data_store.cv_living_unit_id) as households, 'zip5' as lod
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
WHERE {postal_retailer_decile} >= 8 
AND mvh_data_store.cv_zipcode_5 IN {zip5s}
GROUP BY mvh_data_store.cv_zipcode_5, mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME

union

SELECT SUBSTR(mvh_data_store.cv_zipcode_5, 1, 3) as zip,
mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME,
COUNT(DISTINCT mvh_data_store.cv_living_unit_id) as households, 'zip3' as lod
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
WHERE {postal_retailer_decile} >= 8
AND SUBSTR(mvh_data_store.cv_zipcode_5, 1, 3) IN {zip3s}
GROUP BY SUBSTR(mvh_data_store.cv_zipcode_5, 1, 3),
mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME
)
unpivot(description for demos in (HH_SIZE, HOH_AGE, HOH_LIFESTAGE, HOH_ETHNICITY, HH_INCOME))
group by zip, demos, description, lod