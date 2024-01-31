--DEMO HOUSEHOLDS BY DX_ID
SELECT dx_id, demos, description, sum(households) as households, 'dx_id' as lod
FROM
(
SELECT mvh_hh_all_store.dx_id as dx_id,
mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME,
COUNT(DISTINCT mvh_data_store.cv_living_unit_id) as households
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_DATA_STORE" mvh_data_store
JOIN "DP_SDNA_US"."HOUSEHOLD_RELEASE"."MVH_HH_ALL_STORE" mvh_hh_all_store
    ON mvh_data_store.cv_living_unit_id = mvh_hh_all_store.cv_living_unit_id
WHERE mvh_hh_all_store.is_avbl_decile >= 8
AND mvh_hh_all_store.dx_id IN {dx_ids}
GROUP BY mvh_hh_all_store.dx_id, mvh_data_store.HH_SIZE, mvh_data_store.HOH_AGE, mvh_data_store.HOH_LIFESTAGE, mvh_data_store.HOH_ETHNICITY, mvh_data_store.HH_INCOME
)
unpivot(description for demos in (HH_SIZE, HOH_AGE, HOH_LIFESTAGE, HOH_ETHNICITY, HH_INCOME))
group by dx_id, demos, description