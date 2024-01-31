SELECT dx_store_attr.dx_id as dx_id, dx_store_attr.dx_id_src, dx_store_attr.tdlinx_id as tdlinx_id, 
  try_cast(dx_store_attr.btg_retailer_store_nbr as int) as btg_store_nbr, dx_store_attr.btg_store_nm as btg_store_nm, 
  dx_store_attr.btg_retailer as btg_retailer, dx_store_attr.btg_address as btg_address, dx_store_attr.btg_city as btg_city, 
  dx_store_attr.btg_state as btg_state, dx_store_attr.btg_zip5 as btg_zip5, dx_store_attr.latitude as latitude, 
  dx_store_attr.longitude as longitude
FROM "DP_SDNA_US"."HOUSEHOLD_RELEASE"."DX_STORE_ATTR" dx_store_attr
WHERE
  {}
  and btg_zip5 is not null --dxid: 999EYYO | albertsons issue