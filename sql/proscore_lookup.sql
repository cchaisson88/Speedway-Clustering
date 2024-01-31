select mvs_name, mvs_desc, group_name
from "DP_SDNA_US"."DX_TEAM_PROD"."PROSCORE_XREF"
where group_name = '{}'
and mvs_name not like '%_decile'
and mvs_desc not like '%protien%'
and group_name <> 'characteristic'