select
	t.*,
    coalesce(lead(t.load_ts) over(partition by t.hk_customer order by t.load_ts) - interval 1 milliseconds, to_timestamp('2099-12-31 23:59:59.999', 'yyyy-MM-dd HH:mm:ss.SSS')) as loadend_ts,
    coalesce(cdGender.cod_text, t.cod_gender) as txt_gender_en,
    coalesce(cdLanguage.cod_text, t.cod_language) as txt_language_en
from {{ ref('s_customer') }} t
left outer join {{ ref('vls_codedefinition') }} cdGender
	on  cdGender.cog_group = 1
	and cdGender.cod_value = t.cod_gender
	and cdGender.cod_language = 10
left outer join {{ ref('vls_codedefinition') }} cdLanguage
	on  cdLanguage.cog_group = 2
	and cdLanguage.cod_value = t.cod_language
	and cdLanguage.cod_language = 10