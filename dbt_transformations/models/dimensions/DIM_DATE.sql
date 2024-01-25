with stg_date as (
    select
        PRIOR_YEAR_OVER_YEAR_DATE_DAY,
        NEXT_DATE_DAY,
        DAY_OF_YEAR,
        DATE_DAY,
        DAY_OF_MONTH,
        PRIOR_DATE_DAY,
        DAY_OF_WEEK,
        DAY_OF_WEEK_NAME,
        PRIOR_YEAR_DATE_DAY
    from
        {{ source('FIVETRAN_SALES', 'STG_DATE') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['stg_date.date_day']) }} as date_key,
    *
from stg_date