{% set property_types = get_column_values_alphabetically(
  table='source_data.listings',
  column='property_type',
  max_records=10)
%}

{{ log(property_types, info=True) }}

with listings as (
  select * from source_data.listings
)

select
  zipcode,
  {%- for property_type in property_types %}
    {% set property_type_lower = property_type | lower %}
    sum(case when lower(property_type)='{{ property_type_lower}}' then 1 else 0 end)
      as {{ property_type_lower | replace(' ', '_')| replace('/','_') }}_count {{- "," if not loop.last -}}
  {%- endfor %}

from listings

group by 1
