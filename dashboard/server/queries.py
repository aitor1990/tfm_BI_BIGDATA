
fact_by_specific_country= '''
select {{ operation }}(b.{{ fact }}) as fact ,a.city_name as dimension
from dfs.`/data/city_dimension.parquet` a , dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.city_name
order by fact desc
limit {{ number_rows }}
'''

fact_by_countries= '''
select {{ operation }}(b.{{ fact }}) as fact, a.country_name as dimension, a.country_map_code
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.country_name, a.country_map_code
order by fact desc
limit {{ number_rows }}
'''

fact_agg_by_country= '''
select {{ operation }}(b.{{ fact }}) as fact, a.country_name as dimension, a.country_map_code
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.country_name, a.country_map_code
order by fact desc
limit {{ number_rows }}
'''
fact_by_selected_cities= '''
select {{ operation }}(b.{{ fact }}) as fact, a.city_name as dimension
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city and a.city_name in {{ city_names }}
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.city_name
order by fact desc
limit {{ number_rows }}'''

fact_evolution_by_country = '''
select {{ operation }}(b.{{ fact }}) as fact, a.country_name as dimension,
        a.country_map_code, b.`year`
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.country_name, a.country_map_code,b.`year`
order by b.`year` asc
limit {{ number_rows }}
'''

fact_evolution_by_city = '''
select {{ operation }}(b.{{ fact }}) as fact, a.city_name as dimension, b.`year`
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.city_name, b.`year`
order by b.`year` asc
limit {{ number_rows }}
'''

fact_evolution_by_selected_cities = '''
select {{ operation }}(b.{{ fact }}) as fact, a.city_name as dimension, b.`year`
from dfs.`/data/city_dimension.parquet` a, dfs.`/data/{{ table }}.parquet` b
where a.index_city = b.index_city and  a.city_name in {{ city_names }}
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.city_name, b.`year`
order by b.`year` asc
limit {{ number_rows }}
'''


dimension_values = 'select distinct `{{ dimension }}` as dimension from dfs.`/data/{{ table }}.parquet` a order by `{{ dimension }}` asc'
dimension_values_year = 'select distinct `year` as dimension from dfs.`/data/{{ table }}.parquet` a where `year` >= 2010 order by `year` asc'
country_code_request = '''select country_map_code from dfs.`/data/city_dimension.parquet` where country_name = '{{country_name}}' '''
cities_by_country = '''select city_name from dfs.`/data/city_dimension.parquet` where country_name = '{{ country_name }}' '''
