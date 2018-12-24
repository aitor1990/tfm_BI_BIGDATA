from pydrill.client import PyDrill
from jinja2 import Template

fact_by_specific_country= '''
select {{ operation }}(b.{{ fact }}) as fact ,a.city_name as dimension
from dfs.`/data/city_dimension.parquet` a ,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.city_name
order by fact desc
limit {{ number_rows }}
'''

fact_by_countries= '''
select {{ operation }}(b.{{ fact }}) as fact, a.country_name as dimension, a.country_map_code
from dfs.`/data/city_dimension.parquet` a,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.country_name, a.country_map_code
order by fact desc
limit {{ number_rows }}
'''

fact_agg_by_country= '''
select {{ operation }}(b.{{ fact }}) as fact, a.country_name as dimension, a.country_map_code
from dfs.`/data/city_dimension.parquet` a,dfs.`/data/tourism_facts.parquet` b
where a.index_city = b.index_city and a.country_name = '{{ country_name }}'
and b.`year` >= {{ min_year }} and b.`year` <= {{ max_year }}
group by a.country_name, a.country_map_code
order by fact desc
limit {{ number_rows }}
'''

dimension_values = 'select distinct `{{ dimension }}` as dimension from dfs.`/data/{{ table }}.parquet` a order by `{{ dimension }}` asc'

country_code_request = 'select country_map_code from dfs.`/data/city_dimension.parquet` where country_name = {{country_name}}'

drill = PyDrill(host='drill', port=8047)
if not drill.is_active():
    raise ImproperlyConfigured('Please run Drill first')

def getFactByCountryName(factName,minYear,maxYear,countryName = '',aggOperation='avg',numberRows=1000 ):
    if not countryName or countryName == 'all':
        return getFactByCountries(factName,minYear,maxYear,aggOperation,numberRows)
    else:
        return getFactByCountry(factName,minYear,maxYear,countryName,aggOperation,numberRows,)

def getFactByCountry(factName,minYear,maxYear,countryName,aggOperation='avg',numberRows=1000 ):
         query = Template(fact_by_specific_country).render(country_name = countryName,operation = aggOperation,fact = factName, min_year = minYear ,max_year = maxYear,number_rows = numberRows)
         results_query = drill.query(query)
         cities_name = []
         facts = []
         for result in results_query:
             cities_name += [result['dimension']]
             facts += [result['fact']]
         return {'dimension' :  cities_name, 'fact': facts}


def getAggFactByCountry(factName,minYear,maxYear,countryName,aggOperation='avg',numberRows=1000 ):
    query = Template(fact_agg_by_country).render(country_name = countryName,operation = aggOperation,fact = factName,min_year  = minYear ,max_year  = maxYear,number_rows = numberRows)
    result_query = drill.query(query)
    for result in result_query:
         print(result)
         return {'dimension' : [result['dimension']], 'dimension_aux' : [result['country_map_code']],'fact':[result['fact']]}
    return {}

def getFactByCountries(factName,minYear,maxYear,aggOperation='avg',numberRows=1000):
     query = Template(fact_by_countries).render(operation = aggOperation,fact = factName,min_year  = minYear ,max_year  = maxYear,number_rows = numberRows)
     results_query = drill.query(query)
     countries_name = []
     countries = []
     facts = []
     for result in results_query:
         countries_name += [result['dimension']]
         countries += [result['country_map_code']]
         facts += [result['fact']]
     return {'dimension' : countries_name, 'dimension_aux':countries, 'fact':facts}

def getDimensionValues(dimension,table = 'city_dimension'):
   query = Template(dimension_values).render(dimension = dimension,table = table)
   result_query =  drill.query(query)
   response = [{'label': 'all', 'value': ''}]
   for result in result_query:
       response += [{'label': result['dimension'], 'value': result['dimension']}]
   return response

def getDimensionValuesList(dimension,table = 'city_dimension'):
   query = Template(dimension_values).render(dimension = dimension,table = table)
   result_query =  drill.query(query)
   response = []
   for result in result_query:
       response += [result['dimension']]
   return response
