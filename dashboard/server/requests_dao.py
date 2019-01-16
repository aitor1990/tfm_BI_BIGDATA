from pydrill.client import PyDrill
from jinja2 import Template
from time import sleep
from queries import *

TOURISM_FACTS_TABLE = "tourism_facts"
LABOUR_FACTS_TABLE = "labour_facts"

DEFAULT_TABLE = TOURISM_FACTS_TABLE

wait_for_drill = True
while wait_for_drill:
    try:
        drill = PyDrill(host='drill', port=8047)
        while not drill.is_active():
            sleep(5)
            print("waiting for drill")
        print("drill ready")
        wait_for_drill = False
    except:
        sleep(5)
        print("waiting for drill")

MAXIMUN_LINES_BAR_CHART = 10
FILTER_LINES_BAR_CHART = 3
#drill = PyDrill(host='drill', port=8047)
def getFactByCountryName(factName, minYear, maxYear, countryName='', cityNames=[], aggOperation='avg', numberRows=1000, table=DEFAULT_TABLE):
    if not countryName or countryName == 'all':
        return getFactByCountries(factName, minYear, maxYear, aggOperation, numberRows, table)
    else:
        return getFactByCountry(factName, minYear, maxYear, countryName, cityNames, aggOperation, numberRows, table)


def getFactByCountry(factName, minYear, maxYear, countryName, cityNames=[], aggOperation='avg', numberRows=1000, table=DEFAULT_TABLE):
         if len(cityNames) == 0:
             query = Template(fact_by_specific_country)\
                .render(country_name=countryName, operation=aggOperation, fact=factName,
                        min_year=minYear, max_year=maxYear, number_rows=numberRows, table=table)
         else:
             query = Template(fact_by_selected_cities)\
                .render(city_names=parseListToSqlList(cityNames), operation=aggOperation, fact=factName,
                        min_year=minYear, max_year=maxYear, number_rows=numberRows, table=table)
         results_query = drill.query(query)
         cities_name = []
         facts = []
         for result in results_query:
             if 'dimension' in result and 'fact' in result:
                 cities_name += [result['dimension']]
                 facts += [result['fact']]
         return {'dimension':  cities_name, 'fact': facts}


def getAggFactByCountry(factName, minYear, maxYear, countryName, aggOperation='avg', numberRows=1000, table=DEFAULT_TABLE):
    query = Template(countryList)\
            .render(country_name=countryName, operation=aggOperation, fact=factName,
                    min_year=minYear, max_year=maxYear, number_rows=numberRows, table=table)
    result_query = drill.query(query)
    for result in result_query:
        if 'dimension' in result and 'country_map_code' in result:
            return {'dimension': [result['dimension']], 'dimension_aux': [result['country_map_code']], 'fact': [40]}
    return {'dimension': [], 'dimension_aux': [], 'fact': []}


def getFactByCountries(factName, minYear, maxYear, aggOperation='avg', numberRows=1000, table=DEFAULT_TABLE):
     query = Template(fact_by_countries)\
            .render(operation=aggOperation, fact=factName, min_year=minYear,
                    max_year=maxYear, number_rows=numberRows, table=table)
     results_query = drill.query(query)
     countries_name = []
     countries = []
     facts = []
     for result in results_query:
         if 'dimension' in result and 'fact' in result and 'country_map_code' in result:
             countries_name += [result['dimension']]
             countries += [result['country_map_code']]
             facts += [result['fact']]
     return {'dimension': countries_name, 'dimension_aux': countries, 'fact': facts}


def getFactByCountriesEvolution(factName, minYear, maxYear, countryName, cityNames=[], aggOperation='avg', numberRows=1000, table=DEFAULT_TABLE):
     if countryName == '':
         query = Template(fact_evolution_by_country)\
             .render(operation=aggOperation, fact=factName, min_year=minYear,
                     max_year=maxYear,  table=table)
     elif len(cityNames) == 0:
         query = Template(fact_evolution_by_city)\
             .render(operation=aggOperation, fact=factName, min_year=minYear,
                     max_year=maxYear, number_rows=numberRows, table=table, country_name=countryName)
     else:
         query = Template(fact_evolution_by_selected_cities)\
             .render(operation=aggOperation, fact=factName, min_year=minYear,
                     max_year=maxYear, table=table, city_names=parseListToSqlList(cityNames))

     results_query = drill.query(query)
     response = {}
     for result in results_query:
         if 'dimension' in result:
             country = result['dimension']
             if result['dimension'] in response:
               response[country]['facts'] += [result['fact']]
               response[country]['years'] += [result['year']]
             else:
               response[country] = {'facts': [result['fact']], 'years': [result['year']]}
     #Cleaning lines with low statistic cases
     if len(cityNames) == 0 and len(response) > MAXIMUN_LINES_BAR_CHART:
         for key in list(response.keys()):
             if len(response[key]['years']) < (int(maxYear) - int(minYear))/FILTER_LINES_BAR_CHART :
                 del response[key]
     return response


def getDimensionValues(dimension, table='city_dimension'):
   query = Template(dimension_values).render(dimension=dimension, table=table)
   result_query = drill.query(query)
   response = [{'label': 'all', 'value': ''}]
   for result in result_query:
       if 'dimension' in result:
           response += [{'label': result['dimension'],'value': result['dimension']}]
   return response


def getDimensionValuesList(dimension, table='city_dimension'):
   query = Template(dimension_values).render(dimension=dimension, table=table)
   result_query = drill.query(query)
   response = []
   for result in result_query:
       if 'dimension' in result:
           response += [result['dimension']]
   return response


def getDimensionValuesListYear(dimension, table='city_dimension'):
      query = Template(dimension_values_year).render(
          dimension=dimension, table=table)
      result_query = drill.query(query)
      response = []
      for result in result_query:
          if 'dimension' in result:
            response += [result['dimension']]
      return response


def getCitiesByCountry(country):
      query = Template(cities_by_country).render(country_name=country)
      result_query = drill.query(query)
      response = []
      for result in result_query:
         if 'city_name' in result:
             response += [result['city_name']]
      return response


def getTableFromTopic(group):
    if group == 'tourism':
        return TOURISM_FACTS_TABLE
    else:
        return LABOUR_FACTS_TABLE


def parseListToSqlList(list):
     stringList = [''' '{0}' '''.format(element) for element in list]
     return '('+','.join(stringList)+')'
