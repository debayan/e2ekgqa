import sys,os,json,requests,re
import itertools
import time


class QueryProcessor:
	def __init__(self):
		pass

	def sparqlendpoint(self, query):
		url = 'http://ltcpu3:8890/sparql'
		r = requests.get(url, params = {'format': 'json', 'query': query})
		print(r)
		try:
			data = r.json()
			print(type(data))
			return data
		except Exception as err:
			print(err)
			return {"error":repr(err), "errorcode":r.status_code}

	def findresults(self, query_arr):
		print("querry_arr:",query_arr)
		result_queries = []
		queries = []
		result = {}
		query = ' '.join(query_arr)
		query = query.replace(' wd: ',' wd:').replace(' p: ',' p:').replace(' wdt: ',' wdt:').replace(' ps: ' , ' ps:').replace(' pq: ',' pq:')
		query_ = query.replace(' wd:q',' wd:Q').replace(' p:p',' p:P').replace(' wdt:p',' wdt:P').replace(' ps:p' , ' ps:P').replace(' pq:p',' pq:P').replace(", \' ",", \'").replace(" \' )","\' )").replace(" en","en")
		query = '''PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wds: <http://www.wikidata.org/entity/statement/> PREFIX wdv: <http://www.wikidata.org/value/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX wikibase: <http://wikiba.se/ontology#> PREFIX p: <http://www.wikidata.org/prop/> PREFIX ps: <http://www.wikidata.org/prop/statement/> PREFIX pq: <http://www.wikidata.org/prop/qualifier/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ''' + query_
		print("QUERY:",query)
		queries.append(query_)
		result = self.sparqlendpoint(query)
		return query_,result
#		if 'error' in result:
#			result_queries.append({"query":query_,"result":result,"type":"error"})
#			return queries,result_queries
#		if 'results' in result:
#			if 'bindings' in result['results']:
#				if len(result['results']['bindings']) > 0:
#					result_queries.append({"query":query_,"result":result['results']['bindings'],"type":"normal"})
#					return queries,result_queries
#		if 'boolean' in result:
#			result_queries.append({"query":query_,"result":result['boolean'],"type":"bool"})
#			return queries,result_queries
#		return queries,result_queries	
			
		 
	def fetchanswer(self, queryarr):
		queries = []
		results = []
		for query in queryarr:
			query_,r = self.findresults(query[0])
			queries.append(query_)
			results.append(r)
		return queries,results


q = QueryProcessor()

if __name__ == '__main__':
    print(q.fetchanswer([(['select', 'distinct', '?answer', 'where', '{', 'wd:', 'q32491', 'wdt:', 'p3362', '?answer}'],-1)]))
