import sys,os,json,requests,re
import itertools
import time


class QueryProcessor:
	def __init__(self):
		pass

	def sparqlendpoint(self, query):
		url = 'https://query.wikidata.org/sparql'
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
		result_queries = []
		queries = []
		result = {}
		query = ' '.join(query_arr)
		query = query.replace(' wd: ',' wd:').replace(' p: ',' p:').replace(' wdt: ',' wdt:').replace(' ps: ' , ' ps:').replace(' pq: ',' pq:')
		query = query.replace(' wd:q',' wd:Q').replace(' p:p',' p:P').replace(' wdt:p',' wdt:P').replace(' ps:p' , ' ps:P').replace(' pq:p',' pq:P').replace(", \' ",", \'").replace(" \' )","\' )").replace("\' en \'","\'en\'")
		queries.append(query)
		#result = self.sparqlendpoint(query)
		if 'error' in result:
			result_queries.append({"query":query,"result":result,"type":"error"})
			return queries,result_queries
		if 'results' in result:
			if 'bindings' in result['results']:
				if len(result['results']['bindings']) > 0:
					result_queries.append({"query":query,"result":result['results']['bindings'],"type":"normal"})
					return queries,result_queries
		if 'boolean' in result:
			result_queries.append({"query":query,"result":result['boolean'],"type":"bool"})
			return queries,result_queries
		return queries,result_queries	
			
		 
	def fetchanswer(self, queryarr):
		url = 'https://query.wikidata.org/sparql'
		queries,results = self.findresults(queryarr)
		return queries,results


q = QueryProcessor()

#print(q.fetchanswer(["select","?vr0","where","{","q310903","p69","?vr1",".","?vr1","p131","?vr0","}"]))  # list of ents
#print(q.fetchanswer(['SELECT', '(', 'COUNT', '(', '?var0', ')', 'AS', '?value', ')', '{', 'q338430', 'p2563', '?var0', '}']))  #count
#print(q.fetchanswer(['SELECT', '?value1', '?value2', 'WHERE', '{', 'q11806', 'p40', '?s', '.', '?s', 'p40', 'q4667661', '.', '?s', 'p569', '?value1', '.', '?s', 'p25', '?value2', '}'])) #dual intent

		
