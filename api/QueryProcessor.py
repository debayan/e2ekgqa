import sys,os,json,requests,re
import itertools
from elasticsearch import Elasticsearch
import time


class QueryProcessor:
	def __init__(self):
		self.es = Elasticsearch(host='ltcpu1',port=45198)
		self.prefixes = {'wdt:': 'http://www.wikidata.org/prop/direct/', 'p:' : 'http://www.wikidata.org/prop/', 'ps:': 'http://www.wikidata.org/prop/statement/', 'pq:': 'http://www.wikidata.org/prop/qualifier/'}

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

	def addprefix(self, queryarr):
		prefixed_query = []
		for token in queryarr:
			if  re.match('^[q0-9_]+$',token): #entity
				prefixed_query.append(['wd:'+token.upper()])
				continue
			elif re.match('^[p0-9_]+$',token): #prop
				props = []
				for k,v in self.prefixes.items():
					prop = k+token.upper()
					props.append(prop)
				prefixed_query.append(props)
					
			else:
				prefixed_query.append([token])
		print(prefixed_query)
		prefixed_queries = list(itertools.product(*prefixed_query))
		return prefixed_queries

	def findresults(self,prefixed_queries):
		result_queries = []
		for query in prefixed_queries:
			time.sleep(0.5)
			query = ' '.join(list(query))
			result = self.sparqlendpoint(query)
			if 'error' in result:
				result_queries.append({"query":query,"result":result,"type":"error"})
				continue
			if 'results' in result:
				if 'bindings' in result['results']:
					if len(result['results']['bindings']) > 0:
						result_queries.append({"query":query,"result":result['results']['bindings'],"type":"normal"})
						return result_queries
			if 'boolean' in result:
				result_queries.append({"query":query,"result":result['boolean'],"type":"bool"})
				return result_queries
		return result_queries	
			
		 
	def fetchanswer(self, queryarr):
		url = 'https://query.wikidata.org/sparql'
		prefixed_queries = self.addprefix(queryarr)
		result_queries = self.findresults(prefixed_queries)
		return result_queries


q = QueryProcessor()

#print(q.fetchanswer(["select","?vr0","where","{","q310903","p69","?vr1",".","?vr1","p131","?vr0","}"]))  # list of ents
#print(q.fetchanswer(['SELECT', '(', 'COUNT', '(', '?var0', ')', 'AS', '?value', ')', '{', 'q338430', 'p2563', '?var0', '}']))  #count
#print(q.fetchanswer(['SELECT', '?value1', '?value2', 'WHERE', '{', 'q11806', 'p40', '?s', '.', '?s', 'p40', 'q4667661', '.', '?s', 'p569', '?value1', '.', '?s', 'p25', '?value2', '}'])) #dual intent

		
