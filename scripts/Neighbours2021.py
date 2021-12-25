import sys,os,json,requests,re
import itertools
import time
import re


class Neighbours:
    def __init__(self):
        pass

    def sparqlendpoint(self, query):
        url = 'http://ltcpu3:8890/sparql'
        r = requests.get(url, params = {'format': 'json', 'query': query})
        try:
            data = r.json()
            return data
        except Exception as err:
            print(err)
            return {"error":repr(err), "errorcode":r.status_code}

    def process(self,result):
        props = []
        try:
            for item in result['results']['bindings']:
                prop = item['p']['value']
                extracted_prop = re.findall(r"^.*(P[\d].*)$",prop) 
                if extracted_prop:
                    props.append(extracted_prop[0])
            return props
        except Exception as err:
            print(err)
            return props

    def fetch_neighbours_relations(self, entid):
        #1 hop neighbours
        query = '''select distinct ?p where { <http://www.wikidata.org/entity/%s> ?p ?o}'''%(entid)
        onehopresult = self.sparqlendpoint(query)
        #print("onehop:",onehopresult)
        props1 = self.process(onehopresult)
        # 1.5 hop statemement relations
        #query = '''select distinct ?p where { <http://www.wikidata.org/entity/%s> ?p1 ?x . ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.wikidata.org/ontology#Statement> . ?x ?p ?o }'''%(entid)
        query = '''select distinct ?p where 
                   { <http://www.wikidata.org/entity/%s> ?p1 ?x .
                     ?x <http://wikiba.se/ontology#rank> ?y .
                     ?x ?p ?o
                   } 
                '''%(entid)
        onepointfivehopresult = self.sparqlendpoint(query)
        #print("onehalfhop:",onepointfivehopresult)
        props15 = self.process(onepointfivehopresult)
        return list(set(props1+props15))
        
if __name__ == '__main__':
    n = Neighbours()
    neighbours = n.fetch_neighbours_relations('Q76')
    print(neighbours)
