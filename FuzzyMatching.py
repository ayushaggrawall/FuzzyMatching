from fuzzywuzzy import fuzz
import json
import sys
from elasticsearch import Elasticsearch, helpers

#Load json file (smaller size)
with open('/home/Ayush/Downloads/Andaman and Nicobar Islands_.json') as f:
    y=json.load(f)

#Uploading to ElasticSearch (large size json)
'''with open("/home/Ayush/Downloads/zauba_companies.json",'r') as fp1:
    docs1 = json.load(fp1)
for i in docs1:
    del i["_id"]
new={}
def function_1():
    for i in docs1:
        new['_id']=i['cin']
        new['_index']="gstin"
        new['_type']="_doc"
        new['_op_type']="index"
        new["_source"] = i
        yield new
dict_of_list = json.dumps(list(function_1()))
es = Elasticsearch(['localhost'], port=9200 )
res=helpers.bulk(Elasticsearch(['localhost'], port=9200 ),function_1())'''

es = Elasticsearch(['localhost'], port=9200 )

# Fuzzy Search from ES (Zauba)
def elastic_name(name):
    return es.search(index='gstin',body={
  "query": {
    "match": {                                             
      "name.keyword": {                                          
        "query": name,
        "fuzziness": "AUTO",
        "operator":  "and"
         
      }
      
    }                                              
  } 
})

# Combining gst and cin
elastic_match=[]
score=0
flag=0
for name in y:
    data = elastic_name(name['Taxpayers Name'])
    score=data['hits']['max_score']
    gst_fetch=name['GSTIN']
    if((len(data['hits']['hits']))!=0):
        #elastic_match.append(data['hits']['hits'])
        for ix,value in enumerate(data['hits']['hits']):
            if(value['_score']==score):
                #print('dat score',score)
                    #collection.insert(data['hits']['hits'])
                elastic_match.append(data['hits']['hits'][ix])
                #elastic_match.append()
                data['hits']['hits'][ix]['_source'].update( {'GSTIN' : gst_fetch} )

                '''flag=flag+1
                if(flag==200):
                    break
    if(flag==200):
        break'''
                
    
#MongoDB
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['mat2']
collection = db['mat2']


flag=0
for j in elastic_match:
    elastic_match[flag]['_source'].update( {'_score' : j['_score']} )
    flag=flag+1
    #collection.insert(j['_source'])

# Pushing to MongoDB
for j in elastic_match:
    collection.insert(j['_source'])

# Aggregation
flag=0
dupli=[]

#for duplicate scores
for i in db.mat2.aggregate([  
    {"$group": {"_id": {"GSTIN":"$GSTIN"}, "count": {"$sum": 1}, "name" : {"$push" : "$name"}, "cin" : {"$push" : "$cin"}}
           },
    {"$match": {
        "count" : {"$gt" : 2}}
    },
    {"$project" : {"name":1,"cin":1}}
]):
    flag=flag+1
    dupli.append(i)




#for  unique scores
flag=0
final=[]
for i in db.mat2.aggregate([  
    {"$group": {"_id": {"GSTIN":"$GSTIN"}, "count": {"$sum": 1}, "name" : {"$push" : "$name"}, "cin" : {"$push" : "$cin"}}
           },
    {"$match": {
        "count" : {"$eq" : 1}}
    },
    {"$project" : {"name":1,"cin":1}}
]):
    flag=flag+1
    final.append(i)
temp=final
d=[]
flag=0
#adding to new mongodb(aggregated)
for i in final:
    temp[flag]['_id'].update({'name': i['name'][0]})
    temp[flag]['_id'].update({'cin': i['cin'][0]})
    flag=flag+1
client = MongoClient('localhost', 27017)
db1 = client['mat2agg']
collection1 = db1['mat2agg']
for j in temp:
    collection1.insert(j['_id'])

#for campanies with same name but different cin
flag=0
di={}
for i in dupli:
    for j in i['name']:
        if not j in di:
            di[j] = 1
        else:
            di[j] += 1
d=[]
for i in di:
    if(di.get(i)>1):
                d.append(i)

x=[]
for i in d:
    for j in db.mat2.find():
        if(i==j['name']):
            collection1.insert(j)

        
