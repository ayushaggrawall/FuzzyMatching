import json
#zauba_companies
#Andaman and Nicobar Islands_
with open('/home/Ayush/Downloads/zauba_companies.json') as f:
    x=json.load(f)

x=x[:500]
with open('short.json', 'w') as json_file:
    json.dump(x, json_file)
