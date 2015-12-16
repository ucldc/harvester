import requests
from harvester.queue_harvest import main as queue_harvest
c_prod=[]
c_harvest=[]
url_reg = "https://registry.cdlib.org"
url_reg_api = '{}{}'.format(url_reg, "/api/v1/collection/")
url='{}{}'.format(url_reg_api, "?format=json&limit=1000")
resp=requests.get(url)
api=resp.json()
nextpage=api['meta']['next']
print "NEXTPAGE:{}".format(nextpage)
while nextpage:
    for o in api['objects']:
        if o['ready_for_publication']:
            c_prod.append(o)
            url_api_collection = '{}{}/'.format(url_reg_api, o['id'])
            print url_api_collection
            queue_harvest('mredar@gmail.com', url_api_collection,
                    rq_queue='normal-prod')
        if o['url_harvest']:
            c_harvest.append(o)
    resp = requests.get(''.join(('https://registry.cdlib.org', nextpage)))
    api = resp.json()
    nextpage=api['meta']['next']
    print "NEXTPAGE:{}".format(nextpage)

print "READY FOR PUB:{}".format(len(c_prod))
print "READY FOR HARVEST:{}".format(len(c_harvest))
