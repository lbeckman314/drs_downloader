import requests
import json
import re
import aiohttp
import asyncio

class DRSClient:
	def __init__(self, api_url_base, access_id=None, public=False, debug=False):
		self.api_url_base = api_url_base
		self.access_id = access_id
		self.debug = None #debug
		self.public = public
		self.authorized = False

	def getHeaders(self):
		return {'Authorization' : 'Bearer {0}'.format(self.access_token) }

	def get_access_url(self, object_id, access_id=None):
		if access_id == None:
			access_id = self.access_id
		
		if not self.public:
			headers = self.getHeaders()
		else:
			headers ={}

		responses = asyncio.run(main(object_id,headers))
		print(responses)
		return responses


async def get_more(session,url):
   # try:
	async with session.get(url) as response:
		resp = await response.json(content_type=None)
		print(resp['url'])
		return resp['url']

   #except Exception as e:
    #    print("Unable to get url {} due to {} and {}.".format(url, e.__class__,e))


async def main(urls,headers):
    connector = aiohttp.TCPConnector(limit=10)
    session_timeout =   aiohttp.ClientTimeout(total=150,sock_connect=100,sock_read=100)
    async with aiohttp.ClientSession(headers=headers,connector=connector) as session:
        for url in urls:
            ret = await asyncio.gather(*[get_more(session,url)])
        return ret 




#for res in object_id:
			#response = requests.get(res, headers=headers)
			#if response.status_code == 200:
				#resp = response.content.decode('utf-8')
				#resp = json.loads(resp)['url']
				#responses.append(resp)
				#print(response)
#
			#if response.status_code == 401:
				#print('Unauthorized for that DRS id')
				#continue
			#else:
				#print("DRS id failed to be signed in some other way")
				#continue