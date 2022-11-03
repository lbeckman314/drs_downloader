import click
import requests
import json
import async_downloader.download as async_downloader
from pathlib import Path
import os
from DRSClient import DRSClient
import pandas as pd
import aiohttp
import asyncio
import time
import logging 
import subprocess
from tqdm import tqdm
import collections
"""
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
"""
    
def seperate_conflicts(numbers,urls,md5s_and_sizes):
    #add duplicate indexes to new array
    name_problems = []
    for i in range(len(numbers)):                
        name_problems. append([urls[numbers[i]],
        md5s_and_sizes[numbers[i]][0],md5s_and_sizes[numbers[i]][1]])

    for index in sorted(numbers, reverse=True):
        del urls[index]
        del md5s_and_sizes[index]
    

    return name_problems, urls, md5s_and_sizes

def duplicate_names(names):
    l1= []
    numbers = []
    names_2 = []
    count =0 
    for i in names:
        if i not in l1:
            l1.append(i)
        else:  
            names_2.append(i[0])
            numbers.append(count)
        count= count +1
    
    if(len(names_2)> 0):
        return names_2,numbers
    else:
        return False

def Extract_TSV_Information(Tsv_Path,duplicateflag):
    urls = []
    md5s_and_sizes = []
    names = [] 
    df = pd.read_csv(Tsv_Path,sep = '\t')
    if('pfb:ga4gh_drs_uri' in df.columns.values.tolist()):
        for i in range(df['pfb:ga4gh_drs_uri'].count()):
            urls.append(df['pfb:ga4gh_drs_uri'][i])
            md5s_and_sizes.append([df['pfb:file_md5sum'][i],df['pfb:file_size'][i]])
            names.append([df['pfb:file_name'][i]])
    elif('ga4gh_drs_uri' in df.columns.values.tolist()):
        for i in range(df['ga4gh_drs_uri'].count()):
            urls.append(df['ga4gh_drs_uri'][i])
            md5s_and_sizes.append([df['file_md5sum'][i],df['file_size'][i]])
            names.append([df['file_name'][i]])
    else:
        raise KeyError("Key format is bad do either pfb:ga4gh_drs_uri or ga4gh_drs_uri")

    
    if (duplicate_names(names) != False and (duplicateflag=="FILE" or duplicateflag=="NAME")):
        name_problems, numbers =duplicate_names(names)

    elif(duplicateflag == "NONE" and duplicate_names(names) != False):
        name_problems, numbers =duplicate_names(names)
        raise Exception("Files sharing the same name have been found and duplicate flag has not been specified.  \
         The Files that are effected are: ", set(name_problems), " Reconfigure 'DUPLICATE' flag to 'NAME' or 'FILE' to resolve this error")
    else:
        name_problems= False
        numbers= False

    return urls,md5s_and_sizes,numbers

async def get_more(session,uri,url_endpoint):
    data = '{ "url": "'+uri+'", "fields": ["fileName", "size", "hashes", "accessUrl"]}'
    async with session.post(url=url_endpoint,data=data) as response:
        resp = await response.json(content_type=None)
        return resp['accessUrl']['url']

async def get_signed_uris(uris):
    conn = aiohttp.TCPConnector(limit=None)
    url_endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
    token = subprocess.check_output(["gcloud" ,"auth" ,"print-access-token"]).decode("ascii")[0:-1]
    header = {'authorization': 'Bearer '+token, 'content-type': 'application/json'}
    async with aiohttp.ClientSession(headers=header ,connector= conn) as session:
        ret = await asyncio.gather(*[get_more(session,uri,url_endpoint) for uri in tqdm(uris)])
        return ret 


    #duplicateflag="FILE"
    #tsvname= 'smol.tsv'
    #outputfile = "/Users/peterkor/Desktop/terra-implementation/DATA"
    #Download_Files(tsvname,outputfile,duplicateflag)

@click.command()
@click.option('--duplicateflag', default="NONE", show_default=True, help='The first number of lines to display.')
@click.option('--tsvname', default=None, show_default=True, help='The last last number of lines to display.')
@click.option('--outputfile', default=None, show_default=True, help='The number of lines to skip before displaying.')
def Download_Files(duplicateflag,tsvname,outputfile):
    drs_ids, md5s_and_sizes,numbers= Extract_TSV_Information(tsvname,duplicateflag)
    print("Signing URIS")
    urls = asyncio.run(get_signed_uris(drs_ids))

    if (numbers != False and (duplicateflag=="FILE" or duplicateflag=="NAME")):
        name_problems_catalogued,urls,md5s_and_sizes = seperate_conflicts(numbers,urls,md5s_and_sizes)

    print("Downloading URLS")
    download_obj = []
    name_problem_download_obj= [] 

    for url in urls:
        download_url_bundle = async_downloader.DownloadURL(url,md5s_and_sizes[urls.index(url)][0],md5s_and_sizes[urls.index(url)][1])
        download_obj.append(download_url_bundle)

    if(numbers != False):
        for name_problems in name_problems_catalogued:
            download_url_bundle = async_downloader.DownloadURL(name_problems[0],name_problems[1],name_problems[2])
            name_problem_download_obj.append(download_url_bundle)

    if(not Path(str(outputfile)).exists() and not os.path.exists(outputfile)):
        print("The path that you specified did not exist so the directory was made in your current working directory")
        os.mkdir(str(outputfile))

    if(numbers == False):
        async_downloader.download(download_obj, Path(str(outputfile)),duplicateflag)
        return

    elif(numbers != False and duplicateflag == "NAME"):
        duplicateflag="NONE"
        async_downloader.download(download_obj, Path(str(outputfile)),duplicateflag)
        duplicateflag="NAME"
        async_downloader.download(name_problem_download_obj, Path(str(outputfile)),duplicateflag)
        return

    elif(numbers != False and duplicateflag  == "FILE"):
        async_downloader.download(download_obj, Path(str(outputfile)),Name_Flag="FILE")
        return

    raise Exception("How did we get here?")

if __name__ == '__main__':
    Download_Files()

    


