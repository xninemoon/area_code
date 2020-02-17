import io
import json
import re
import sys
import time
import urllib.request
import requests
import sqlite3

from functools import reduce

from lxml import etree

#改变标准输出的默认编码
sys.stdout=io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')

def readFile(path, code):
    str = ''
    file = open(path, 'r+', encoding=code)
    for line in file.readlines():
        str += line
    return str

def getHttpPage2(url, code):
    # proxy_addr = 'localhost:9999'
    headers = {
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.5383.400 QQBrowser/10.0.1313.400',
            #    'Cookie': 'AD_RS_COOKIE=20082856; _trs_uv=jy9gbrtk_6_4q7t; __utma=207252561.1466905274.1563526197.1563526197.1563526197.1; __utmc=207252561; __utmz=207252561.1563526197.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; __utmb=207252561.2.10.1563526197; wzws_cid=f568b29f15242ce981d1d8ba9929c3f2ddc7fa786357ce107b0552fc7923c2fbeac7094ec97b2c9483dfc7c3a439d679d424d049b291f85e242cc7b4dfb951c3'
               }
    
    # proxy = urllib.request.ProxyHandler({'http': proxy_addr})
    # opener = urllib.request.build_opener(proxy, urllib.request.HTTPHandler)
    # urllib.request.install_opener(opener)

    request = urllib.request.Request(url, headers=headers)
    reponse = urllib.request.urlopen(request, timeout=10).read()
    html = reponse.decode(code) # 获取页面信息，转码
    return html

def getCode(href):
    p = re.search(r'/([\d]+)\.html$',href, re.M|re.I)
    return p.group(1)

def getCityCode(html,  type, isIndex):
    page = etree.HTML(html)
    datas = page.xpath('//tr[@class="'+type+'"]/td/a')
    hrefs = page.xpath('//tr[@class="'+type+'"]/td/a/@href')
    items = []
    print(">> datas.length: ", len(datas))
    print(">> hrefs.length: ", len(hrefs))
    index = 0
    while index < len(datas):
        if isIndex:
            item = {
                "code": '',
                "name": datas[index].text,
                "href": None if hrefs is None or len(hrefs) == 0 else hrefs[index]
            }
            item["code"] = getCode(item["href"])
        else:
            item = {
                "code": datas[index].text,
                "name": datas[index+1].text,
                "href": None if hrefs is None or len(hrefs) == 0 else hrefs[index]
            }
            index+=1

        index+=1
        items.append(item)
    return items
############################### database ######################################################

class DBManager:
    __conn = sqlite3.connect("ac.db")
    __c = __conn.cursor()

    def addToDB(self, list,  parent, level,  desc):
        for item in list:
            self.__c.execute(
            '''
            insert into codes (name, code, parent, level) values ('%s', '%s', '%s', '%d' )
            '''%(item["name"], item["code"], parent, level)
            )
            self.__conn.commit()
        print("Add success for "+ desc)

    def getParentList(self, level):
        rows = self.__c.execute("select * from codes where level = '%d'"%(level))
        plist = []
        for row in rows:
            item = {
                "code":row[0],
                "name":row[1],
                "href":''
            }
            plist.append(item)
        return plist

    def deleteStart(self, parent):
        self.__c.execute("delete from codes where parent = '%s'"%(parent))
        self.__conn.commit()

    def close(self):
        self.__conn.close()

    



###########################################################################################



def getProvinces(dbm):
    print("Starting...")
    html = readFile("index.txt", 'UTF-8')
    provinces = getCityCode(html, "provincetr", True)
    dbm.addToDB(provinces,"0",1,"省份")

def get2L(dbm, provinces, startAt):
    
    index = 0
    for province in provinces:
        index+=1
        if index < startAt:
            continue

        dbm.deleteStart(province["code"]) # 删除,避免重复

        print("+++++++++ Area[", index, "] : ", province["name"], " - ", province["code"], " ++++++++++++++++++++")
        url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/'+province["code"]+".html"
        html = getHttpPage2(url, "gb2312")
        citys = getCityCode(html,"citytr", False)

        dbm.addToDB(citys, province["code"], 2, province["name"])

def get3L(dbm, citylist, startAt):
    index = 0
    html = ''
    try:
        for city in citylist:
            index+=1
            if index < startAt:
                continue
            dbm.deleteStart(city["code"]) # 删除,避免重复
            
            url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/'+city["code"][0:2]+"/"+city["code"][0:4]+".html"
            print("+++++++++ Area【", index, "】:", city["name"], "-", city["code"], " >>> ", url)
            html = getHttpPage2(url, "gbk")
            citys = getCityCode(html,"countytr", False)
            dbm.addToDB(citys, city["code"], 3, city["name"])
            # time.sleep(1000)
        index = -1; ## 完成退出
    except Exception as identifier:
        print(identifier)
    finally:
        return index

def get4L(dbm, countyList, startAt):
    index = 0
    html = ''
    try:
        for county in countyList:
            index+=1
            if index < startAt:
                continue
            dbm.deleteStart(county["code"]) # 删除,避免重复  
            url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/'+county["code"][0:2]+"/"+county["code"][2:4]+"/"+county["code"][0:6]+".html"
            print("+++++++++ Area【", index, "】:", county["name"], "-", county["code"], " >>> ", url)
            html = getHttpPage2(url, "gbk")
            citys = getCityCode(html,"towntr", False)
            dbm.addToDB(citys, county["code"], 4, county["name"])
            # time.sleep(1000)
        index = -1; ## 完成退出
    except Exception as identifier:
        print(identifier)
    finally:
        return index
    


print("Start .... ")
dbm = DBManager()

# getProvinces(dbm)

plist = dbm.getParentList(3)

iex = 2614
times = 0
maxFailTimes = 20
while iex != -1 and times < maxFailTimes:
    iex = get4L(dbm, plist, iex)
    times += 1


dbm.close()
print("End .... ")