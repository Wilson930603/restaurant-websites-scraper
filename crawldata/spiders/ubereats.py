import scrapy,json,requests,cloudscraper,os,platform
from datetime import datetime
from urllib.parse import quote
from crawldata.functions import *
class CrawlerSpider(scrapy.Spider):
    name = 'ubereats'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Content-Type': 'application/json','x-csrf-token': 'x','Origin': 'https://www.ubereats.com','Alt-Used': 'www.ubereats.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
    json_data = {'feedSessionCount': {'announcementCount': 0,'announcementLabel': '',},'userQuery': '','date': '','startTime': 0,'endTime': 0,'carouselId': '','sortAndFilters': [],'marketingFeedType': '','billboardUuid': '','feedProvider': '','promotionUuid': '','targetingStoreTag': '','venueUUID': '','selectedSectionUUID': '','favorites': '','vertical': '','searchSource': '','keyName': '','pageInfo': {'offset': 0,'pageSize': 80,},}
    ZIPCODE=[
        'Oklahoma',
        'Oklahoma City'
        'Shawnee',# Oklahoma 
        "Austin",# Texas 
        "Dallas",# Texas" 
        "Tulsa",# Oklahoma" 
        "Little Rock",# Arkansas" 
        "Kansas City",# Missouri" 
        "Omaha",# Nebraska"
        "Indianapolis",# Indiana" 
        "Santa Fe", #New Mexico"
        "Baton Rouge",# Louisiana"
        "bethel"#, Oklahoma",
        "Tecumseh"#, Oklahoma",  
        ]
    scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    def start_requests(self):
        for ZIPCODE in self.ZIPCODE:
            json_data = {"query":ZIPCODE}
            url='https://www.ubereats.com/api/getLocationAutocompleteV1'
            response = requests.post(url, headers=self.headers, json=json_data)
            Data=json.loads(response.text)
            for row in Data['data']:
                if row["addressLine1"]==ZIPCODE and 'USA' in row['addressLine2']:
                # if ZIPCODE.lower() in row["addressLine1"].lower() and ZIPCODE.lower() in row['addressLine2']:
                    json_data = {'placeId': row['id'],'provider': 'google_places','source': 'manual_auto_complete'}
                    url='https://www.ubereats.com/api/getDeliveryLocationV1'
                    response = requests.post(url, headers=self.headers, json=json_data)
                    Data=json.loads(response.text)
                    if len(Data['data'])>0:
                        LOC=quote(str(json.dumps(Data['data'])))
                        cookies={}
                        cookies['uev2.loc']=LOC
                        yield scrapy.Request(self.URL,callback=self.parse_location,meta={'proxy':None,'cookies':cookies,'offset':0},dont_filter=True)
                    else:
                        print('Not found for zip=',self.ZIPCODE)
    def parse_location(self, response):
        cookies=response.meta['cookies']
        offset=response.meta['offset']
        url='https://www.ubereats.com/api/getFeedV1'
        json_data={}
        json_data.update(self.json_data)
        json_data['pageInfo']['offset']=offset
        response = requests.post(url, cookies=cookies, headers=self.headers, json=json_data)
        Data=json.loads(response.text)
        for rows in Data['data']['feedItems']:
            if 'store' in rows:
                row=rows['store']
                url='https://www.ubereats.com'+row['actionUrl']
                yield scrapy.Request(self.URL,callback=self.parse_content,meta={'URL':url,'Level':0,'ROW':rows},dont_filter=True)
                # break
        if Data['data']['meta']['hasMore']==True:
            yield scrapy.Request(self.URL,callback=self.parse_location,meta={'proxy':None,'cookies':cookies,'offset':Data['data']['meta']['offset']},dont_filter=True)
    def parse_content(self,response):
        meta = response.meta
        URL=response.meta['URL']
        Level=response.meta['Level']
        ROW=response.meta['ROW']
        HTML=self.scraper.get(URL)
        response=scrapy.Selector(text=HTML.text)
        txt=response.xpath('//main[@id="main-content"]/script/text()').get()
        try:
            row=json.loads(txt)
        except Exception as ex:
            print(ex)
            if meta.get('try_count'):
                if meta.get('try_count') == 2:
                    return
                meta['try_count'] = meta['try_count']+1
            else:
                meta['try_count'] = 1
            yield scrapy.Request(self.URL,callback=self.parse_content,meta=meta,dont_filter=True)
            return

        ITEM={}
        ITEM['SHEET']='restaurants'
        ITEM['provider']=self.name
        ITEM['uuid']=ROW['store']['storeUuid']
        ITEM['name']=ROW['store']['title']['text']
        ITEM['description'] = ''

        address=row['address']
        address_data = address['streetAddress']+', '+address['addressLocality']+', '+address['addressRegion']+' '+address['postalCode']+', '+address['addressCountry']
        address_data = address_data.replace('Address:','').strip()
        ITEM['address']=address_data

        try:
            ITEM['rating']=row['aggregateRating']['ratingValue']
        except:
            ITEM['rating']=''
        ITEM['scraping date']=self.DATE_CRAWL
        if 'Convenience' in row['servesCuisine'] or 'Retail' in row['servesCuisine']:
            print('Not a restaturnat')
            return
        ITEM['types']=row['servesCuisine']
        ITEM['url']=row['@id']
        TIME=[]
        ex_time = {}
        for rs in row['openingHoursSpecification']:
            if isinstance(rs['dayOfWeek'], list):
                for x in rs['dayOfWeek']:
                    if ex_time.get(x) is None:
                        ex_time[x] =[]

                for rs1 in rs['dayOfWeek']:
                    it={}
                    ex_time[rs1].append(self.convert_to_12_hour_format(rs['opens'])+' - '+self.convert_to_12_hour_format(rs['closes']))
                    
            else:
                it={}
                if ex_time.get(rs['dayOfWeek']) is None:
                    ex_time[rs['dayOfWeek']] = []
                    ex_time[rs['dayOfWeek']].append(self.convert_to_12_hour_format(rs['opens'])+' - '+self.convert_to_12_hour_format(rs['closes']))
                else:
                    ex_time[rs['dayOfWeek']].append(self.convert_to_12_hour_format(rs['opens'])+' - '+self.convert_to_12_hour_format(rs['closes']))
        TIME.append(ex_time)
        
        TIME = sort_dates(TIME)
        TIME = format_timing(TIME)
        ITEM['open_closed_time']=TIME
        try:
            ITEM['images']=[row['image'][-1]]
        except:
            ITEM['images']=[]
        ITEM['latitude']=row['geo']['latitude']
        ITEM['longitude']=row['geo']['longitude']
        yield(ITEM)
        txt=response.xpath('//script[@id="__REACT_QUERY_STATE__"]/text()').get()
        txt=str(txt).strip()
        txt=txt.encode().decode('unicode-escape')
        txt=str(txt).replace('%5C', '\\')
        DATA=json.loads(txt)
        Data=DATA['queries']
        for rows in Data:
            if 'getStoreV1' in rows['queryHash']:
                row=rows['state']['data']
                for k,v in row['catalogSectionsMap'].items():
                    for rcs in v:
                        for rs in rcs['payload']['standardItemsPayload']['catalogItems']:
                            MENU={}
                            MENU['SHEET']='menu_items'
                            MENU['uuid']=rs['uuid']
                            MENU['restaurant uuid']=ITEM['uuid']
                            MENU['name']=str(rs['title']).encode('latin-1').decode('utf-8')
                            MENU['description']=str(rs.get('itemDescription','')).encode('latin-1').decode('utf-8')
                            MENU['scraping date']=self.DATE_CRAWL
                            MENU['rating']=''
                            try:
                                new_temp = []
                                temp = [rs['imageUrl']]
                                for temp_img in temp:
                                    if temp_img.strip() != "":
                                        new_temp.append(temp_img)
                                    
                                MENU['images']=new_temp

                            except:
                                MENU['images']=[]
                            MENU['restaurant_id']=ITEM['uuid']
                            yield(MENU)

    def convert_to_12_hour_format(self,time_str):
        time_obj = datetime.strptime(time_str, '%H:%M')
        hour = time_obj.strftime('%I').lstrip('0')
        minute = time_obj.strftime('%M')
        am_pm = time_obj.strftime('%p').lower()
        if minute == "00" or minute=="0am" or minute=="0pm":
            return f'{hour}{am_pm}'
        return f'{hour}:{minute}{am_pm}'
        

