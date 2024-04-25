import scrapy,json,requests
from datetime import datetime
from urllib.parse import quote,unquote
from crawldata.functions import *
import uuid
class CrawlerSpider(scrapy.Spider):
    name = 'tripadvisor'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Referer': 'https://www.tripadvisor.com/','content-type': 'application/json','Origin': 'https://www.tripadvisor.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
    custom_settings={'CLOSESPIDER_ITEMCOUNT': 20000,'ROTATING_PROXY_LIST_PATH':'proxies.txt','ROTATING_PROXY_PAGE_RETRY_TIMES':200,'CONCURRENT_REQUESTS_PER_IP':1,'DOWNLOADER_MIDDLEWARES':{'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,'rotating_proxies.middlewares.BanDetectionMiddleware': 620}}
    headers_json = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Referer': 'https://www.tripadvisor.com/','content-type': 'application/json','x-requested-by': '','Origin': 'https://www.tripadvisor.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
    api_images = "https://www.tripadvisor.com/DynamicPlacementAjax?aggregationId=101&albumViewMode=imageThumbs&albumid=101&filter=7&albumPartialsToUpdate=partial&offset={off_set}&loadDirection=Downward&heroMinWidth=1880&heroMinHeight=400&gridItemMinWidth=376&updateType=partial&placementRollUps=responsive-photo-viewer&puid={uid}&geo={geo_id}&detail={detail_id}&area=QC_Meta_Mini%7CPhoto_Lightbox&metaReferer=Restaurant_Review"
    headers_images = {
        'Sec-Ch-Device-Memory': '8',
        'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'Sec-Ch-Ua-Arch': '"arm"',
        'Sec-Ch-Ua-Full-Version-List': '"Google Chrome";v="113.0.5672.126", "Chromium";v="113.0.5672.126", "Not-A.Brand";v="24.0.0.0"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Model': '""',
        'Sec-Ch-Ua-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'X-Puid': '',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept-Language': 'en-US,en;q=0.9',
        }
    ZIPCODE = [
        'Oklahoma',
        'Oklahoma City',
        'Shawnee',# Oklahoma
        "Austin",# Texas"
        "Dallas",# Texas"
        "Tulsa",# Oklahoma"
        "Little Rock",# Arkansas" n
        "Kansas City",# Missouri"
        "Omaha",# Nebraska"
        "Indianapolis",# Indiana"
        "Santa Fe", #New Mexico"
        "Baton Rouge",# Louisiana" 
        "bethel"#, Oklahoma",
        "Tecumseh"#, Oklahoma",   
    ]
    url='https://www.tripadvisor.com'
    def __init__(self,zip=None, *args,**kwargs):
        if zip:
            self.ZIPCODE=zip
        super(CrawlerSpider, self).__init__(*args, **kwargs)
    def start_requests(self):
        # yield scrapy.Request(
        #     'https://www.tripadvisor.com/Restaurant_Review-g51560-d3369860-Reviews-Vast-Oklahoma_City_Oklahoma.html',
        #     callback=self.parse_results
        #     )
        yield scrapy.Request(self.url,callback=self.parse_token,headers=self.headers,dont_filter=True)
    def parse_token(self,response):
        print('parse_token')
        Data=str(response.xpath('//script[@async and contains(@src,"securityToken")]/@src').get()).replace('%5C', '')
        Data=unquote(Data)
        Token=str(Data).split('"securityToken":"')[1].split('"')[0]
        self.headers_json['x-requested-by']=Token
        for ZIPCODE in self.ZIPCODE:
            json_data = [{'query': '5eec1d8288aa8741918a2a5051d289ef','variables': {'request': {'query': ZIPCODE,'limit': 10,'scope': 'WORLDWIDE','locale': 'en-US','scopeGeoId': 1,'searchCenter': None,'types': ['LOCATION','QUERY_SUGGESTION','RESCUE_RESULT',],'locationTypes': ['GEO','AIRPORT','ACCOMMODATION','ATTRACTION','ATTRACTION_PRODUCT','EATERY','NEIGHBORHOOD','AIRLINE','SHOPPING','UNIVERSITY','GENERAL_HOSPITAL','PORT','FERRY','CORPORATION','VACATION_RENTAL','SHIP','CRUISE_LINE','CAR_RENTAL_OFFICE']}}}]
            url='https://www.tripadvisor.com/data/graphql/ids'
            yield scrapy.Request(url,callback=self.parse_location,method='POST',body=json.dumps(json_data),headers=self.headers_json,meta={'ZIPCODE':ZIPCODE},dont_filter=True)
    def parse_location(self, response):
        print('parse_location')
        ZIPCODE=response.meta['ZIPCODE']
        DATA=json.loads(response.text)
        Data=DATA[0]['data']['Typeahead_autocomplete']['results']
        for row in Data:
            details=row.get('details','')
            if details!='':
                if details['localizedName']==ZIPCODE:
                    url=self.url+(details['RESTAURANTS_URL'])
                    yield scrapy.Request(url,callback=self.parse_list,headers=self.headers,dont_filter=True)
    def parse_list(self,response):
        print('parse_list')
        Data=response.xpath('//div[@class="geo_entry"]')
        if len(Data)>0:
            for row in Data:
                url=self.url + row.xpath('.//a/@href').get()
                yield scrapy.Request(url,callback=self.parse_group,dont_filter=True)
                # break
            next_page=response.xpath('//div[contains(@class,"pagination")]//a[contains(@class,"next")]/@href').get()
            if next_page:
                url=self.url+next_page
                yield scrapy.Request(url,callback=self.parse_list_next,headers=self.headers,dont_filter=True)
        else:
            Data=response.xpath('//div[@data-test-target="restaurants-list"]//div[contains(@data-test,"_list_item")]')
            for row in Data:
                url=self.url + row.xpath('.//a/@href').get()
                yield scrapy.Request(url,callback=self.parse_results,dont_filter=True)
            next_page=response.xpath('//div[contains(@class,"pagination")]//a[contains(@class,"next")]/@href').get()
            if next_page:
                url=self.url+next_page
                yield scrapy.Request(url,callback=self.parse_group,dont_filter=True)
    def parse_list_next(self,response):
        print('parse_listing')
        urls=response.xpath('//ul[@class="geoList"]/li/a/@href').getall()
        for link in urls:
            url=self.url+link
            yield scrapy.Request(url,callback=self.parse_group,dont_filter=True)
        next_page=response.xpath('//a[contains(@class,"sprite-pageNext")]/@href').get()
        if next_page:
            url=self.url+next_page
            yield scrapy.Request(url,callback=self.parse_list_next,headers=self.headers,dont_filter=True)
    def parse_group(self,response):
        print('parse_groups')
        Data=response.xpath('//div[@data-test-target="restaurants-list"]//div[contains(@data-test,"_list_item")]')
        for row in Data:
            url=self.url + row.xpath('.//a/@href').get()
            yield scrapy.Request(url,callback=self.parse_results,dont_filter=True)
            # break
        next_page=response.xpath('//div[contains(@class,"pagination")]//a[contains(@class,"next")]/@href').get()
        if next_page:
            url=self.url+next_page
            yield scrapy.Request(url,callback=self.parse_group,dont_filter=True)
    def get_ids(self,URL):
        ids = URL.split('Restaurant_Review')[-1].split('-')
        return ids[1].replace('g',''), ids[2].replace('d','')
    def parse_results(self, response):

        HTML=response.xpath('//script[contains(text(),"__WEB_CONTEXT__")]/text()').get()
        HTML=str(HTML).split('pageManifest:')[1].split('};')[0]
        DATA=json.loads(HTML)
        Data=DATA['urqlCache']['results']
        get_uid = response.xpath('''//script[contains(text(),"define('page-model', [], function()")]/text()''').get().split('return')[-1][:-5].strip()
        get_uid = json.loads(get_uid)
        get_uid = get_uid['session'].get('uid')
        header_img = self.headers_images
        header_img['X-Puid'] = get_uid
        geo_id, detail_id = self.get_ids(response.url)
        url = self.api_images.format(off_set='0',uid=get_uid,geo_id=geo_id,detail_id=detail_id)
        
        for rows in Data:
            rows1=json.loads(Data[rows]['data'])
            if 'RestaurantPresentation_searchRestaurantsByGeo' in rows1:
                for row in rows1['RestaurantPresentation_searchRestaurantsByGeo']['restaurants']:
                    ITEM={}
                    ITEM['SHEET']='restaurants'
                    ITEM['provider']=self.name
                    ITEM['uuid']=row['external_reference']['id']
                    ITEM['name']=row['name']
                    ITEM['address']=row['localizedRealtimeAddress'].replace('Address:','').strip()
                    ITEM['description']=response.xpath("//div[contains(text(), 'About')]/following-sibling::div/text()").get(default='')
                    star = row['reviewSummary']['rating']
                    if star == -1:
                        star = 0
                    ITEM['rating'] = star
                    ITEM['scraping date']=self.DATE_CRAWL
                    ITEM['latitude'] = None
                    ITEM['longitude'] = None
                    TAGS=[]
                    for rs in row['topTags']:
                        if rs['secondary_name']==None:
                            TAGS.append(rs['tag']['localizedName'])
                    ITEM['types']= TAGS
                    ITEM['url']=response.url
                    operation_dict = []
                    if row.get('open_hours'):
                        if row['open_hours'].get('schedule'):
                            for days in row['open_hours'].get('schedule').keys():
                                operation_time = ""
                                for time in row['open_hours']['schedule'][days]:
                                    if operation_time == "":
                                        operation_time = f"{self.convert_to_12_hour_format(time['open_time'])} - {self.convert_to_12_hour_format(time['close_time'])}"
                                    else:
                                        operation_time += f", {self.convert_to_12_hour_format(time['open_time'])} - {self.convert_to_12_hour_format(time['close_time'])}"
                                operation_dict.append({self.get_day(days):operation_time})
                    # print(f'Before: {operation_dict}')
                    operation_dict = sort_dates(operation_dict)
                    # print(f'After: {operation_dict}')
                    operation_dict = format_timing(operation_dict)
                    ITEM['open_closed_time'] = operation_dict
                    if get_uid:
                        yield scrapy.Request(
                            url,
                            callback=self.parse_images,
                            headers=header_img,
                            meta={
                                "ITEM":ITEM,
                                "offset":0,
                                "geo_id":geo_id,
                                "detail_id":detail_id,
                                "uid":get_uid,
                                "json_data":DATA,
                            }
                        )
                        
                    # yield(ITEM)
                    


    def parse_images(self,response):
        ITEM = response.meta.get('ITEM')
        get_uid = response.meta.get('uid')
        geo_id = response.meta.get('geo_id')
        detail_id = response.meta.get('detail_id')
        offset = response.meta.get('offset')
        json_data = response.meta.get('json_data')

        offset_extend = len(response.xpath('//img/@data-lazyurl').extract())
        data_img = response.xpath('//img/@data-lazyurl').extract()
        if ITEM.get('images'):
            ITEM["images"].extend(data_img)
        else:
            ITEM["images"] = data_img
        if offset_extend < 48:
            yield ITEM
            Data = json_data['urqlCache']['results']
            for rows in Data:
                rows1=json.loads(Data[rows]['data'])
                if 'RestaurantPresentation_searchRestaurantsByGeo' in rows1:
                    for row in rows1['RestaurantPresentation_searchRestaurantsByGeo']['restaurants']:
                        if row['menu']['has_provider']==True:
                            for rcsall in Data:
                                rs=json.loads(Data[rcsall]['data'])
                                if 'menuResponse' in rs:
                                    for rs1 in rs['menuResponse']['providerMenu']['menu']:
                                        for rs2 in rs1['sections']:
                                            for rs3 in rs2['items']:
                                                for rs4 in rs3['prices']:
                                                    TITLE=''
                                                    if 'title' in rs3 and rs3['title']:
                                                        TITLE=rs3['title']
                                                    if 'title' in rs4 and rs4['title']:
                                                        TITLE+=(' '+rs4['title'])
                                                    if 'unit' in rs4 and rs4['unit']:
                                                        TITLE+=(' '+rs4['unit'])
                                                    TITLE=str(TITLE).strip()
                                                    MENU={}
                                                    MENU['SHEET']='menu_items'
                                                    MENU['uuid']=key_MD5(TITLE)
                                                    MENU['restaurant uuid']=ITEM['uuid']
                                                    MENU['name']=TITLE
                                                    MENU['description']=rs3.get('description','')
                                                    MENU['scraping date']=self.DATE_CRAWL
                                                    MENU['images'] = []
                                                    MENU['rating']=''
                                                    MENU['restaurant_id'] =ITEM['uuid']
                                                    yield(MENU)

        else:
            header_img = self.headers_images
            header_img['X-Puid'] = get_uid
            url = self.api_images.format(off_set=str(offset+offset_extend),uid=get_uid,geo_id=geo_id,detail_id=detail_id)
            yield scrapy.Request(
                        url,
                        callback=self.parse_images,
                        headers=header_img,
                        meta={
                            "ITEM":ITEM,
                            "offset":offset+offset_extend,
                            "geo_id":geo_id,
                            "detail_id":detail_id,
                            "uid":get_uid,
                            "json_data":json_data
                        }
                    )
    
    def get_day(self,day):
        data = {"sun":"Sunday","mon":"Monday","tue":"Tuesday","wed":"Wednesday","thu":"Thursday","fri":"Friday","sat":"Saturday"}

        return data[day]

    def convert_to_12_hour_format(self,time_str):
        time_obj = datetime.strptime(time_str, '%H:%M:%S')
        hour = time_obj.strftime('%I').lstrip('0')
        minute = time_obj.strftime('%M')
        am_pm = time_obj.strftime('%p').lower()
        if minute == "00":
            return f'{hour}{am_pm}'
        return f'{hour}:{minute}{am_pm}'
