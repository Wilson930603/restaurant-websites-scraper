import scrapy,json,requests
from datetime import datetime,timedelta
from urllib.parse import quote
from crawldata.functions import *
class CrawlerSpider(scrapy.Spider):
    name = 'grubhub'
    conn=None
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    # download_delay = 1
    headers = {'authority': 'api-gtm.grubhub.com','accept': 'application/json','accept-language': 'en-US,en;q=0.9','cache-control': 'max-age=0','if-modified-since': '0','origin': 'https://www.grubhub.com','referer': 'https://www.grubhub.com/','sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"','sec-ch-ua-mobile': '?0','sec-ch-ua-platform': '"Windows"','sec-fetch-dest': 'empty','sec-fetch-mode': 'cors','sec-fetch-site': 'same-site','user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',}
    ZIPCODE=[
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
    def __init__(self,zip=None, *args,**kwargs):
        if zip:
            self.ZIPCODE=zip
        super(CrawlerSpider, self).__init__(*args, **kwargs)
    def start_requests(self):
        for ZIPCODE in self.ZIPCODE:
            response=requests.get('https://www.grubhub.com/eat/static-content-unauth?contentOnly=1')
            CLIENT_ID=str(response.text).split("clientId: '")[1].split("'")[0]
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36','authorization': 'Bearer','content-type': 'application/json;charset=UTF-8'}
            json_data = {'brand': 'GRUBHUB','client_id': CLIENT_ID,'device_id': 458398867,'scope': 'anonymous'}
            url='htts://api-gtm.grubhub.com/auth'
            response = requests.post(url, headers=headers, json=json_data)
            Data=json.loads(response.text) 
            token=Data['session_handle']['access_token']
            self.headers['authorization']='Bearer '+token
            url='https://api-gtm.grubhub.com/geocode/autocomplete?queryText='+ZIPCODE+'&locationBias=true'
            yield scrapy.Request(url,callback=self.parse,headers=self.headers,meta={'ZIPCODE':ZIPCODE})
    def parse(self, response):
        ZIPCODE=response.meta['ZIPCODE']
        Data=json.loads(response.text)
        for row in Data:
            KQ=str(row).split(',')[0]
            if KQ==ZIPCODE:
                url='https://api-gtm.grubhub.com/geocode?address='+quote(row)
                yield scrapy.Request(url,callback=self.parse_location,headers=self.headers)
    def parse_location(self, response):
        Data=json.loads(response.text)
        if len(Data)>0:
            data=Data[0]
            longitude=data['longitude']
            latitude=data['latitude']
            url='https://api-gtm.grubhub.com/restaurants/search/search_listing?orderMethod=delivery_or_pickup&locationMode=DELIVERY_OR_PICKUP&facetSet=umamiV6&pageSize=20&hideHateos=true&searchMetrics=true&location=POINT('+longitude+'%20'+latitude+')&preciseLocation=true&sorts=avg_rating&sortSetId=umamiv3&countOmittingTimes=true'
            yield scrapy.Request(url,callback=self.parse_results,headers=self.headers,meta={'longitude':longitude,'latitude':latitude})
    def parse_results(self, response):
        longitude=response.meta['longitude']
        latitude=response.meta['latitude']
        Data=json.loads(response.text)
        if 'results' in Data:
            for row in Data['results']:
                item={}
                item['name']=row['name']
                try:
                    item['image']=row['media_image']['base_url']+row['media_image']['public_id']+'.'+row['media_image']['format']
                except:
                    item['image']=""
                try:
                    if str(row['description'])!='null':
                        item['description']=row['description']
                except:
                    item['description']=""
                try:
                    item['address']=row['address']['street_address']
                except:
                    item['address']=""
                item['address2']=""
                try:
                    item['city']=row['address']['address_locality']
                except:
                    item['city']=""
                try:
                    item['state']=row['address']['address_region']
                except:
                    item['state']=""
                try:
                    item['zipcode']=row['address']['postal_code']
                except:
                    item['zipcode']=""
                item['website']=""
                try:
                    item['phone']=row['phone_number']
                except:
                    item['phone']=""
                item['email']=""
                try:
                    item['latitude']=row['address']['latitude']
                except:
                    item['latitude']=latitude
                try:
                    item['longitude']=row['address']['longitude']
                except:
                    item['longitude']=longitude
                try:
                    item['ratings']=row['ratings']
                except:
                    item['ratings']=[]
                ITEM={}
                ITEM['SHEET']='restaurants'
                ITEM['provider']=self.name
                ITEM['uuid']=row['restaurant_id']
                ITEM['name']=item['name']
                address = item['address']+' '+item['city']+', '+item['state']+' '+item['zipcode']
                address = address.replace('Address:','').strip()
                ITEM['address']=address
                
                ITEM['rating']=item['ratings']['rating_bayesian10_point']
                ITEM['scraping date']=self.DATE_CRAWL
                ITEM['types']= row['cuisines']
                ITEM['url']='https://www.grubhub.com/restaurant/'+row['merchant_url_path']+'/'+row['restaurant_id']
                ITEM['open_closed_time']=[]
                ITEM['description'] = item['description']
                ITEM['images']=[item['image']]
                ITEM['latitude']=item['latitude']
                ITEM['longitude']=item['longitude']
                item['menus']=[]
                url='https://api-gtm.grubhub.com/restaurants/'+row['restaurant_id']+'?version=4&variationId=rtpFreeItems&orderType=standard&hideUnavailableMenuItems=true&hideMenuItems=false&locationMode=delivery'
                yield scrapy.Request(url,callback=self.parse_detail,headers=self.headers,meta={'item':item,'ITEM':ITEM})
            if Data['pager']['current_page']<Data['pager']['total_pages']:
                url='https://api-gtm.grubhub.com/restaurants/search/search_listing?orderMethod=pickup&locationMode=PICKUP&facetSet=umamiV6&pageSize=20&hideHateos=true&searchMetrics=true&location=POINT('+longitude+'%20'+latitude+')&dinerLocation=POINT('+longitude+'%20'+latitude+')&sorts=default&radius=5&includeOffers=true&sortSetId=umamiv3&sponsoredSize=3&countOmittingTimes=true&pageNum='+str(Data['pager']['current_page']+1)+'&searchId='+Data['listing_id']
                yield scrapy.Request(url,callback=self.parse_results,headers=self.headers,meta={'longitude':longitude,'latitude':latitude})
    def parse_detail(self, response):
        item=response.meta['item']
        ITEM=response.meta['ITEM']
        DATA=json.loads(response.text)
        Data=DATA['restaurant_availability']
        DAYS=['','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        TIME=[]
        for row in Data['available_hours']:
            txt=''
            if '-' in row['time_ranges'][0]:
                Times=str(row['time_ranges'][0]).split('-')
                t1=datetime.strptime(Times[0], '%H:%M')
                T1=t1-timedelta(hours=5)
                t2=datetime.strptime(Times[1], '%H:%M')
                T2=t2-timedelta(hours=5)
                # txt=T1.strftime('%I:%M %p')+" - "+T1.strftime('%I:%M %p')
                t_one = T1.strftime('%I:%M%p').lstrip('0')
                t_two = T2.strftime('%I:%M%p').lstrip('0')
                # input(t_one)
                if t_one.split(':')[1].replace('AM','').replace('PM','').strip() =="00":
                    t_one= t_one.split(':')[0]+t_one.split(':')[1].replace('00','').strip().lower()
                if t_two.split(':')[1].replace('AM','').replace('PM','').strip() =="00":
                    t_two= t_two.split(':')[0]+t_two.split(':')[1].replace('00','').strip().lower()
                txt=t_one+" - "+t_two
            else:
                txt=row['time_ranges'][0]
            TM={DAYS[row['day_of_week']]:txt}
            TIME.append(TM)
        # input(TIME)

        # print(f'Before: {TIME}')
        TIME = sort_dates(TIME)
        TIME = format_timing(TIME)
        # print(f'After: {TIME}')
        ITEM['open_closed_time']=TIME
        yield ITEM

        menu={}
        menu['name']='Grubhub'
        menu['description']="Grubhub menu for "+item['address']
        menu['section']=[]
        Data=DATA['restaurant']
        for row in Data['menu_category_list']:
            it={}
            it['name']=row['name']
            it['entries']=[]
            for rs in row['menu_item_list']:
                MENU={}
                MENU['SHEET']='menu_items'
                if 'uuid' in rs:
                    MENU['uuid']=rs['uuid']
                else:
                    MENU['uuid']=rs['id']
                MENU['restaurant uuid']=rs['restaurant_id']
                MENU['name']=rs['name']
                MENU['description']=rs.get('description','')
                MENU['scraping date']=self.DATE_CRAWL
                MENU['rating']=''
                try:
                    MENU['images']=[rs['media_image']['base_url']+rs['media_image']['public_id']+'.'+rs['media_image']['format']]
                except:
                    MENU['images']=[]
                MENU['restaurant_id']=MENU['restaurant uuid']
                yield(MENU)
            menu['section'].append(it)
        if len(menu['section'])>0:
            item['menus'].append(menu)