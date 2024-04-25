import scrapy,json,requests
from datetime import datetime
from urllib.parse import quote,unquote
from crawldata.functions import *

class CrawlerSpider(scrapy.Spider):
    name = 'yelp_com'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    custom_settings={'CLOSESPIDER_ITEMCOUNT': 20000,'ROTATING_PROXY_LIST_PATH':'proxies.txt','ROTATING_PROXY_PAGE_RETRY_TIMES':200,'CONCURRENT_REQUESTS_PER_IP':1,'DOWNLOADER_MIDDLEWARES':{'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,'rotating_proxies.middlewares.BanDetectionMiddleware': 620}}
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
        "bethel, Oklahoma",
        "Tecumseh, Oklahoma",  
    ]
    URL='https://www.yelp.com'
    handle_httpstatus_list = [503]
    def start_requests(self):
        for ZIPCODE in self.ZIPCODE:
            yield scrapy.Request(self.URL,callback=self.parse_list,meta={'ZIPCODE':ZIPCODE,'Start':0},dont_filter=True)
            # break
    def parse_list(self,response):
        ZIPCODE=response.meta['ZIPCODE']
        Start=response.meta['Start']
        if (response.status in self.handle_httpstatus_list 
            or response.xpath('//h1[text()="This page is not available"]').get()
            or response.xpath('//p[text()="Oops! Something went wrong. Please try again."]').get()):
            yield scrapy.Request(response.url,callback=self.parse_list,meta={'ZIPCODE':ZIPCODE,'Start':Start},dont_filter=True)
            return
        URL='https://www.yelp.com/search/snippet?find_desc=Restaurants&find_loc='+ZIPCODE+'&start='+str(Start)
        response=requests.get(URL,proxies={"http":response.meta['proxy'],"https":response.meta['proxy']})
        DATA=json.loads(response.text)
        Data=DATA['searchPageProps']['rightRailProps']['searchMapProps']['hovercardData']
        for k,row in Data.items():
            ITEM={}
            ITEM['SHEET']='restaurants'
            ITEM['provider']=self.name
            ITEM['uuid']=k
            ITEM['name']=row['name']
            ITEM['address']=' '.join(row['addressLines']).replace('Address:','').strip()
            ITEM['rating']=row['rating']
            ITEM['scraping date']=self.DATE_CRAWL
            ITEM['latitude'] = None
            ITEM['longitude'] = None
            CATES=[]
            for rs in row['categories']:
                CATES.append(rs['title'])
            ITEM['types']=CATES
            ITEM['url']='https://www.yelp.com'+row['businessUrl']
            if 'redirect_url=' in ITEM['url']:
                ITEM['url']=unquote(str(ITEM['url']).split('redirect_url=')[1].split('&')[0])
            else:
                ITEM['url']=str(ITEM['url']).split('?')[0]
            url=str(ITEM['url']).replace('/biz/', '/menu/')

            yield scrapy.Request(ITEM['url'],callback=self.resturant_page,meta={"ITEM":ITEM},dont_filter=True)
            # break
        if len(Data)>5:
            Start+=len(Data)
            yield scrapy.Request(self.URL,callback=self.parse_list,meta={'ZIPCODE':ZIPCODE,'Start':Start},dont_filter=True)
    
    def resturant_page(self,response):
        ITEM=response.meta['ITEM']
        if (response.status in self.handle_httpstatus_list 
            or response.xpath('//h1[text()="This page is not available"]').get()
            or response.xpath('//p[text()="Oops! Something went wrong. Please try again."]').get()):
            print('Blocked resturant_page resturant')
            yield scrapy.Request(ITEM['url'],callback=self.resturant_page,meta={"ITEM":ITEM},dont_filter=True)
            return
        na = []
        # try:
        day = response.xpath('//tbody/tr/th/p/text()').extract()
        time = response.xpath('//tbody/tr/td/ul/li/p/text()').extract()
        operating_hours = []
        for i in range(len(day)):
            # print(time[i])
            if i==7:
                break
            operating_hours.append({self.get_day(day[i]): self.modify_time_range(time[i])})
        # except:
        #     operating_hours =[]
        operating_hours = sort_dates(operating_hours)
        operating_hours = format_timing(operating_hours)
        # input(f'After: {operating_hours}')
        ITEM['open_closed_time'] = operating_hours

        ITEM['description'] = response.xpath('//meta[@property="og:description"]/@content').get(default='').replace('Specialties:','')
        yield scrapy.Request(
            ITEM['url'].replace('/biz/','/biz_photos/'),
            callback=self.resturant_images,
            meta={"ITEM":ITEM},dont_filter=True)

    def resturant_images(self,response):
        ITEM=response.meta['ITEM']
        if (response.status in self.handle_httpstatus_list 
            or response.xpath('//h1[text()="This page is not available"]').get()
            or response.xpath('//p[text()="Oops! Something went wrong. Please try again."]').get()):
            print('Blocked resturant_images resturant.')
            yield scrapy.Request(response.url,callback=self.resturant_images,meta={"ITEM":ITEM},dont_filter=True)
            return

        data_img = response.xpath('//ul//img[@class="photo-box-img"]/@src').extract()

        if ITEM.get('images'):
            ITEM["images"].extend(data_img)
        else:
            ITEM["images"] = data_img
        next = response.xpath('//a[contains(@class,"next ")]/@href').get()
        if next:
            yield scrapy.Request('https://www.yelp.com'+next,callback=self.resturant_images,dont_filter=True,meta={"ITEM":ITEM})
        else:
            ITEM["images"] = ITEM["images"]
            yield ITEM
            url=str(ITEM['url']).replace('/biz/', '/menu/')
            yield scrapy.Request(url,callback=self.parse_content,meta={'ITEM':ITEM,'Type':0},dont_filter=True)

    def parse_content(self,response):
        ITEM=response.meta['ITEM']
        Type=response.meta['Type']
        if (response.status in self.handle_httpstatus_list 
            or response.xpath('//h1[text()="This page is not available"]').get()
            or response.xpath('//p[text()="Oops! Something went wrong. Please try again."]').get()):
            print('Blocked parse_content Menu')
            yield scrapy.Request(url,callback=self.parse_content,meta={'ITEM':ITEM,'Type':Type},dont_filter=True)
            return
        MENUS=response.xpath('//div[@class="menu-sections"]/div/div[contains(@class,"menu-item")]')
        for row in MENUS:
            TITLE=cleanhtml(row.xpath('.//h4').get()).strip()
            MENU={}
            MENU['SHEET']='menu_items'
            MENU['uuid']=key_MD5(ITEM['uuid']+TITLE)
            MENU['restaurant uuid']=ITEM['uuid']
            MENU['name']=TITLE
            MENU['description']=row.xpath('.//p[@class="menu-item-details-description"]/text()').get()
            MENU['images'] = row.xpath('.//a[@data-analytics-label="biz-photo"]/img/@src').extract()
            MENU['scraping date']=self.DATE_CRAWL
            MENU['rating']=''
            MENU['restaurant_id'] =ITEM['uuid']
            yield(MENU)
        if Type==0:
            Type+=1
            SUB=response.xpath('//ul[@class="sub-menus"]//a/@href').getall()
            for sub in SUB:
                url=self.URL+sub
                yield scrapy.Request(url,callback=self.parse_content,meta={'ITEM':ITEM,'Type':Type},dont_filter=True)

    def get_day(self,day):
        data = {"sun":"Sunday","mon":"Monday","tue":"Tuesday","wed":"Wednesday","thu":"Thursday","fri":"Friday","sat":"Saturday"}

        return data[day.lower()]

    def modify_time_format(self,time_str):
        time_parts = time_str.split(' ')
        hour, minute = time_parts[0].split(':')
        meridian = time_parts[1]
        if minute == "00":
            return f"{hour}{meridian.lower()}"
        modified_hour = str(int(hour)) if hour.startswith('0') else hour
        modified_time_str = f"{modified_hour}:{minute}{meridian.lower()}"
        return modified_time_str

    def modify_time_range(self,time_range):
        parts = time_range.split(' - ')
        if len(parts)!=2:
            return time_range
        start_time = parts[0]
        end_time = parts[1]

        modified_start_time = self.modify_time_format(start_time)
        modified_end_time = self.modify_time_format(end_time)

        modified_range = f"{modified_start_time} - {modified_end_time}"
        return modified_range
    
    