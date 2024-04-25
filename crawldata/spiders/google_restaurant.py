import scrapy,time
from datetime import datetime
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from crawldata.functions import *
import re

class CrawlerSpider(scrapy.Spider):
    name = 'google_restaurant'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    ZIPCODE=[
        'https://www.google.com/maps/search/Restaurant+in+Oklahoma',
        'https://www.google.com/maps/search/Restaurant+in+Oklahoma+City',
        'https://www.google.com/maps/search/Restaurant+in+Austin',
        'https://www.google.com/maps/search/Restaurant+in+Dallas',
        'https://www.google.com/maps/search/Restaurant+in+Tulsa',
        'https://www.google.com/maps/search/Restaurant+in+Little+Rock',
        'https://www.google.com/maps/search/Restaurant+in+Kansas+City',
        'https://www.google.com/maps/search/Restaurant+in+Omaha',
        'https://www.google.com/maps/search/Restaurant+in+Indianapolis',
        'https://www.google.com/maps/search/Restaurant+in+Santa+Fe',
        'https://www.google.com/maps/search/Restaurant+in+Baton+Rouge',
        'https://www.google.com/maps/search/Restaurant+in+bethel',
        'https://www.google.com/maps/search/Restaurant+in+tecumseh+Oklahoma',
        ]
    URL='https://www.google.com'
    def start_requests(self):
        yield scrapy.Request(self.URL,callback=self.parse_list,dont_filter=True)
    def parse_list(self,response):
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options,executable_path=GeckoDriverManager().install())#GeckoDriverManager().install())

        driver.maximize_window()
        driver.get('https://www.google.com/maps/search/Restaurants')
        time.sleep(3)
        URLS=[]
        for ZIP in self.ZIPCODE:
            driver.get(ZIP)
            time.sleep(5)
            LIST=None
            SL=0
            LV=0
            RUN=True
            while RUN:
                LIST=driver.find_elements(By.XPATH,'//div[contains(@jsaction,"mouseover:pane")]')
                # LIST=driver.find_elements_by_xpath('//div[@role="article"]')
                print(len(LIST))
                # RUN=False
                driver.execute_script("arguments[0].scrollIntoView();", LIST[len(LIST)-1])
                time.sleep(3)
                if "You've reached the end of the list" in driver.page_source:
                    RUN=False
                if len(LIST)>SL:
                    SL=len(LIST)
                else:
                    LV+=1
                if LV>=10:
                    RUN=False
            if LIST:
                for LS in LIST:
                    # link=LS.find_element_by_xpath('.//a').get_attribute('href')
                    link=LS.find_element(By.XPATH,'.//a').get_attribute('href')
                    print(link)
                    if not link in URLS:
                        URLS.append(link)
                        # break
        for url in URLS:
            try:
                driver.get(url)
            except:
                print(url)
                try:
                    driver.close()
                except Exception as ex:
                    print(ex)
                try:
                    driver.quit()
                except Exception as ex:
                    print(ex)
                driver = webdriver.Firefox(options=options,executable_path=GeckoDriverManager().install())#GeckoDriverManager().install())
                driver.get(url)
            time.sleep(3)
            try:
                ITEM={}
                ITEM['SHEET']='restaurants'
                ITEM['provider']=self.name
                ITEM['uuid']=key_MD5(url)
                ITEM['description'] = ''

                # ITEM['restaurant name']=driver.find_element_by_xpath('//h1').text
                ITEM['name']=driver.find_element(By.XPATH,'//h1').text
                ITEM['address']=driver.find_element(By.XPATH,'//button[@data-item-id="address"]').get_attribute('aria-label').replace('Address:','').strip()
                ITEM['scraping date']=self.DATE_CRAWL
                if 'pane.rating.category' in driver.page_source:
                    ITEM['types']=[driver.find_element(By.XPATH,'//button[@jsaction="pane.rating.category"]').text]
                else:
                    ITEM['types'] = []
                E=driver.find_element(By.XPATH,'//div[@jsaction="pane.reviewChart.moreReviews"]')
                driver.execute_script("arguments[0].scrollIntoView();", E)
                time.sleep(3)
                ITEM['rating']=E.find_element(By.XPATH,'.//div[@class="fontDisplayLarge"]').text
                ITEM['url']=url
                latitude, longitude = self.get_long_lat(driver.current_url)
                ITEM['latitude'] = latitude
                ITEM['longitude'] = longitude
                RES=scrapy.Selector(text=driver.page_source)
                TIMES=RES.xpath('//table[contains(@class,"fontBodyMedium")]/tbody/tr')
                # cehck = RES.xpath('//table[contains(@class,"fontBodyMedium")]/../../../div/@aria-label').get(default='NA')
                # input(cehck)
                # TIME = self.format_time(cehck)
                # input(TIME)
                TIME=[]
                for row in TIMES:
                    TM={}
                    TITLE=row.xpath('./td[1]//div/text()').get()
                    # VAL=row.xpath('./td[2]//li/text()').get(default='').replace('\u202f','').replace('\u20133','')
                    VAL=row.xpath('./td[2]/@aria-label').get(default='').replace('\u202f','').replace('\u2013','').replace('u202f','').replace('to','-')
                    TM[TITLE]=VAL
                    TIME.append(TM)
                # input(f'before:{TIME}')
                TIME = sort_dates(TIME)
                TIME = format_timing(TIME)
                # input(f'After:{TIME}')

                ITEM['open_closed_time'] = TIME
                img_list = []
                try:
                    element_img = driver.find_element(By.XPATH,'//button/div[contains(text(),"photos")]/../../..')
                    driver.execute_script("arguments[0].scrollIntoView();", element_img)
                    driver.find_element(By.XPATH,'//button/div[contains(text(),"photos")]').click()
                    time.sleep(3)
                    org_num = 0
                    self.scrol_to_end(driver)
                    images = driver.find_elements(By.XPATH,'//div[@role="img"]')
                    for itr,x in enumerate(images):

                        if itr>0:
                            x.click()
                        time.sleep(0.5)
                        url_img = x.get_attribute('style').split('url("')[-1][:-3]
                        print(url_img)
                        img_list.append(url_img)
                    new_dimensions = "w1920-h1080"
                    # input(len(img_list))
                    img_list = [self.replace_dimensions(img,new_dimensions) for img in img_list]
                except Exception as e:
                    #driver.save_screenshot('test.png')
                    print(f' the exception in images is {e}')
                    
                    img_list = []
                ITEM["images"] = img_list
                yield(ITEM)
                # input(ITEM)
            except:
                pass
        driver.quit()
    def scrol_to_end(self,driver):
        org_num = 0
        images = driver.find_elements(By.XPATH,'//div[@role="img"]')
        while org_num != len(images):
            org_num = len(images)
            driver.execute_script("arguments[0].scrollIntoView();", images[len(images)-1])
            time.sleep(4)
            images = driver.find_elements(By.XPATH,'//div[@role="img"]')
            print(f'Remaining:{org_num}/{len(images)}')
    def replace_dimensions(self,url, new_dimensions):
        pattern = r"w\d+-h\d+"
        replaced_url = re.sub(pattern, new_dimensions, url)
        return replaced_url
    def get_long_lat(self,url):
        for x in url.split('/'):
            if '@' in x:
                return x.split(',')[0].replace('@',''), x.split(',')[1]
    
    def get_time(self,hours):
        listing = hours.split(';')
        dict_data = []
        for x in listing:
            temp = x.split(',')
            dict_data.append({temp[0].strip():temp[1].replace('Hide open hours for the week','').strip()})
        return dict_data
        
    def format_time(self,hours):
        data = hours.split(';')
        new_dict = []
        for x in data:
            key = x.split(',')[0].strip()
            value = x.split(',')[
                1
                ].replace('Hide open hours for the week','').replace('to','-').strip().replace('\u202f','').replace('\u2013','').replace('u202f','')
            new_dict.append({key:value})
        
        return new_dict
