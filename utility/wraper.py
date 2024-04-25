import os
import datetime
import threading
# Change the current working directory to to current working directory of the project
os.chdir("/root/restaurant-project")

def run_spider(spider_name):
    os.system('/usr/local/bin/scrapy crawl ' + spider_name)


date_today = datetime.datetime.now().strftime("%Y_%m_%d")

grubhub = threading.Thread(target=run_spider, args=('grubhub',))
ubereats = threading.Thread(target=run_spider, args=('ubereats',))
yelp = threading.Thread(target=run_spider, args=('yelp_com',))
tripadvisor = threading.Thread(target=run_spider, args=('tripadvisor',))
google = threading.Thread(target=run_spider, args=('google_restaurant',))


grubhub.start()
ubereats.start()
yelp.start()
tripadvisor.start()
google.start()



grubhub.join()
ubereats.join()
yelp.join()
tripadvisor.join()
google.join()

os.system('python3 drop_duplicates.py')

