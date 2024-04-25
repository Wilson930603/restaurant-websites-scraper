# Scrapy settings for crawldata project
BOT_NAME = 'crawldata'
SPIDER_MODULES = ['crawldata.spiders']
NEWSPIDER_MODULE = 'crawldata.spiders'
URLLENGTH_LIMIT = 50000
DOWNLOAD_DELAY = 0.5
ROBOTSTXT_OBEY = False
HTTPERROR_ALLOW_ALL=True
TELNETCONSOLE_ENABLED = False
ITEM_PIPELINES = {'crawldata.pipelines.CrawldataPipeline': 300}
# LOG_FILE='./LOGS.log'