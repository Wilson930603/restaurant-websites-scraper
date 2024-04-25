# Restaurant Project
This scrapy contains multiple spiders that extracts restaurant location information along with its menu that is available from google maps, trip advisor, ubereats, grubhub, and yelp. The data is extracted and the addresss is used to get the longitude and latitude from googole geocoding api, and then the data is pushed to db.

## Requirments
- Install python version >= 3.6.8.
- Open the project directory in terminal.
- Install the packages using the follwing command:
```bash
pip install -r requirments.txt
```
### Usage
- Open the project folder.
- Make sure the IP is whitelisted on which the scirpt is meant to run.
- Open the terminal and enter the following commands to run spiders
- Google_restaurant spider:
```bash
scrapy crawl google_restaurant
```
- Grubhub spider:
```bash
scrapy crawl grubhub
```
- Tripadvisor spider:
```bash
scrapy crawl tripadvisor
```
- Ubereats spider:
```bash
scrapy crawl ubereats
```
- Yelp spider:
```bash
scrapy crawl yelp_com
```
- The spiders will run and populate the database.
- To remove the duplicates that will be available after running all the spiders. Use the following script.
```bash
python drop_duplicates.py
```
- This script will drop duplicats from restaurant table, on the basis of longitude, latitude, name, and addresses. Duplicates in menu table will also be removed.

- To run all the scirpt at once use the following command.
```bash
python wraper.py
```
### Items Extracted
There are two types of items that are extracted.
1. Restaurant information
```bash
provider
uuid
name
address
rating
scraping date
latitude
longitude
types
url
open_closed_time
description
images
```
2. Menu Information
```bash
uuid
restaurant uuid
name
description
images
scraping date
rating
restaurant_id
```