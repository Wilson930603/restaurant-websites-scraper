# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
#pip install mysql-connector-python
import mysql.connector,re
from mysql.connector import Error,IntegrityError
from crawldata.functions import *
import json
import datetime
class CrawldataPipeline:
    def open_spider(self,spider):
        # self.DATABASE_NAME='crawler'    
        # self.HOST='localhost'
        # self.username='root'
        # self.password=''
        # self.TABLE={}
        self.DATABASE_NAME = "eatthat_scraping"
        self.HOST = "xxxxxx"
        self.username = "xxxxx"
        self.password = "xxxxx"
        self.PORT = 25060
        self.TABLE={}
        try:
            self.conn = mysql.connector.connect(host=self.HOST,port=self.PORT,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
            if self.conn.is_connected():
                print('Connected to DB')
                db_Info = self.conn.get_server_info()
                print(f"Connected to MySQL Server version {db_Info}")
            else:
                print('Not connect to DB')
                raise Exception("Failed to connect to the database.")
        except Error as e:
            print(f"Error while connecting to MySQL {e}")
            raise Exception(f"Error while connecting to MySQL")
            self.conn=None
        except Exception as ex:
            print(f"Error while connecting to MySQL {e}")
            raise Exception(f"Error while connecting to MySQL")
            self.conn=None
    
    def close_spider(self,spider):
        if self.conn.is_connected():
            self.conn.close()
    def get_restaurants_id(self,ITEM):
        temp_id = ITEM['restaurant_id']
        SQL = f"SELECT id from restaurants WHERE `uuid`='{temp_id}';"
        # input(SQL)
        mycursor = self.conn.cursor(buffered=True)
        mycursor.execute(SQL)
        result = mycursor.fetchone()
        mycursor.close()

        if result:
            restaurant_id = result[0]
            ITEM['restaurant_id'] = restaurant_id
        else:
            ITEM['restaurant_id'] = None
        return ITEM
    def find_dup_restaurant(self,ITEM):
        SQL = "SELECT id FROM " + ITEM['SHEET'] + " WHERE uuid = '" + ITEM['uuid'] + "';"
        cursor = self.conn.cursor()
        cursor.execute(SQL)
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    def find_dup_menu(self,ITEM):
        SQL = "SELECT id FROM " + ITEM['SHEET'] + " WHERE uuid = '" + ITEM['uuid'] + "';"
        cursor = self.conn.cursor()
        cursor.execute(SQL)
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    def get_table_data(self,table_name):
        SQL="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = Database() AND TABLE_NAME = '"+table_name+"';"
        mycursor = self.conn.cursor()
        mycursor.execute("SET NAMES 'UTF8MB4'")
        mycursor.execute("SET CHARACTER SET UTF8MB4")
        mycursor.execute(SQL)
        myresult = mycursor.fetchall()
        for x in myresult:
            if not x[0] in self.TABLE[table_name]:
                self.TABLE[table_name].append(x[0])
    def process_item(self, ITEM, spider):
        #print('Do with DB')
        # Check and add more field if not existed in data table
        if ITEM['SHEET'] == "menu_items":
            ITEM = self.get_restaurants_id(ITEM)
            if ITEM['restaurant_id'] is None:
                print('Id not available in restaurant table')
                return
        else:
            lat, long = get_coordinates_google(ITEM["address"])
            ITEM["longitude"] = long
            ITEM["latitude"] = lat
            ITEM["location"] = 'POINT({}, {})'.format(lat,long)
        item={}
        for K,V in ITEM.items():
            if 'image' in K or 'types' in K:
                item[self.Get_Key_String(K)]=V
            else:
                item[self.Get_Key_String(K)]=str(V).replace('\\','').replace("'","\'")
        if not 'SHEET' in item.keys():
            item['SHEET']=spider.name
        if not item['SHEET'] in self.TABLE:
            self.TABLE[item['SHEET']]=[]
            self.create_table(self.conn,item['SHEET'],item)
            self.get_table_data(item['SHEET'])
            print('FIELDS:',self.TABLE[item['SHEET']])
            if item['SHEET'] == 'restaurants':
                self.TABLE['rating']=[]
                self.get_table_data('rating')
                print('FIELDS:',self.TABLE['rating'])


        for key in item.keys():
            if not key.replace('`','') in self.TABLE[item['SHEET']] and key!='SHEET':
                self.TABLE[item['SHEET']].append(key.replace('`',''))
                self.add_column_to_db(self.conn,item['SHEET'],key)
        find_true = None
        # if ITEM['SHEET'] == "restaurants":
        #     find_true = self.find_dup_restaurant(ITEM)
        # elif ITEM['SHEET'] == "menu_items":
        #     find_true = self.find_dup_menu(ITEM)
        if find_true:
            SQL = "UPDATE " + item['SHEET'] + " SET "
            update_values = []
            
            for key in self.TABLE[item['SHEET']]:
                if key == 'id' or key == 'SHEET':
                    continue
                if self.Get_Key_String(key) in item:
                    V = str(item[self.Get_Key_String(key)]).replace("'", "''").replace("\\", "\\\\")
                    if V == 'None':
                        V = ""
                else:
                    V = ""
                update_values.append(self.Get_Key_String(key) + "='" + V + "'")
            
            SQL += ', '.join(update_values)
            SQL += " WHERE id = " + str(find_true) + ";"
            # input(SQL)
            cursor = self.conn.cursor()
            try:
                cursor.execute(SQL)
                self.conn.commit()
                print(f"{item['SHEET']}: Updated in DB")
            except Exception as ex:
                # input(ex)
                print('Error updating record')
        else:
            restaurant_id = self.insert_in_db(item['SHEET'],item)
            if item['SHEET'] == 'restaurants':
                self.insert_in_db_rating('rating',item,restaurant_id)
            # Insert data to table
            #except:
            #    print('Error: ',item,'\n',SQL)
            # input(SQL)
            return item
    def get_latest_id_by_uuid(self, uuid):
        SQL = "SELECT id FROM restaurants WHERE uuid = %s ORDER BY id DESC LIMIT 1;"
        cursor = self.conn.cursor()
        cursor.execute(SQL, (uuid,))
        result = cursor.fetchone()
        if result is not None:
            latest_id = result[0]
            return latest_id
        else:
            return None

    def insert_in_db_rating(self,table_name,item,restaurant_id):
        SQL = "INSERT INTO rating (uuid, provider, rating, restaurant_id, restaurant_uuid, latitude, longitude, created_at) VALUES ("
        # SQL = "INSERT INTO rating (provider, rating, restaurant_id, restaurant_uuid, created_at) VALUES ("
        SQL += "'" + key_MD5(item["`uuid`"]) + "', "
        SQL += "'" + str(item["`provider`"]).replace("'", "''").replace("\\", "\\\\") + "', "
        if str(item["`rating`"]).replace("'", "''").replace("\\", "\\\\").strip() != "" and str(item["`rating`"]).replace("'", "''").replace("\\", "\\\\").strip() != "None":
            SQL += str(item["`rating`"]).replace("'", "''").replace("\\", "\\\\") + ", "
        else:
            SQL = SQL.replace('rating, ','')
        SQL += str(restaurant_id).replace("'", "''").replace("\\", "\\\\") + ", "
        SQL += "'" + item["`uuid`"] + "', "
        if str(item["`latitude`"]).replace("'", "''").replace("\\", "\\\\").strip() != "" and str(item["`latitude`"]).replace("'", "''").replace("\\", "\\\\").strip() != "None":
            SQL += str(item["`latitude`"]).replace("'", "''").replace("\\", "\\\\") + ", "
        else:
            SQL = SQL.replace('latitude, ','')
        if str(item["`longitude`"]).replace("'", "''").replace("\\", "\\\\").strip() != "" and str(item["`longitude`"]).replace("'", "''").replace("\\", "\\\\").strip() != "None":
            SQL += str(item["`longitude`"]).replace("'", "''").replace("\\", "\\\\") + ", "
        else:
            SQL = SQL.replace('longitude, ','')
        SQL += "'" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        SQL += ");"

        cursor = self.conn.cursor()
        try:
            cursor.execute(SQL)
            self.conn.commit()
            print("rating: Inserted to DB")
        except IntegrityError:
            print("Record already exists")
        except Exception as ex:
            input(f'Exception: {ex}. SQL:{SQL}')
    def insert_in_db(self,table_name,item):
        SQL="INSERT INTO "+table_name
        LIST_FIELDS=''
        VALUES=''
        STR_UPDATE=''
        for key in self.TABLE[table_name]:
            if key == 'id':
                continue
            
            if key == 'SHEET':
                continue
            if LIST_FIELDS=='':
                LIST_FIELDS=self.Get_Key_String(key)
            else:
                LIST_FIELDS+=','+self.Get_Key_String(key)
            
            if self.Get_Key_String(key) in item:
                if key == 'open_closed_time':
                    # Convert the JSON value to a string and escape special characters
                    V = json.dumps(item[self.Get_Key_String(key)]).replace("'", '"').replace("\\", "\\\\")[1:-1]
                elif 'image' in key or 'types' in key:
                    V = json.dumps(item[self.Get_Key_String(key)]).replace("'", '"').replace("\\", "\\\\")
                elif 'location' in key:
                    V = str(item[self.Get_Key_String(key)]).replace("\\", "\\\\")
                else:
                    V = str(item[self.Get_Key_String(key)]).replace("'", "''").replace("\\", "\\\\")
                if (V == 'None' or V=="") and ('latitude' in key or 'longitude' in key or 'rating' in key):
                    LIST_FIELDS = LIST_FIELDS.replace(','+self.Get_Key_String(key),"")
                    continue
                if V=='None':
                    V=""
            else:
                V=""
            if VALUES=='':
                VALUES="'"+V+"'"
            else:
                if 'location' in key:
                    VALUES+=","+V

                else:
                    VALUES+=",'"+V+"'"
        
            if STR_UPDATE=="":
                STR_UPDATE=self.Get_Key_String(key)+"='"+V+"'"
            else:
                if 'location' in key:
                    STR_UPDATE+=", "+self.Get_Key_String(key)+"="+V+""
                else:
                    STR_UPDATE+=", "+self.Get_Key_String(key)+"='"+V+"'"
        SQL+="("+LIST_FIELDS+") VALUES("+VALUES+") ON DUPLICATE KEY UPDATE "+STR_UPDATE+";"
        cursor = self.conn.cursor()
        # print(SQL)
        try:
            cursor.execute(SQL)
            self.conn.commit()
            print(f"{table_name}: Inserted to DB")
            if table_name == 'restaurants':
                return self.get_latest_id_by_uuid(item['`uuid`'])
        except IntegrityError as ex:
            print(f'Record already exist')
            input(f'{SQL}\n{ex}')
        except Exception as ex:
            input(f'{SQL}\n{ex}')
    def get_DataType(self,strtxt):
        strtxt=str(strtxt).strip()
        if Get_Number(strtxt)==strtxt:
            if '.' in strtxt and str(strtxt).count('.')==1:
                return 'FLOAT'
            elif not '.' in str(strtxt):
                return 'INT'
            else:
                return 'TEXT'
        else:
            return 'TEXT'
    def create_rating_table(self):
        create_rating_table = """
            CREATE TABLE IF NOT EXISTS rating (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                uuid CHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                provider VARCHAR(255) NOT NULL,
                rating DOUBLE,
                restaurant_id BIGINT UNSIGNED,
                restaurant_uuid VARCHAR(1024),
                latitude DOUBLE,
                longitude DOUBLE,
                created_at TIMESTAMP
            );
            """
        mycursor = self.conn.cursor()
        mycursor.execute(create_rating_table)
        self.conn.commit()
        print('Rating table created')
    
    def create_table(self,connection,table_name,item):
        SQL='CREATE TABLE IF NOT EXISTS '+table_name+'('
        columns = []
        if table_name == 'restaurants':
            self.create_rating_table()
        # Add the auto-increment primary key column
        columns.append('id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY')
        i=0
        for K in item.keys():
            if K == 'SHEET':
                continue
            elif 'restaurant_id' in K  and table_name == 'menu_items':
                columns.append(K + ' BIGINT UNSIGNED')
                columns.append('FOREIGN KEY (' + K + ') REFERENCES restaurants(id)')
                continue

            if 'open_closed_time' in K.lower() or 'types' in K.lower() or 'image' in K.lower():
                columns.append(K + ' JSON')
            elif 'description' in K.lower():
                columns.append(K + ' TEXT')
            elif 'scraping_date' in K.lower():
                columns.append(K + ' DATE')
            elif 'latitude' in K.lower() or 'longitude' in K.lower() or 'rating' in K.lower():
                columns.append(K + ' DOUBLE')
            elif 'location' in K.lower():
                columns.append(K + 'POINT')
            else:
                columns.append(K+' VARCHAR(1024)')
        SQL += ', '.join(columns) + ');'
        print(SQL)
        try:
            print('Creating Table:',table_name)
            cursor = connection.cursor()
            cursor.execute(SQL)
            connection.commit()
        except Exception as ex:
            print(f"{ex}: {SQL}")
        # input(f'here: {SQL}')
        # input(item)
    def add_column_to_db(self,connection,table_name,field):
        SQL="ALTER TABLE "+table_name+" ADD COLUMN "+field+" "+self.get_DataType(field)+ " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL;"
        try:
            print('Adding column name:',field)
            cursor = connection.cursor()
            cursor.execute(SQL)
            connection.commit()
        except Exception as ex:
            print(f"{ex}: {SQL}")
        # input(SQL)
    def Get_Key_String(self,xau):
        if xau == 'SHEET':
            return xau
        else:
            KQ=re.sub(r"([^A-Za-z0-9])","_", str(xau).strip())
            return '`'+KQ+'`'