import mysql.connector
from random import randint
from crawldata.functions import key_MD5
# Example usage
# DATABASE_NAME='crawler'
# HOST='localhost'
# username='root'
# password=''
DATABASE_NAME='eatthat_scraping'
HOST='xxxxxxx'
username='xxxxxx'
password='xxxxxx'
port=25060

cnx = mysql.connector.connect(host=HOST,database=DATABASE_NAME,user=username,password=password,port=port,charset='utf8')

mycursor = cnx.cursor()


def drop_dup_data_long_lat(mycursor,cnx):
    print('Droping Duplicates by longitude and latitude')
    ids_to_update ={}
    rating_to_update = {}
    
    SQL = """
        SELECT GROUP_CONCAT(r1.id) AS duplicate_ids, r1.latitude, r1.longitude, AVG(r1.rating) AS average_rating
        FROM restaurants r1
        JOIN (
            SELECT latitude, longitude
            FROM restaurants
            GROUP BY latitude, longitude
            HAVING COUNT(*) > 1
        ) r2 ON r1.latitude = r2.latitude AND r1.longitude = r2.longitude
        WHERE r1.latitude IS NOT NULL AND r1.longitude IS NOT NULL
        GROUP BY r1.latitude, r1.longitude
        ORDER BY r1.longitude, r1.latitude;

    """

    mycursor.execute(SQL)

    batch_size = 4  # Adjust the batch size as per your needs
    while True:
        batch = mycursor.fetchmany(batch_size)
        if not batch:
            break
        # Process the batch of rows
        for row in batch:
            ids_to_update[row[0].split(',')[0]] = row[0].split(',')[1:]
            if row[-1] is not None:
                rating_to_update[row[-1]] = row[0].split(',')
            print(row)
    print(ids_to_update)
    # Update foreign key values in the menu_items table
    for main_id, duplicate_ids in ids_to_update.items():
        print(main_id,duplicate_ids)
        query = f"UPDATE menu_items SET restaurant_id = {main_id} WHERE restaurant_id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query)
    for main_id, duplicate_ids in rating_to_update.items():
        print(main_id,duplicate_ids)

        query = f"UPDATE restaurants SET rating = {main_id} WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query)
        ###Add code for updating uuids for restaurant and rating table
        new_uuid = key_MD5(', '.join(duplicate_ids))
        query_set_uuid_restaurant = f"UPDATE restaurants SET uuid = '{new_uuid}' WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query_set_uuid_restaurant)

        query_set_uuid_rating = f"UPDATE rating SET restaurant_uuid = '{new_uuid}' WHERE restaurant_id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query_set_uuid_rating)

    # # # Remove duplicate rows from the restaurant table
    for main_id, duplicate_ids in ids_to_update.items():
        delete_query = f"DELETE FROM restaurants WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(delete_query)

    cnx.commit()
def drop_duplicates_by_name_address(mycursor,cnx):
    print('Droping Duplicates based on Name, and address')
    ids_to_update ={}
    rating_to_update = {}
    SQL = """
        SELECT GROUP_CONCAT(r.id) AS duplicate_ids, r.name, COUNT(r.name) cname, r.address,COUNT(r.address) caddress, AVG(r.rating) AS average_rating FROM restaurants r GROUP BY r.name, r.address HAVING (cname>1) AND (caddress>1);
        """
    mycursor.execute(SQL)

    batch_size = 4  # Adjust the batch size as per your needs
    while True:
        batch = mycursor.fetchmany(batch_size)
        if not batch:
            break
        # Process the batch of rows
        for row in batch:
            ids_to_update[row[0].split(',')[0]] = row[0].split(',')[1:]
            if row[-1] is not None:
                rating_to_update[row[-1]] = row[0].split(',')
            print(row)
    
     # Update foreign key values in the menu_items table
    for main_id, duplicate_ids in ids_to_update.items():
        print(main_id,duplicate_ids)
        query = f"UPDATE menu_items SET restaurant_id = {main_id} WHERE restaurant_id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query)
    for main_id, duplicate_ids in rating_to_update.items():
        print(main_id,duplicate_ids)

        query = f"UPDATE restaurants SET rating = {main_id} WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query)
        ###Add code for updating uuids for restaurant and rating table
        new_uuid = key_MD5(', '.join(duplicate_ids))
        query_set_uuid_restaurant = f"UPDATE restaurants SET uuid = '{new_uuid}' WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query_set_uuid_restaurant)

        query_set_uuid_rating = f"UPDATE rating SET restaurant_uuid = '{new_uuid}' WHERE restaurant_id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(query_set_uuid_rating)

    # # # Remove duplicate rows from the restaurant table
    for main_id, duplicate_ids in ids_to_update.items():
        delete_query = f"DELETE FROM restaurants WHERE id IN ({', '.join(duplicate_ids)})"
        mycursor.execute(delete_query)

    cnx.commit()
    
def drop_duplicates_menu_table(mycursor,cnx):
    print('Droping Duplicates in menu_table')
    delete_query = """
        DELETE t1
        FROM menu_items t1
        JOIN (
            SELECT uuid, MIN(id) AS min_id
            FROM menu_items
            GROUP BY uuid
            HAVING COUNT(*) > 1
        ) t2 ON t1.uuid = t2.uuid AND t1.id > t2.min_id
        """
    # Execute the delete query
    mycursor.execute(delete_query)

    # Commit the changes to the database
    cnx.commit()
# Close the cursor and database connection
drop_dup_data_long_lat(mycursor,cnx)
drop_duplicates_by_name_address(mycursor,cnx)
drop_duplicates_menu_table(mycursor,cnx)
mycursor.close()
cnx.close()
print("Drop Duplication Complete")