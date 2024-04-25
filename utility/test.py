import json
import mysql.connector
import pytest

DATABASE_NAME = 'crawler'
HOST = 'localhost'
USERNAME = 'root'
PASSWORD = ''
# DATABASE_NAME='restaurants'
# HOST='dev1.crawler.pro.vn'
# USERNAME='root'
# PASSWORD='Crawler@2021'

def test_load_records():
    cnx = mysql.connector.connect(
        host=HOST, database=DATABASE_NAME,
        user=USERNAME, password=PASSWORD,
        charset='utf8'
    )
    cursor = cnx.cursor()

    # Execute the query to fetch the records
    query = "SELECT open_closed_time FROM restaurants"
    cursor.execute(query)

    # Fetch all the records
    records = cursor.fetchall()

    # Iterate over each record and try loading it with json.loads
    for record in records:
        try:
            json_data = json.loads(record[0])
        except json.JSONDecodeError:
            pytest.fail(f"Failed to load record: {record[0]}")

    # Close the cursor and the database connection
    cursor.close()
    cnx.close()

    # If all records were loaded without any exceptions, the test passes
    assert True
def test_json_records_restaurat():
    cnx = mysql.connector.connect(
        host=HOST, database=DATABASE_NAME,
        user=USERNAME, password=PASSWORD,
        charset='utf8'
    )
    cursor = cnx.cursor()

    # Retrieve the JSON records from the database
    query = "SELECT images FROM restaurants"
    cursor.execute(query)
    records = cursor.fetchall()

    for record in records:
        json_data = record[0]
        try:
            parsed_data = json.loads(json_data)
            assert isinstance(parsed_data, list)
        except (json.JSONDecodeError, AssertionError):
            pytest.fail("Failed to load restaurant images")

def test_json_records_type_restaurat():
    cnx = mysql.connector.connect(
        host=HOST, database=DATABASE_NAME,
        user=USERNAME, password=PASSWORD,
        charset='utf8'
    )
    cursor = cnx.cursor()

    # Retrieve the JSON records from the database
    query = "SELECT images FROM restaurants"
    cursor.execute(query)
    records = cursor.fetchall()

    for record in records:
        json_data = record[0]
        try:
            parsed_data = json.loads(json_data)
            assert isinstance(parsed_data, list)
        except (json.JSONDecodeError, AssertionError):
            pytest.fail("Failed to load restaurnat type")


def test_json_records_menu():
    cnx = mysql.connector.connect(
        host=HOST, database=DATABASE_NAME,
        user=USERNAME, password=PASSWORD,
        charset='utf8'
    )
    cursor = cnx.cursor()

    # Retrieve the JSON records from the database
    query = "SELECT images FROM menu_items"
    cursor.execute(query)
    records = cursor.fetchall()

    for record in records:
        json_data = record[0]
        try:
            parsed_data = json.loads(json_data)
            assert isinstance(parsed_data, list)
        except (json.JSONDecodeError, AssertionError):
            pytest.fail("Failed to load menu images")

# Run the test
test_load_records()
test_json_records_restaurat()
test_json_records_type_restaurat()
test_json_records_menu()