from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

from configparser import ConfigParser
import psycopg2
import uuid
import os
import json


CUISINE_ENUM_LIST = set(
    [
        "ITALIAN",
        "AMERICAN",
        "JAPANESE",
        "CHINESE",
        "INDIAN",
        "MEXICAN",
        "FRENCH",
        "BRAZILIAN",
        "THAI",
        "KOREAN",
        "JAMAICAN",
        "TURKISH",
    ]
)
CUISINE_ENUM_DEFAULT = "OTHER"


def load_config(filename="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, filename)
        )

    return config


def connect(config):
    """Connect to the PostgreSQL database server"""
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print("Connected to the PostgreSQL server.")
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def insert_restaurant(
    restaurant_name,
    restaurant_address_line_1,
    restaurant_address_line_2,
    restaurant_address_city,
    restaurant_address_state,
    restaurant_address_zip,
    restaurant_address_country,
    restaurant_description,
    restaurant_cuisine_ENUM,
    restaurant_display_name,
    restaurant_type_ENUM,
):
    """Insert a new vendor into the vendors table"""

    address_sql = """insert 
            into
                consi.address
                (city,country,line1,line2,state,zip_code,id) 
            values
        (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """

    restaurant_address_id = None

    restaurant_sql = """INSERT INTO consi.restaurant(address_id,average_menu_rating,contact_id,cuisine,description,display_name,name,rating,restaurant_type,id) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;"""

    vendor_id = None
    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    address_sql,
                    (
                        restaurant_address_city,
                        restaurant_address_country,
                        restaurant_address_line_1,
                        restaurant_address_line_2,
                        restaurant_address_state,
                        restaurant_address_zip,
                        str(uuid.uuid4()),
                    ),
                )

                # get the generated id back
                rows = cur.fetchone()
                if rows:
                    restaurant_address_id = rows[0]
                # execute the INSERT statement
                cur.execute(
                    restaurant_sql,
                    (
                        restaurant_address_id,
                        None,
                        "ce406f47-ecea-494a-bed8-106a74673307",
                        restaurant_cuisine_ENUM.upper(),
                        restaurant_description,
                        restaurant_display_name,
                        restaurant_name,
                        None,
                        restaurant_type_ENUM.upper(),
                        str(uuid.uuid4()),
                    ),
                )

                # get the generated id back
                rows = cur.fetchone()
                if rows:
                    vendor_id = rows[0]
                    print(vendor_id)

                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return vendor_id


if __name__ == "__main__":
    config = load_config()
    connect(config)

    driver = webdriver.Firefox()

    files = [
        f for f in os.listdir("./data") if os.path.isfile(os.path.join("./data", f))
    ]
    store_files = [f for f in files if "stores" in f]
    for zipCode in store_files:
        print(zipCode)
        with open("./data/" + zipCode, "r") as zipFile:
            items = json.load(zipFile)["data"]["feedItems"]
            print("Items in zip ", zipCode, len(items))
            for i, feedItem in enumerate(items):
                if feedItem["type"] == "REGULAR_STORE":
                    url = feedItem["store"]["actionUrl"]
                    storeURL = "https://www.ubereats.com" + url
                    print("Going to:", storeURL)
                    driver.get(storeURL)
                    try:
                        driver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/div[1]/div[2]/main/div/div[2]/div/div/div[1]/div[1]/a",
                        ).click()
                        description = driver.find_element(
                            By.XPATH,
                            "/html/body/div[1]/div[1]/div[1]/div[2]/main/div/div[2]/div/div/div[1]/div[1]",
                        ).text
                        print(description)
                        restaurant_name = feedItem["store"]["title"]["text"]
                        print(restaurant_name)
                        item_uuid = feedItem["uuid"]

                        with open(
                            "./data/" + item_uuid + ".json", "r"
                        ) as restaurantFile:
                            restaurant = json.load(restaurantFile)
                            restaurant_name = restaurant["data"]["title"]

                            if ("\r\n") in restaurant["data"]["location"][
                                "streetAddress"
                            ]:
                                street_address, suite = restaurant["data"]["location"][
                                    "streetAddress"
                                ].split("\r\n")
                            else:
                                street_address = restaurant["data"]["location"][
                                    "streetAddress"
                                ]
                                suite = None
                            city = restaurant["data"]["location"]["city"]
                            country = restaurant["data"]["location"]["country"]
                            postalCode = restaurant["data"]["location"]["postalCode"]
                            state = restaurant["data"]["location"]["region"]
                            cuisine_list = restaurant["data"]["cuisineList"]
                            cuisine = None
                            for i in cuisine_list:
                                if i.upper() in CUISINE_ENUM_LIST:
                                    cuisine = i.upper()
                            if cuisine == None:
                                cuisine = CUISINE_ENUM_DEFAULT

                            insert_restaurant(
                                restaurant_name.replace(" ", "_").upper(),
                                street_address,
                                suite,
                                city,
                                state,
                                postalCode,
                                country,
                                description,
                                cuisine,
                                restaurant_name,
                                "SMALL",
                            )
                            print(
                                "Inserting",
                                restaurant_name.replace(" ", "_").upper(),
                                street_address,
                                suite,
                                city,
                                state,
                                postalCode,
                                country,
                                description,
                                cuisine,
                                restaurant_name,
                                "SMALL",
                            )

                        print(item_uuid)

                    except Exception as e:
                        print(e)
                        pass
                if i == 10:
                    break
            break
            print()
