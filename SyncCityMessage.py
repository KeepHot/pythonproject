import datetime

from loguru import logger

import json
import pymysql
import pymongo

# 日志打印
logger.add('synCityMessageLog.log')

"""mysql连接及查询插入操作"""
# 连接数据库
host = '192.168.104.49'
port = 3306
user = 'ecdbuser'
pwd = 'Fe98321$3'
db = 'vesync_main'
charset = 'utf8mb4'
# client = pymongo.MongoClient(host="192.168.104.49:27017", username="weather", password="ciDeploy123")
client = pymongo.MongoClient("mongodb://weather:ciDeploy123@192.168.104.49:27017/weather")
mongoDB = client['weather']
geographyDB = mongoDB['city_geography_msg']

# 获取数据库连接
with pymysql.connect(host=host, port=port, user=user, password=pwd, database=db, charset=charset) as conn:
    # 获取游标
    with conn.cursor() as cur:

        logger.info("start-syncDistinctCityMessage-time: {}", datetime.datetime.now())

        """查询city表中的数据"""
        # SQL查询city表中重复的数据，若重复按照更新时间选取
        select_distinct_query = 'SELECT city_name, state_name, country, longitude, latitude , create_time, max(update_time) FROM ( SELECT * FROM city a WHERE (a.city_name, a.state_name, a.country) IN ( SELECT city_name, state_name, country FROM city GROUP BY city_name, state_name, country HAVING count(*) > 1 ) ORDER BY city_name ) t1 GROUP BY city_name'
        cur.execute(select_distinct_query)
        city_msg_tuple = cur.fetchall()
        select_data_size = len(city_msg_tuple)
        logger.info("query distinctByUpdateTime data complete, city total size: {}", select_data_size)

        while select_data_size > 0:
            # 构建city新表数据
            city_msg_list = []
            for data in city_msg_tuple:
                cityName = data[0]
                stateName = data[1]
                country = data[2]
                cityNameAlias = str(cityName).strip(' ').lower()
                stateNameAlias = str(stateName).strip(' ').lower()
                countryAlias = str(country).strip(' ').lower()
                longitude = data[3]
                latitude = data[4]
                isValidGeographicMsg = True
                createdTime = data[5]
                updatedTime = data[6]
                class_msg = "com.etekcity.cloud.openweatherservice.dto.mongodb.CityGeographyMsg"
                insert_data = {'cityName': cityName, 'stateName': stateName, 'country': country,
                               'cityNameAlias': cityNameAlias, 'stateNameAlias': stateNameAlias,
                               'countryAlias': countryAlias,
                               'longitude': longitude, 'latitude': latitude,
                               'isValidGeographicMsg': isValidGeographicMsg,
                               'createdTime': createdTime, 'updatedTime': updatedTime, '_class': class_msg}
                try:
                    geographyDB.insert_one(insert_data)
                except Exception as e:
                    logger.exception("insert data failed, dataMsg: {}, exception:", insert_data, e)
                select_data_size = select_data_size - 1
            insert_data_size = len(city_msg_list)
        logger.info("syncDistinctCityMessage success,endTime:{}", datetime.datetime.now())

        logger.info("start-syncNotDistinctCityMessage-time: {}", datetime.datetime.now())

        # SQL查询city表中不重复的数据
        select_no_distinct_query = 'SELECT city_name, state_name, country, longitude, latitude , create_time, update_time FROM city WHERE id NOT IN ( SELECT id FROM city a WHERE (a.city_name, a.state_name, a.country) IN ( SELECT city_name, state_name, country FROM city GROUP BY city_name, state_name, country HAVING count(*) > 1 ) )'
        cur.execute(select_no_distinct_query)
        city_msg_tuple = cur.fetchall()
        select_data_size = len(city_msg_tuple)
        logger.info("query data no distinct complete, city total size: {}", select_data_size)

        while select_data_size > 0:
            # 构建city新表数据
            city_msg_list = []
            for data in city_msg_tuple:
                cityName = data[0]
                stateName = data[1]
                country = data[2]
                cityNameAlias = str(cityName).strip(' ').lower()
                stateNameAlias = str(stateName).strip(' ').lower()
                countryAlias = str(country).strip(' ').lower()
                longitude = data[3]
                latitude = data[4]
                isValidGeographicMsg = True
                createdTime = data[5]
                updatedTime = data[6]
                class_msg = "com.etekcity.cloud.openweatherservice.dto.mongodb.CityGeographyMsg"
                insert_data = {'cityName': cityName, 'stateName': stateName, 'country': country,
                               'cityNameAlias': cityNameAlias, 'stateNameAlias': stateNameAlias,
                               'countryAlias': countryAlias,
                               'longitude': longitude, 'latitude': latitude,
                               'isValidGeographicMsg': isValidGeographicMsg,
                               'createdTime': createdTime, 'updatedTime': updatedTime, '_class': class_msg}
                try:
                    geographyDB.insert_one(insert_data)
                except Exception as e:
                    logger.exception("insert data failed, dataMsg: {}, exception:", insert_data, e)
                select_data_size = select_data_size - 1
            insert_data_size = len(city_msg_list)

        logger.info("syncNotDistinctCityMessage success,endTime:{}", datetime.datetime.now())

        select_repeat_query = 'SELECT * FROM city a WHERE (a.city_name, a.state_name, a.country) IN ( SELECT city_name, state_name, country FROM city GROUP BY city_name, state_name, country HAVING count(*) > 1 ) ORDER BY city_name'
        cur.execute(select_repeat_query)
        repeat_city_msg = cur.fetchall()
        select_data_size = len(repeat_city_msg)
        logger.info("query data complete, repeat city total size: {}", select_data_size)

        select_city_query = 'SELECT * FROM city'
        cur.execute(select_city_query)
        repeat_city_msg = cur.fetchall()
        select_data_size = len(repeat_city_msg)
        logger.info("select_city_query , city total size: {}", select_data_size)
