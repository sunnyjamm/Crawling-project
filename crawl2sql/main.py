# module import
import geohash2
import pandas as pd
import requests
import configparser
config = configparser.ConfigParser()

import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb

from module.kakao import kakao
from module.zigbang import SearchDatas

config.read("./datas.ini")
info = config["kakao"]
print("import done")

# Set Datas range
gwangjin = SearchDatas("서울시 광진구")

# Zigbang crawling for sales data
zigbang_oneroom = gwangjin.zigbang_oneroom()
zigbang_villa = gwangjin.zigbang_villa()
zigbang_officetel = gwangjin.zigbang_officetel()
zigbang_apt = gwangjin.zigbang_apt()

# Receive crawling for get facility data
# And also concat sales and facility data
oneroom = kakao(info['REST_API'], target= zigbang_oneroom)
apt = kakao(info['REST_API'], target= zigbang_apt)
villa = kakao(info['REST_API'], target= zigbang_villa)
officetel = kakao(info['REST_API'], target= zigbang_officetel)

# Sql server connect
engine = create_engine("mysql+mysqldb://root:dss@35.184.129.247:3306/map_crawl", encoding='utf-8')
conn = engine.connect()

# Dataframe to sql and insert
oneroom.to_sql(name='oneroom', con=engine, if_exists='replace', index=False)
apt.to_sql(name='apt', con=engine, if_exists='replace', index=False)
villa.to_sql(name='villa', con=engine, if_exists='replace', index=False)
officetel.to_sql(name='officetel', con=engine, if_exists='replace', index=False)
