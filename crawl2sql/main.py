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

# Sql server connect

engine = create_engine("mysql+mysqldb://root:fast@ipaddress/data", encoding='utf-8')
conn = engine.connect()

# Set Datas range
gwangjin = SearchDatas("서울시 광진구")

# Zigbang crawling for sales data
# Receive crawling for get facility data
# And also concat sales and facility data
# Dataframe to sql and insert

zigbang_oneroom = gwangjin.zigbang_oneroom()
oneroom = kakao(info['REST_API'], target= zigbang_oneroom)
oneroom.to_sql(name='oneroom', con=engine, if_exists='replace', index=False)


zigbang_villa = gwangjin.zigbang_villa()
villa = kakao(info['REST_API'], target= zigbang_villa)
villa.to_sql(name='villa', con=engine, if_exists='replace', index=False)


zigbang_officetel = gwangjin.zigbang_officetel()
officetel = kakao(info['REST_API'], target= zigbang_officetel)
officetel.to_sql(name='officetel', con=engine, if_exists='replace', index=False)


zigbang_apt = gwangjin.zigbang_apt()
apt = kakao(info['REST_API'], target= zigbang_apt)
apt.to_sql(name='apt', con=engine, if_exists='replace', index=False)
