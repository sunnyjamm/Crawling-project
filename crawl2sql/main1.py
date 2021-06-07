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

engine = create_engine("mysql+mysqldb://root:fast@proxy73.rt3.io:33046/data", encoding='utf-8')
conn = engine.connect()

# Set Datas range
gwangjin = SearchDatas("서울시 광진구")

# Zigbang crawling for sales data
# Receive crawling for get facility data
# And also concat sales and facility data
# Dataframe to sql and insert

zigbang_oneroom = gwangjin.zigbang_oneroom()
zigbang_oneroom = zigbang_oneroom.fillna(0)
zigbang_oneroom  = zigbang_oneroom.drop(["tags","title"], 1)
zigbang_oneroom.to_sql(name='zigbang_oneroom', con=engine, if_exists='replace', index=False)


zigbang_villa = gwangjin.zigbang_villa()
zigbang_villa = zigbang_villa.fillna(0)
zigbang_villa  = zigbang_villa.drop(["tags","title"], 1)
zigbang_villa.to_sql(name='zigbang_villa', con=engine, if_exists='replace', index=False)


zigbang_officetel = gwangjin.zigbang_officetel()
zigbang_officetel = zigbang_officetel.fillna(0)
zigbang_officetel  = zigbang_officetel.drop(["tags","title"], 1)
zigbang_officetel.to_sql(name='zigbang_officetel', con=engine, if_exists='replace', index=False)

zigbang_apt = gwangjin.zigbang_apt()
zigbang_apt = zigbang_apt.fillna(0)
zigbang_apt.to_sql(name='zigbang_officetel', con=engine, if_exists='replace', index=False)
