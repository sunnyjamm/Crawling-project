#!/usr/bin/env python
# coding: utf-8

# ### zigbang 원룸 매물 데이터 수집 
# - 조금 복잡한 형태의 크롤링 
# - 결과 데이터를 수집하기 위해서 requests > response를 여러번 해야함 
# - 웹사이트 분석을 잘해야함 
# - 동이름 입력 > 매물정보 DF 

# In[ ]:


# 1. 서비스 분석: url 
# (1) 동이름 입력 > 동이름에 해당하는 데이터 
# https://apis.zigbang.com/v2/search?leaseYn=N&q=동작구&serviceType=원룸
# (2) geohash code > 매물 아이디 
# https://apis.zigbang.com/v2/items?deposit_gteq=0&domain=zigbang
# &geohash=wydme&needHasNoFiltered=true&rent_gteq=0&sales_type_in=전세|월세&service_type_eq=원룸
# (3) 매물 아이디 > 매물 정보 
# POST: https://apis.zigbang.com/v2/items/list
# (젤아래 view source) PARAMS: {"domain":"zigbang","withCoalition":true,"item_ids":[27091158,27127450,27013159]}


# In[2]:


#!pip install geohash


# In[1]:


import geohash2
import requests 
import pandas as pd


# In[2]:


# 1. 동이름 > 위도 경도 


# In[4]:


addr = "성수동"
url = f"https://apis.zigbang.com/v2/search?leaseYn=N&q={addr}&serviceType=원룸"
response = requests.get(url)
response


# In[5]:


data = response.json()["items"][0]
lat, lng = data["lat"], data["lng"]
lat, lng


# In[6]:


# 2. 위도, 경도 > geohash code


# In[7]:


# precision이 커질수록 영역이 작아진다. 4정도 되면 서울 전지역 
geohash_code = geohash2.encode(lat, lng, precision=5)
geohash_code


# In[8]:


# 3. geohash code > 매물 아이디


# In[9]:


geohash_code 
url = f"https://apis.zigbang.com/v2/items?deposit_gteq=0&domain=zigbang&geohash={geohash_code}&needHasNoFiltered=true&rent_gteq=0&sales_type_in=전세|월세&service_type_eq=원룸"
response = requests.get(url)
response


# In[10]:


datas = response.json()["items"]
len(datas), datas[0]
item_ids = [data["item_id"] for data in datas] # list comprehension
len(item_ids), item_ids[:3]


# In[11]:


# 4. 매물 아이디 > 매물 정보 


# In[20]:


# 매물 데이터는 최대 1000개씩 가져옴 
url = "https://apis.zigbang.com/v2/items/list"
params = {"domain":"zigbang","withCoalition":"true","item_ids":item_ids}
response = requests.post(url, params)
response


# In[22]:


response.text


# In[23]:


datas = response.json()["items"] #json은 딕셔너리로 파싱
columns = ["item_id", "sales_type", "deposit", "rent", "size_m2", "floor", "building_floor", 
           "title", "manage_cost", "address1", "random_location"]
len(datas), datas[0]
df = pd.DataFrame(datas)[columns]
df.tail()


# In[24]:


# precision이 커서 중동이 나오넹?
df = df[df["address1"].str.contains("성수")]
df.tail()


# In[ ]:





# In[ ]:





# In[ ]:




