import geohash2
import pandas as pd
import requests
import configparser


config = configparser.ConfigParser()
config.read("./datas.ini")
info = config["kakao"]


class SearchDatas:
    '''
    SearchDatas
    ============
    provides
    1. get_geohash : saves lat, lng, geohash data in SearchDatas class
    2. zigbang_oneroom : crawling single room type housings and returns oneroom dataframe
    3. zigbang_apt : crawling apartment types housings and returns aptartment dataframe
    4. zigbang_villa : crawling villa type housings and returns zigbang_villa dataframe
    5. zigbang_officetel: crawling officetel type housings and returns zigbang_officetel dataframe

    '''

    def __init__(self, query):
        self.query = query
        self.geohash = None
        self.lat = None
        self.lng = None
        self.get_geohash()


    def get_geohash(self, precision=5):

        '''
        SearchData.get_geohash(
        self, precision=5),

        Docstring:
        get lat, log data
        self.lat, self.lng, self.geohash


        Parameters
        ----------
        precision=5 default parameter and no changes are recommended
        because zigbang's geohash value coincide when precision is not at 5
        '''

        url = f"https://apis.zigbang.com/search?q={self.query}"
        response = requests.get(url)

        self.lat, self.lng = response.json()['items'][0]['lat'], response.json()['items'][0]['lng']
        self.geohash = geohash2.encode(self.lat, self.lng, precision=precision)


    def zigbang_oneroom(self):

        '''
        SearchData.zigbang_oneroom(
        self),

        Docstring:
        crawling zigbang oneroom data and
        Returning dataframe

        Dataframe
        ----------
        colums : "lat", "lng", "공급면적_m2", "공급면적_p", "전용면적_m2", "전용면적_p", "category"
        '''

        item_url = f"https://apis.zigbang.com/v2/items?deposit_gteq=0&domain=zigbang&geohash={self.geohash}&rent_gteq=0&sales_type_in=전세|월세&service_type_eq=원룸"
        response = requests.get(item_url)

        items = response.json()['items']
        ids = [item['item_id'] for item in items]

        url = "https://apis.zigbang.com/v2/items/list"
        dfs = []
        for idx in range(0, len(ids), 900):
            start, end = idx, idx + 900

            params = {"domain": "zigbang", "withCoalition": "false", "item_ids": ids[start:end],}
            response = requests.post(url, params)

            datas = response.json()['items']
            df = pd.DataFrame(datas)

            df['lat'] =  df['random_location'].apply(lambda x: x['lat'])
            df['lng'] =  df['random_location'].apply(lambda x: x['lng'])

            df['공급면적_m2'] = df['공급면적'].apply(lambda x:x['m2'])
            df['공급면적_p'] = df['공급면적'].apply(lambda x:x['p'])
            df['전용면적_m2'] = df['전용면적'].apply(lambda x:x['m2'])
            df['전용면적_p'] = df['전용면적'].apply(lambda x:x['p'])

            df.drop(columns=["random_location", "공급면적", "전용면적", "계약면적"], inplace=True)
            df['category'] = "원룸"
            dfs.append(df)

        zigbang_oneroom_df = pd.concat(dfs)
        zigbang_oneroom_df.reset_index(drop=True, inplace=True)
        print("end oneroom crawling")
        return zigbang_oneroom_df

    def zigbang_apt(self):

        '''
        SearchData.zigbang_apt(
        self),

        Docstring:
        crawling zigbang apt data and
        Returning dataframe

        Dataframe
        ----------
        colums : "register", "online", "rent_min", "rent_max", "rent_avg", "sales_min", "sales_max",
        "sales_avg", "offer_min", "offer_max", "offer_avg", "m2", "p", "category"

        "m2" means "m^2"
        "p" means "평"
        "category" : "아파트"
        '''

        url = f"https://apis.zigbang.com/property/apartments/location/v3?geohash={self.geohash}&markerType=large&q=type=sales,price=0~-1,floorArea=0~-1&serviceType[0]=apt&serviceType[1]=offer"
        response = requests.get(url)

        datas = response.json()['filtered']
        df = pd.DataFrame(datas)

        df['register'] = df['item_count'].apply(lambda x:x["register"])
        df['online'] = df['item_count'].apply(lambda x:x["online"])

        df['rent_min'] = df["price"].apply(lambda x:x['rent']['min'])
        df['rent_max'] = df["price"].apply(lambda x:x['rent']['max'])
        df['rent_avg'] = df["price"].apply(lambda x:x['rent']['avg'])

        df['sales_min'] = df["price"].apply(lambda x:x['sales']['min'])
        df['sales_max'] = df["price"].apply(lambda x:x['sales']['max'])
        df['sales_avg'] = df["price"].apply(lambda x:x['sales']['avg'])

        df['offer_min'] = df["price"].apply(lambda x:x['offer']['min'])
        df['offer_max'] = df["price"].apply(lambda x:x['offer']['max'])
        df['offer_avg'] = df["price"].apply(lambda x:x['offer']['avg'])

        df["m2"] = df["floorArea"].apply(lambda x: x['m2'])
        df["p"] = df["floorArea"].apply(lambda x: x['p'])

        df['category'] = "아파트"
        df.drop(columns=["item_count", "price", "floorArea", "marker"], inplace=True)
        zigbang_apt_df = df

        print("end apartment crawling")
        return zigbang_apt_df


    def zigbang_villa(self):
        '''
        SearchData.zigbang_villa(
        self),

        Docstring:
        Crawling zigbang villa data and
        Returning dataframe

        Dataframe
        ----------
        colums : "공급면적_m2", "공급면적_p", "전용면적_m2", "전용면적_p", "lat", "lng", "category",

        "m2" means "m^2"
        "p" means "평"
        "category" : "빌라"
        '''

        url = f"https://apis.zigbang.com/v2/items?domain=zigbang&geohash={self.geohash}&needHasNoFiltered=true&new_villa=true&sales_type_in=매매&zoom=14"
        response = requests.get(url)

        items = response.json()['items']
        ids = [item['item_id'] for item in items]

        url = "https://apis.zigbang.com/v2/items/list"
        params = {"domain":"zigbang","withCoalition": "false", "item_ids": ids}
        response = requests.post(url, params)

        df = pd.DataFrame(response.json()['items'])

        df['공급면적_m2'] = df['공급면적'].apply(lambda x: x['m2'])
        df['공급면적_p'] = df['공급면적'].apply(lambda x: x['p'])
        df['전용면적_m2'] = df['전용면적'].apply(lambda x: x['m2'])
        df['전용면적_p'] = df['전용면적'].apply(lambda x: x['p'])

        df['lat'] = df['random_location'].apply(lambda x: x['lat'])
        df['lng'] = df['random_location'].apply(lambda x: x['lng'])

        df.drop(columns=['공급면적', '전용면적', '계약면적', 'random_location'], inplace=True)
        df['category'] = "빌라"

        zigbang_villa_df = df
        print("end villa crawling")
        return zigbang_villa_df


    def zigbang_officetel(self):
        '''
        SearchData.zigbang_officetel(
        self),

        Docstring:
        Crawling zigbang officetel data and
        Returning dataframe

        Dataframe
        ----------
        colums : "공급면적_m2", "공급면적_p", "전용면적_m2", "전용면적_p", "lat", "lng", "category",

        "m2" means "m^2"
        "p" means "평"
        "category" : "빌라"
        '''
        url = f"https://apis.zigbang.com/v2/officetels?buildings=true&domain=zigbang&geohash={self.geohash}"
        response = requests.get(url)

        ids = []
        for data in response.json()['sections']:
            ids += data['item_ids']

        url_2 = "https://apis.zigbang.com/v2/items/list"
        params = {"domain": "zigbang", "withCoalition": "false", "item_ids": ids}
        response_2 = requests.post(url_2, params)

        df = pd.DataFrame(response_2.json()['items'])

        df['공급면적_m2'] = df['공급면적'].apply(lambda x: x['m2'])
        df['공급면적_p'] = df['공급면적'].apply(lambda x: x['p'])
        df['전용면적_m2'] = df['전용면적'].apply(lambda x: x['m2'])
        df['전용면적_p'] = df['전용면적'].apply(lambda x: x['p'])

        df['lat'] = df['random_location'].apply(lambda x: x['lat'])
        df['lng'] = df['random_location'].apply(lambda x: x['lng'])

        df.drop(columns=['공급면적', '전용면적', '계약면적', 'random_location'], inplace=True)
        df['category'] = '오피스텔'
        zigbang_officetel_df = df

        print("end officetels crawling")
        return zigbang_officetel_df