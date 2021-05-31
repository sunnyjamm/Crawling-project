import geohash2
import pandas as pd
import requests
import configparser


config = configparser.ConfigParser()
config.read("../datas.ini")
info = config["kakao"]


class SearchDatas:

    def __init__(self, query):
        self.query = query
        self.lat = None
        self.lng = None
        self.geohash = None

        self.zigbang_oneroom_df = None
        self.zigbang_apt_df = None
        self.zigbang_villa_df = None
        self.zigbang_office_df = None

        self.kakao_oneroom_df = None
        self.kakao_apt_df = None
        self.kakao_villa_df = None
        self.kakao_office_df = None


    def get_geohash(self, precision=5, get_loc=False):
        url = f"https://apis.zigbang.com/search?q={self.query}"
        response = requests.get(url)

        lat, lng = response.json()['items'][0]['lat'], response.json()['items'][0]['lng']
        self.geohash = geohash2.encode(lat, lng, precision=precision)
        print("geohash :", self.geohash)

        if get_loc:
            self.lat = lat
            self.lng = lng


    def zigbang_oneroom(self):
        url = f"https://apis.zigbang.com/v2/items?deposit_gteq=0&domain=zigbang&geohash={self.geohash}&rent_gteq=0&sales_type_in=전세|월세&service_type_eq=원룸"
        response = requests.get(url)

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

            df.drop(columns=["random_location", "공급면적", "전용면적"], inplace=True)
            df['category'] = "원룸"
            dfs.append(df)

        self.zigbang_oneroom_df = pd.concat(dfs)
        self.zigbang_oneroom_df.reset_index(drop=True, inplace=True)
        print("end oneroom crawling")


    def zigbang_apt(self):
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

        self.zigbang_apt_df = df
        print("end apartment crawling")


    def zigbang_villa(self):
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

        df.drop(columns=['공급면적', '전용면적', 'random_location'], inplace=True)
        df['category'] = "빌라"

        self.zigbang_villa_df = df
        print("end villa crawling")


    def kakao(self, REST_API_KEY, target, obj):
        headers = {"Authorization": f"KakaoAK {REST_API_KEY}",}

        cate_ls =  ["MT1", "CS2", "PS3", "SC4", "AC5", "PK6", "OL7", "SW8", "BK9", "CT1", "AG2", "PO3", "AT4",
                    "AD5", "FD6", "CE7", "HP8", "PM9"]

        page, size, radius = 1, 15, 200
        dfs_2 = []
        obj[cate_ls] = None
        for idx, loc in enumerate(obj[['lat', 'lng']].values):
            print(idx, end=" ")
            lat, lng = loc
            for category in cate_ls:
                while True:
                    url = f"https://dapi.kakao.com/v2/local/search/category.json?category_group_code={category}&page={page}&size={size}&sort=distance&x={lng}&y={lat}&radius={radius}"
                    response = requests.get(url, headers=headers)

                    datas = response.json()['documents']

                    df = pd.DataFrame(datas)
                    df['매물_lat'] = lat
                    df['매물_lng'] = lng

                    dfs_2.append(df)

                    if response.json()['meta']['is_end']:
                        obj.loc[idx, category] = response.json()['meta']['total_count']
                        break

                    page += 1

        if target == "oneroom":
            self.kakao_oneroom_df = pd.concat(dfs_2).reset_index(drop=True)
        elif target == "apartment":
            self.kakao_apt_df = pd.concat(dfs_2).reset_index(drop=True)
        elif target == "villa":
            self.kakao_villa_df = pd.concat(dfs_2).reset_index(drop=True)
        else:
            self.kakao_office_df = pd.concat(dfs_2).reset_index(drop=True)

        print("end kakao crawling")
