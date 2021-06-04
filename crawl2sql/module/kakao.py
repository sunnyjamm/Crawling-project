import requests
import pandas as pd


def kakao(REST_API_KEY, target):

    '''
    kakao(
    REST_API_KEY,
    obj
    ),

    Docstring:
    USE KAKAO API CRAWLING DATA
    return kakao_api_dataframe

    REST_API_KEY: kakao api key,
    target : target df 
   
    Examples
    --------
    kakao(REST_API_KEY, zigbang_villa_df)
    return kakao_villa_df
    '''

    headers = {"Authorization": f"KakaoAK {REST_API_KEY}",}

    cate_ls =  ["MT1", "CS2", "PS3", "SC4", "AC5", "PK6", "OL7", "SW8", "BK9", "CT1", "AG2", "PO3", "AT4",
                "AD5", "FD6", "CE7", "HP8", "PM9"]

    page, size, radius = 1, 15, 200
    dfs_2 = []
    target[cate_ls] = None
    for idx, loc in enumerate(target[['lat', 'lng']].values):
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
                    target.loc[idx, category] = response.json()['meta']['total_count']
                    break

                page += 1
    
    data = pd.concat(dfs_2).reset_index(drop=True)
    print("end kakao crawling")
    return data
