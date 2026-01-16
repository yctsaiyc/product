from Company import Company
from SQLClient import SQLClient
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import json


class AmartRakuten(Company):
    def __init__(self, data_dir="data", config_path="config.json"):
        super().__init__(data_dir, config_path)
        self.base_url = "https://www.rakuten.com.tw/shop/amart"

        # self.categories = []
        self.categories = self.get_category_dict().keys()

        self.columns = [
            "CATEGORY",
            "MANUFACTURER",
            "BRAND",
            "PRODUCT_NAME",
            "SPEC",
            "PRICE",
            "PROMO",
            "REQUEST_URL",
        ]

        # self.db = SQLClient(
        #     server=self.config["db"]["server"],
        #     database=self.config["db"]["database"],
        #     user=self.config["db"]["user"],
        #     password=self.config["db"]["password"],
        #     port=self.config["db"]["port"],
        # )

        self.url_set = set()  # self.get_url_set()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        self.start = time.time()

    def get_category_dict(self):
        return {
            # "▲生鮮冷藏-熱銷飲品大賞": "ba54",
            # "▲生鮮冷藏-鮮奶 HOT": "ba17",
            # "▲生鮮冷藏-調味奶": "ba18",
            # "▲生鮮冷藏-豆米漿": "ba19",
            # "▲生鮮冷藏-優酪乳": "ba20",
            # "▲生鮮冷藏-發酵乳": "ba21",
            # "▲生鮮冷藏-果汁": "ba24",
            # "▲生鮮冷藏-茶飲": "ba25",
            # "▲生鮮冷藏-咖啡": "ba26",
            # "▲生鮮冷藏-穀物飲品": "ba27",
            # "▲生鮮冷藏-其他飲品": "ba28",
            # "▲生鮮冷藏-優格-布丁": "ba30",
            # "▲生鮮冷藏-茶凍-豆花-奶酪": "ba31",
            # "▲生鮮冷藏-沙拉-味噌-芥末": "ba37",
            # "▲米油沖泡-食用油": "ca10",
            "▲米油沖泡-食用油-沙拉油": "ca10a00",
            "▲米油沖泡-食用油-葵花油": "ca10a01",
            "▲米油沖泡-食用油-蔬菜油、芥花油": "ca10a02",
            "▲米油沖泡-食用油-調合油": "ca10a03",
            "▲米油沖泡-食用油-橄欖油": "ca10a04",
            "▲米油沖泡-食用油-葡萄籽、玄米油": "ca10a05",
            "▲米油沖泡-食用油-苦茶油、椰子油": "ca10a06",
            "▲米油沖泡-食用油-黑麻油": "ca10a07",
            # "▲米油沖泡-醬油│油膏": "ca27",
            "▲米油沖泡-醬油│油膏-醬油": "ca27a00",
            "▲米油沖泡-醬油│油膏-油膏、蔭油": "ca27a01",
            "▲米油沖泡-醬油│油膏-蠔油、魚露": "ca27a02",
            "▲米油沖泡-糖│鹽│調味品-味精、雞精粉": "ca28a01",
            "▲米油沖泡-糖│鹽│調味品-胡椒粉、香料、滷包": "ca28a02",
            "▲米油沖泡-糖│鹽│調味品-香油、麻油、料理醋": "ca28a03",
            "▲米油沖泡-糖│鹽│調味品-沙拉醬": "ca28a04",
            "▲米油沖泡-沾醬│拌醬│咖哩-沙茶醬": "ca29a00",
            "▲米油沖泡-沾醬│拌醬│咖哩-辣椒醬、豆瓣醬": "ca29a01",
            "▲米油沖泡-沾醬│拌醬│咖哩-蕃茄醬、甜辣醬": "ca29a02",
            "▲米油沖泡-沾醬│拌醬│咖哩-烤肉醬、燒肉醬": "ca29a03",
            "▲米油沖泡-沾醬│拌醬│咖哩-水餃醬、牛排醬、特色沾醬": "ca29a04",
            "▲米油沖泡-沾醬│拌醬│咖哩-調味拌醬": "ca29a05",
            "▲米油沖泡-沾醬│拌醬│咖哩-義大利麵醬": "ca29a06",
            "▲米油沖泡-沾醬│拌醬│咖哩-肉醬、炸醬": "ca29a08",
            "▲米油沖泡-奶油│煉乳-花生醬、巧克力醬、大蒜醬": "ca30a00",
            "▲米油沖泡-奶油│煉乳-煉乳、奶油、奶酥": "ca30a01",
            "▲餅乾飲料-果凍｜蒟蒻｜布丁-果凍｜蒟蒻": "da05a00",
            "▲餅乾飲料-果凍｜蒟蒻｜布丁-布丁": "da05a01",
            "▲餅乾飲料-果汁｜果醋-單一果汁": "da23a00",
            "▲餅乾飲料-果汁｜果醋-綜合果汁": "da23a01",
            "▲餅乾飲料-果汁｜果醋-蔬果汁": "da23a02",
            "▲餅乾飲料-果汁｜果醋-100%果汁": "da23a03",
            "▲餅乾飲料-果汁｜果醋-濃縮果汁": "da23a04",
            "▲餅乾飲料-果汁｜果醋-進口果汁": "da23a05",
        }

        url = "https://www.rakuten.com.tw/api/shop_category/?ajax=true&shop_url=amart"
        print(f"URL: {url}")

        response = requests.get(url, headers=self.headers)
        categories = response.json().get("categories", [])
        category_dict = {}

        def extract_categories(category_list, parent_name=""):
            for item in category_list:
                name = item["displayName"]["zh_TW"]
                name = f"{parent_name}-{name}" if parent_name else name
                category_dict[name] = item["shopCategoryKey"]
                children = item.get("children", [])

                if children:
                    extract_categories(children, name)

        extract_categories(categories)

        # with open("category_dict.json", "w") as f:
        #     json.dump(category_dict, f, indent=4, ensure_ascii=False)
        #     print(f"Saved: category_dict.json")

        return category_dict

    def get_spec(self, product_name, product_url):
        # 1. 從產品名稱找
        # (\d+(?:\.\d+)?) -> 匹配整數或小數 (如 200 或 1.5)
        # \s* -> 允許數字與單位間有空格
        # (ml|l|g|kg) -> 匹配指定的幾種單位
        pattern = r"(\d+(?:\.\d+)?)\s*(ml|l|g|kg)"

        match = re.search(pattern, product_name, re.IGNORECASE)

        if match:
            return match.group(0).lower()

        return None

        # 2. 從商品說明找 (每個產品格式不同，故先不使用)
        print("URL:", product_url)
        response = requests.get(product_url, headers=self.headers)
        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", {"id": "item-detail"})
        table = div.find("table")

        if not table:
            return None

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")

            if "內容量" in td[0].text:
                return td.text

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)
        category_dict = self.get_category_dict()

        for category in self.categories:
            print(category)

            # 產品代碼
            code = category_dict.get(category)

            if not code:
                print(f"Category {category} not found.")
                continue

            # 組合產品網址
            url = f"{self.base_url}/category/{code}"

            while url:
                print(f"URL: {url}")

                # 取得 html
                response = requests.get(url, headers=self.headers)

                # 檢查是否有產品
                if "未有任何商品相符。" in response.text:
                    print("No product found.")
                    self.print_elapsed_time()
                    break

                # 解析 html
                soup = BeautifulSoup(response.text, "html.parser")

                # 產品標籤
                products_tag = soup.find("div", class_="GtCRmcqQdDRWor6dmJiw")
                product_tags = products_tag.find_all(
                    "div",
                    class_="_6xzpdbl0 _6xzpdb8 _6xzpdbm _6xzpdbg _6xzpdb1u _6xzpdbb _6xzpdbck _6xzpdbch",
                )

                for product_tag in product_tags:
                    # 產品網址
                    product_url_tag = product_tag.find(
                        "a", class_="_6xzpdbys _6xzpdb8 _6xzpdbho"
                    )
                    product_url = product_url_tag.get("href")

                    # 產品名稱
                    name_tag = product_tag.find("div", class_="hs2xhh")
                    name = name_tag.find("span").text.strip()
                    name = name.replace("【愛買】", "")

                    # 檢查產品是否重複
                    if product_url in self.url_set:
                        print(f"Duplicate product: {name} ({product_url})")
                        continue

                    # 價格
                    price_tag = product_tag.find(
                        "div",
                        class_="_1md3074m _6xzpdbhq _6xzpdbi5 _6xzpdbyh _6xzpdbew _1md30741b _6xzpdbh3 _6xzpdbhe _6xzpdbk4 qa-productcard-product-price",
                    )
                    price = price_tag.text

                    # 其他資訊
                    detail_tag = product_tag.find(
                        "div",
                        class_="_6xzpdbha _6xzpdba8 eb8n8r1 _6xzpdbhd ds-price-prefix",
                    )
                    detail = detail_tag.text if detail_tag else ""

                    # 規格
                    spec = self.get_spec(name, product_url)

                    print(name, spec, price, detail, product_url)

                    # 加入資料表
                    df.loc[len(df)] = [
                        None,
                        None,
                        None,
                        name,
                        spec,
                        price,
                        detail,
                        product_url,
                    ]

                    self.url_set.add(product_url)

                # 取得下一頁
                next_page_tag = soup.find("link", rel="next") or {}
                url = next_page_tag.get("href")

                # 計時
                self.print_elapsed_time()

        # 填入原始網頁缺少的欄位
        df = self.process_product_df(df)

        # 去除不要的產品
        # df = self.filter_product_df(df)
        # df = self.to_db_schema(df)
        # self.update_config()

        return df

    def process_product_df(self, df):
        # 填入類別/ 品牌/ 製造商
        for idx, row in df.iterrows():
            name = row["PRODUCT_NAME"]
            df.loc[idx, "CATEGORY"] = self.mapping(name, "product2category")
            df.loc[idx, "BRAND"], df.loc[idx, "MANUFACTURER"] = (
                self.mapping_brand_and_manufacturer(name)
            )

        return df

    def print_elapsed_time(self):
        elapsed = time.time() - self.start
        print(f"Elapsed time {time.strftime('%M:%S', time.gmtime(elapsed))}")


if __name__ == "__main__":
    amart_rakuten = AmartRakuten()
    amart_rakuten.save_csv()
    # amart_rakuten.insert_product()
