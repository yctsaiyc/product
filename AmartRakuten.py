from Company import Company
from SQLClient import SQLClient
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time


class AmartRakuten(Company):
    def __init__(self, data_dir="data", config_path="config.json"):
        super().__init__(data_dir, config_path)
        self.base_url = "https://www.rakuten.com.tw/shop/amart"

        self.categories = [
            "▲餅乾飲料-保久乳｜乳酸飲料-原味乳",
            "▲餅乾飲料-保久乳｜乳酸飲料-調味乳",
            # "▲餅乾飲料-保久乳｜乳酸飲料-乳酸飲料",
            # "▲餅乾飲料-保久乳｜乳酸飲料-燕麥奶",
        ]

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

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        self.start = time.time()

    def get_category_dict(self):
        return {
            "▲餅乾飲料-保久乳｜乳酸飲料-原味乳": "da28a00",
            "▲餅乾飲料-保久乳｜乳酸飲料-調味乳": "da28a01",
            "▲餅乾飲料-保久乳｜乳酸飲料-乳酸飲料": "da28a02",
            "▲餅乾飲料-保久乳｜乳酸飲料-燕麥奶": "da28a03",
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

        return category_dict

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)
        category_dict = self.get_category_dict()

        for category in self.categories:
            code = category_dict.get(category)

            if not code:
                print(f"Category {category} not found.")
                continue

            url = f"{self.base_url}/category/{code}"

            while url:
                print(f"URL: {url}")
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.text, "html.parser")

                products_tag = soup.find("div", class_="GtCRmcqQdDRWor6dmJiw")
                product_tags = products_tag.find_all(
                    "div",
                    class_="_6xzpdbl0 _6xzpdb8 _6xzpdbm _6xzpdbg _6xzpdb1u _6xzpdbb _6xzpdbck _6xzpdbch",
                )

                for product_tag in product_tags:
                    product_url_tag = product_tag.find(
                        "a", class_="_6xzpdbys _6xzpdb8 _6xzpdbho"
                    )
                    product_url = product_url_tag.get("href")

                    name_tag = product_tag.find("div", class_="hs2xhh")
                    name = name_tag.find("span").text.strip()
                    name = name.replace("【愛買】", "")

                    price_tag = product_tag.find(
                        "div",
                        class_="_1md3074m _6xzpdbhq _6xzpdbi5 _6xzpdbyh _6xzpdbew _1md30741b _6xzpdbh3 _6xzpdbhe _6xzpdbk4 qa-productcard-product-price",
                    )
                    price = price_tag.text

                    detail_tag = product_tag.find(
                        "div",
                        class_="_6xzpdbha _6xzpdba8 eb8n8r1 _6xzpdbhd ds-price-prefix",
                    )
                    detail = detail_tag.text

                    print(name, price, detail, product_url)

                    # {TODO}
                    brand = None
                    spec = None

                    df.loc[len(df)] = [
                        None,
                        None,
                        brand,
                        name,
                        spec,
                        price,
                        None,
                        product_url,
                    ]

                # 取得下一頁
                next_page_tag = soup.find("link", rel="next") or {}
                url = next_page_tag.get("href")

                # 計時
                elapsed = time.time() - self.start
                print(f"Elapsed time {time.strftime('%M:%S', time.gmtime(elapsed))}")

        df = self.process_product_df(df)
        # df = self.filter_product_df(df)
        # df = self.to_db_schema(df)
        # self.update_config()

        return df

    def process_product_df(self, df):
        # {TODO}
        return df

    def filter_product_df(self, df):
        # 刪除不要的產品
        df = df.loc[df["CATEGORY"] != ""]
        df = df.loc[df["BRAND"] != ""]
        return df

    def get_rows(self):
        df = self.get_product_df()

        # df 轉可 executemany 的格式
        rows = list(df.itertuples(index=False, name=None))

        # 處理空值
        rows = [tuple(None if pd.isna(x) else x for x in row) for row in rows]
        print(rows[0])

        return rows


if __name__ == "__main__":
    amart_rakuten = AmartRakuten()
    amart_rakuten.save_csv()
    # amart_rakuten.insert_product()
