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
            # "▲餅乾飲料-保久乳｜乳酸飲料-調味乳",
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

        self.url_set = set()  # self.get_url_set()

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

    def get_spec(self, product_name, product_url):
        # 1. 從產品名稱找
        # (\d+(?:\.\d+)?) -> 匹配整數或小數 (如 200 或 1.5)
        # \s* -> 允許數字與單位間有空格
        # (ml|l|g|kg) -> 匹配指定的幾種單位
        pattern = r"(\d+(?:\.\d+)?)\s*(ml|l|g|kg)"

        match = re.search(pattern, product_name, re.IGNORECASE)

        if match:
            return match.group(0)

        # 2. 從商品說明找
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

        return None

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)
        category_dict = self.get_category_dict()

        for category in self.categories:
            # 產品代碼
            code = category_dict.get(category)

            if not code:
                print(f"Category {category} not found.")
                continue

            # 組合產品網址
            url = f"{self.base_url}/category/{code}"

            while url:
                print(f"URL: {url}")
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.text, "html.parser")

                # 產品標籤
                products_tag = soup.find("div", class_="GtCRmcqQdDRWor6dmJiw")
                product_tags = products_tag.find_all(
                    "div",
                    class_="_6xzpdbl0 _6xzpdb8 _6xzpdbm _6xzpdbg _6xzpdb1u _6xzpdbb _6xzpdbck _6xzpdbch",
                )
                self.print_elapsed_time()

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
                        print("Duplicate product:", name(product_url))
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

                # 取得下一頁
                next_page_tag = soup.find("link", rel="next") or {}
                url = next_page_tag.get("href")

                # 計時
                self.print_elapsed_time()

        df = self.process_product_df(df)
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
