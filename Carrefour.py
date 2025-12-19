from Company import Company
from SQLClient import SQLClient
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


class Carrefour(Company):
    def __init__(self, data_dir="data", config_path="config.json"):
        super().__init__(data_dir, config_path)
        self.base_url = "https://online.carrefour.com.tw"

        self.categories = [
            "生鮮冷凍/奶蛋點心食品/雞蛋豆製品",
            "生鮮冷凍/奶蛋點心食品/鮮乳調味乳",
            "生鮮冷凍/奶蛋點心食品/豆米漿",
            "生鮮冷凍/奶蛋點心食品/優酪乳優格",
            "生鮮冷凍/奶蛋點心食品/保久乳",
            "生鮮冷凍/奶蛋點心食品/味噌沙拉醬",
            "生鮮冷凍/奶蛋點心食品/甜點小菜冷盤",
            "飲料零食/水汽水/汽水",
            "飲料零食/水汽水/礦泉水",
            "飲料零食/水汽水/氣泡水",
            "飲料零食/茶飲咖啡/綠茶烏龍茶其他茶飲",
            "飲料零食/茶飲咖啡/紅茶花茶水果茶",
            "飲料零食/茶飲咖啡/奶茶",
            "飲料零食/茶飲咖啡/咖啡",
            "飲料零食/茶飲咖啡/機能飲料",
            "飲料零食/茶飲咖啡/冷藏飲品",
            "飲料零食/果汁養生/蔬果汁",
            "米油沖泡/調味罐頭湯品南北貨/罐頭",
            "米油沖泡/米食用油/食用油",
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

        self.db = SQLClient(
            server=self.config["db"]["server"],
            database=self.config["db"]["database"],
            user=self.config["db"]["user"],
            password=self.config["db"]["password"],
            port=self.config["db"]["port"],
        )

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)

        for category in self.categories:
            url = f"{self.base_url}/zh/{category}"
            print(f"URL: {url}")

            while True:
                response = requests.get(url)
                soup = BeautifulSoup(response.text, "html.parser")
                page = soup.find("ul", id="pagination")

                products_tag = soup.find("div", id="productgrid")
                product_tags = products_tag.find_all(
                    "div", class_="hot-recommend-item line"
                )

                for product_tag in product_tags:
                    a = product_tag.find("a", class_="gtm-product-alink")
                    product = a.get("data-name")
                    print(product)
                    price = a.get("data-price")
                    # baseprice = a.get("data-baseprice")
                    brand = a.get("data-brand")
                    spec = a.get("data-variant")
                    df.loc[len(df)] = [
                        None,
                        None,
                        brand,
                        product,
                        spec,
                        price,
                        None,
                        url,
                    ]

                # 取得下一頁
                li = page.find_all("li")[-2]

                if li["class"] == ["disabled"]:
                    break

                params = li.find("a")["onclick"].split("'")[1].split("?")[-1]
                url = f"{self.base_url}/zh/{category}?{params}"
                print(f"URL: {url}")

        df = self.process_product_df(df)
        df = self.filter_product_df(df)
        self.update_config()

        return df

    def mapping_brand_and_manufacturer(self, data_name, data_brand):
        if not data_name:
            return "", ""

        for keyword, b2m in self.config["product2brand2manufacturer"].items():
            if keyword in data_brand or keyword in data_name:
                brand = next(iter(b2m.keys()))

                if brand != data_brand:
                    print(f"{data_brand} -> {brand}")

                manufacturer = b2m[brand]
                return brand, manufacturer

        if data_brand:
            self.config["product2brand2manufacturer"][data_brand] = {
                data_brand: data_brand
            }

            return data_brand, data_brand

        return "", ""

    def process_product_df(self, df):
        # 依產品名稱分類 (category)
        df["CATEGORY"] = df["PRODUCT_NAME"].apply(
            self.mapping, args=("product2category",)
        )

        df["BRAND"] = df["BRAND"].replace("null", "")

        # 依產品名稱取得 brand/ 依 brand 取得 manufacturer
        mask = df["CATEGORY"] != ""

        df.loc[mask, ["BRAND", "MANUFACTURER"]] = (
            df.loc[mask]
            .apply(
                lambda r: self.mapping_brand_and_manufacturer(
                    r["PRODUCT_NAME"], r["BRAND"]
                ),
                axis=1,
                result_type="expand",
            )
            .values
        )

        return df

    def filter_product_df(self, df):
        # 刪除不要的產品
        df = df.loc[df["CATEGORY"] != ""]
        df = df.loc[df["BRAND"] != ""]
        return df

    def get_rows(self):
        df = self.get_product_df()

        # 刪除不要的產品
        df = df.loc[df["CATEGORY"] != ""]

        # 轉成 db 欄位
        df = df.reindex(columns=self.db_columns)

        # 補欄位
        df["CRAWLER_NAME"] = self.__class__.__name__ + ".py"
        df["SOURCE"] = "爬蟲"
        df["RETAILER"] = "WebCrawler_Carrefour"
        df["NEW"] = 0
        df["STATUS"] = ""

        # df 轉可 executemany 的格式
        rows = list(df.itertuples(index=False, name=None))

        # 處理空值
        rows = [tuple(None if pd.isna(x) else x for x in row) for row in rows]
        print(rows[0])

        return rows


if __name__ == "__main__":
    carrefour = Carrefour()
    carrefour.save_csv()
    # carrefour.insert_product()
