from Company import Company
from SQLClient import SQLClient
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


class Pxmart(Company):
    def __init__(self, data_dir="data", config_path="config.json"):
        super().__init__(data_dir, config_path)
        self.base_url = "https://www.pxmart.com.tw/campaign/life-will/classification"

        self.classifications = [
            "冷藏、冰凍食品",  # #冷藏乳製品",
            "南北貨、油品、調味料、罐頭",  # #罐頭食品",
            "飲料與酒類",
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

        for classification in self.classifications:
            url = f"{self.base_url}/{classification}"
            print(f"URL: {url}")

            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            product_tags = soup.find_all(
                "div",
                class_="Card_card-container__OxmEq Card_card-container--product__Q_BJW",
            )

            for product_tag in product_tags:
                title = product_tag.find("h5").text
                promo = product_tag.find("ul", class_="Card_card-list__54xpV").text

                price = product_tag.find(
                    "div", class_="Card_card-priceContainer__zUES_ margin-b-3"
                ).text

                df.loc[len(df)] = [None, None, None, title, None, price, promo, url]

        df = self.process_product_df(df)

        return df

    def process_product_df(self, df):
        # 依產品名稱分類 (category)
        df["CATEGORY"] = df["PRODUCT_NAME"].apply(
            self.mapping, args=("product2category",)
        )

        # 依產品名稱取得 brand/ 依 brand 取得 manufacturer
        mask = df["CATEGORY"] != ""

        df.loc[mask, ["BRAND", "MANUFACTURER"]] = (
            df.loc[mask, "PRODUCT_NAME"]
            .apply(lambda x: pd.Series(self.mapping_brand_and_manufacturer(x)))
            .values
        )

        # 分離產品名稱、規格
        for idx, row in df.iterrows():
            # 處理嵌套括號
            if "))" in row["PRODUCT_NAME"]:
                split = row["PRODUCT_NAME"].split("(", 1)
                df.loc[idx, "PRODUCT_NAME"] = split[0]
                df.loc[idx, "SPEC"] = split[1][:-1]

            else:
                m = re.match(r"^(.*)\(([^()]*)\)$", row["PRODUCT_NAME"])

                if m:
                    df.loc[idx, "PRODUCT_NAME"], df.loc[idx, "SPEC"] = m.groups()

        # 取出價格數字
        df["PRICE"] = df["PRICE"].apply(self.get_int)

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
        df["RETAILER"] = "WebCrawler_Pxmart"
        df["NEW"] = 0
        df["STATUS"] = ""

        # df 轉可 executemany 的格式
        rows = list(df.itertuples(index=False, name=None))

        # 處理空值
        rows = [tuple(None if pd.isna(x) else x for x in row) for row in rows]
        print(rows[0])

        return rows


if __name__ == "__main__":
    pxmart = Pxmart()
    # pxmart.save_csv()
    pxmart.insert_product()
