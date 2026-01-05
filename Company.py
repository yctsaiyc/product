import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


class Company:
    def __init__(self, data_dir="data", config_path="mapping.json"):
        self.data_dir = data_dir
        self.config_path = config_path
        self.config = self.get_config()
        self.setup()

        self.columns = ["類別", "子類別", "公司", "品牌", "產品", "規格"]

        self.db = None

        self.db_columns = [
            # "ID",  # Primary key
            "CRAWLER_NAME",
            "REQUEST_URL",
            "SOURCE",
            "RETAILER",
            "PRODUCT_NAME",
            "CATEGORY",
            "MANUFACTURER",
            "BRAND",
            "SPEC",
            "PRICE",
            "PROMO",
            "NEW",
            "STATUS",
            # "CREATE_DATE",  # SQL 自動產生
            # "CREATE_USER_ID",  # SQL 自動產生
        ]

    def setup(self):
        os.makedirs(self.data_dir, exist_ok=True)

        # pd.set_option('display.max_rows', None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", None)

    def get_config(self):
        with open(self.config_path) as f:
            return json.load(f)

    def update_config(self):
        with open(self.config_path, "w") as f:
            json.dump(self.config, f, indent=4, ensure_ascii=False)

        print(f"Updated: {self.config_path}")

    def clean_text(self, text):
        text = (
            text.replace("\n", "").replace("\r", "").replace("\t", "").replace(" ", "")
        )

        return text

    def get_int(self, string):
        m = re.search(r"\d+", string)
        return int(m.group()) if m else None

    def mapping(self, name, mapping_type):
        if not name:
            return ""

        for keyword in self.config[mapping_type]:
            if keyword in name:
                return self.config[mapping_type][keyword]

        return ""

    def mapping_brand_and_manufacturer(self, name):
        if not name:
            return "", ""

        for keyword, b2m in self.config["product2brand2manufacturer"].items():
            if keyword in name:
                brand = next(iter(b2m.keys()))
                manufacturer = b2m[brand]
                return brand, manufacturer

        return "", ""

    def filter_product_df(self, df):
        # 刪除不要的產品
        df = df.loc[df["CATEGORY"] != ""]
        df = df.loc[df["BRAND"] != ""]
        return df

    def to_db_schema(self, df):
        # 轉成 db 欄位
        df = df.reindex(columns=self.db_columns)

        # 補欄位
        df["CRAWLER_NAME"] = self.__class__.__name__ + ".py"
        df["SOURCE"] = "爬蟲"
        df["RETAILER"] = "WebCrawler_" + self.__class__.__name__
        df["STATUS"] = ""

        return df

    def get_rows(self):
        df = self.get_product_df()

        # df 轉可 executemany 的格式
        rows = list(df.itertuples(index=False, name=None))

        # 處理空值
        rows = [tuple(None if pd.isna(x) else x for x in row) for row in rows]
        print(rows[0])

        return rows

    def save_csv(self):
        df = self.get_product_df()
        csv_path = os.path.join(self.data_dir, f"{self.__class__.__name__}_product.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {csv_path}")

    def save_keyword_csv(self):
        keywords = []

        for key in self.config.keys():
            keywords.extend(self.config[key].keys())

        df = pd.DataFrame(keywords)
        df.to_csv("data/keywords.csv", index=False, encoding="utf-8-sig")
        print(f"Saved: data/keywords.csv")

    def insert_product(self):
        rows = self.get_rows()
        table_name = self.config["db_table_name"]

        sql = f"""
            INSERT INTO {table_name} ({', '.join(self.db_columns)})
            VALUES ({', '.join(['%s'] * len(self.db_columns))})
        """

        print(sql)

        self.db.cursor.executemany(sql, rows)
        self.db.conn.commit()

    def get_url_set(self):
        table_name = self.config["db_table_name"]
        table = self.db.select_all(table_name)
        df = pd.DataFrame(table)
        url_set = set(df["REQUEST_URL"])

        return url_set
