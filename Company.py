import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


class Company:
    def __init__(self, data_dir="data", config_path="mapping.json"):
        self.data_dir = data_dir
        self.config = self.get_config(config_path)
        self.setup()

        self.columns = ["類別", "子類別", "公司", "品牌", "產品", "規格"]

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

    def get_config(self, config_path):
        with open(config_path) as f:
            return json.load(f)

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
