import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd


class Company:
    def __init__(self, data_dir="data", config_path="mapping.json"):
        self.data_dir = data_dir
        self.config = self.get_config(config_path)
        self.setup()

        self.columns = ["類別", "子類別", "公司", "品牌", "產品", "規格"]

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

    def mapping(self, name, mapping_type):
        if not name:
            return ""

        for keyword in self.config[mapping_type]:
            if keyword in name:
                return self.config[mapping_type][keyword]

        return ""

    def save_csv(self):
        df = self.get_product_df()
        csv_path = os.path.join(self.data_dir, f"{self.__class__.__name__}_product.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {csv_path}")
