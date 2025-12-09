import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib3


class WC:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.brand_url = "https://www.weichuan.com.tw/tw/product/brand"
        self.setup()

        self.columns = ["類別", "子類別", "公司", "品牌", "產品", "規格"]

    def setup(self):
        os.makedirs(self.data_dir, exist_ok=True)

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # pd.set_option('display.max_rows', None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", None)

    def get_category_list(self):
        url = "https://www.weichuan.com.tw/api/brand-category-list"
        print(f"URL: {url}")
        response = requests.get(url, verify=False)
        return response.json()["data"]

    def get_category_df(self):
        category_list = self.get_category_list()
        df = pd.DataFrame(category_list)
        df = df[["name_zh"]]  # , "brand_category_id"]]
        df.columns = ["類別"]
        return df

    def get_brand_list(self):
        url = "https://www.weichuan.com.tw/api/brand-list-by-all-sort"
        print(f"URL: {url}")
        response = requests.get(url, verify=False)
        return response.json()["data"]

    def get_brand_df(self):
        brand_list = self.get_brand_list()
        df = pd.DataFrame(brand_list)
        df = df[["brand_category_name_zh", "name_zh", "brand_id"]]
        df.columns = ["類別", "品牌", "URL"]

        df["URL"] = (
            "https://www.weichuan.com.tw/api/product-list-by-brand-id/" + df["URL"]
        )

        return df

    def get_product_df(self):
        brand_df = self.get_brand_df()
        category_df = self.get_category_df()
        brand_df = pd.merge(category_df, brand_df, how="left", on="類別")
        product_df = pd.DataFrame()

        for idx, row in brand_df.iterrows():
            url = row["URL"]
            print(f"URL: {url}")
            response = requests.get(url, verify=False)
            df = pd.DataFrame(response.json()["data"])
            df = df[["brand_sub_category_name_zh", "name_zh", "capacity"]]
            df.columns = ["子類別", "產品", "規格"]
            df["類別"] = row["類別"]
            df["公司"] = "味全"
            df["品牌"] = row["品牌"]
            df = df.reindex(columns=self.columns)

            if product_df.empty:
                product_df = df
                continue

            product_df = pd.concat([product_df, df])

        return product_df

    def save_csv(self):
        df = self.get_product_df()
        csv_path = os.path.join(self.data_dir, "WC_product.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {csv_path}")


if __name__ == "__main__":
    wc = WC()
    wc.save_csv()
