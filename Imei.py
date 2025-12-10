import os
import requests
from bs4 import BeautifulSoup
import pandas as pd


class Imei:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.setup()

        self.url = "https://imec.imeifoods.com.tw/collections"

        self.categories = {
            "油品調味料｜肉鬆｜抹醬": "oil-seasoning-and-pork-floss-and-jam",
            "保久乳｜豆奶": "milk",
            "果汁茶飲｜沖泡飲品": "juice-tea-and-brewed-beverages",
        }

        self.columns = ["類別", "子類別", "公司", "品牌", "產品", "規格"]

    def setup(self):
        os.makedirs(self.data_dir, exist_ok=True)

        # pd.set_option('display.max_rows', None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", None)

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)

        for category, category_en in self.categories.items():
            url = f"{self.url}/{category_en}/search_products.json?page=1&per=1000"
            print(f"URL: {url}")

            response = requests.get(url)
            products = response.json()["products"]

            for product in products:
                df.loc[len(df)] = [
                    category,
                    category,
                    "義美",
                    "義美",
                    product["title"],
                    "",
                ]

        return df

    def save_csv(self):
        df = self.get_product_df()
        csv_path = os.path.join(self.data_dir, "Imei_product.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"Saved: {csv_path}")


if __name__ == "__main__":
    imei = Imei()
    imei.save_csv()
