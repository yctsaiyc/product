from Company import Company
import requests
import pandas as pd


class Imei(Company):
    def __init__(self, data_dir="data", config_path="mapping.json"):
        super().__init__(data_dir, config_path)

        self.url = "https://imec.imeifoods.com.tw/collections"

        self.categories = {
            "油品調味料｜肉鬆｜抹醬": "oil-seasoning-and-pork-floss-and-jam",
            "保久乳｜豆奶": "milk",
            "果汁茶飲｜沖泡飲品": "juice-tea-and-brewed-beverages",
        }

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


if __name__ == "__main__":
    imei = Imei()
    imei.save_csv()
