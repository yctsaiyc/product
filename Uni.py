from Company import Company
import requests
from bs4 import BeautifulSoup
import pandas as pd


class Uni(Company):
    def __init__(self, data_dir="data", config_path="mapping.json"):
        super().__init__(data_dir, config_path)
        self.brand_url = "https://www.pecos.com.tw/brands.html"
        self.brand_columns = ["類別", "品牌", "URL"]

    def get_category_map(self, soup):
        categories_tag = soup.find("ul", {"class": "nav nav-tabs products"})
        category_tags = categories_tag.find_all("a")
        category_map = {}

        for category_tag in category_tags:
            code = category_tag.get("data-rel")

            if not code:
                continue

            name = category_tag.text
            category_map[code] = name

        return category_map

    def get_brand_df(self):
        print(f"URL: {self.brand_url}")
        response = requests.get(self.brand_url)

        soup = BeautifulSoup(response.text, "html.parser")
        bd = soup.find("div", {"class": "bd"})
        brands_tag = bd.find("div", {"class": "brand-list-xs brand-box-xs"})
        brand_tags = brands_tag.find_all("div", recursive=False)

        category_map = self.get_category_map(soup)
        brand_dict = {}

        for brand_tag in brand_tags:
            category = category_map[brand_tag["class"][-1]]
            name = brand_tag.find("img")["alt"]
            brand_dict[name] = category

        brand_df = pd.DataFrame(list(brand_dict.items()))
        brand_df.columns = ["品牌", "類別"]
        brand_df = brand_df.reindex(columns=self.brand_columns)

        brand_df["URL"] = (
            "https://www.pecos.com.tw/brands-" + brand_df["品牌"] + ".html"
        )

        return brand_df

    def get_spec(self, product_id):
        url = "https://www.pecos.com.tw/api.html"

        payload = {
            "action": "GetProductData",
            "pdID": product_id,
        }

        response = requests.post(url, data=payload)
        html = response.json()["html"]
        soup = BeautifulSoup(html, "html.parser")
        spec = soup.find("span").text
        return spec

    def get_product_df(self):
        brand_df = self.get_brand_df()
        df = pd.DataFrame(columns=self.columns)

        for idx, row in brand_df.iterrows():
            url = row["URL"]
            print(f"URL: {url}")

            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            products_tag = soup.find("div", {"class": "carousel product-list"})
            product_tags = products_tag.find_all("div", {"class": "product-box"})

            for product_tag in product_tags:
                a = product_tag.find("a")
                product_id = a.get("data-rel")
                name = a.get("data-name")

                df.loc[len(df)] = [
                    row["類別"],
                    row["類別"],
                    "統一",
                    row["品牌"],
                    name,
                    self.get_spec(product_id),
                ]

        return df


if __name__ == "__main__":
    uni = Uni()
    uni.save_csv()
