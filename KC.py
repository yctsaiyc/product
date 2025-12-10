from Company import Company
import requests
from bs4 import BeautifulSoup
import pandas as pd


class KC(Company):
    def __init__(self, data_dir="data", config_path="mapping.json"):
        super().__init__(data_dir, config_path)
        self.base_url = "https://www.kuangchuan.com"
        self.brand_url = f"{self.base_url}/Product"

    def get_brand_df(self):
        df = pd.DataFrame(columns=["品牌", "URL"])
        print(f"URL: {self.brand_url}")
        response = requests.get(self.brand_url)
        soup = BeautifulSoup(response.text, "html.parser")
        brands_tag = soup.find("div", {"class": "row mt-3 g-3 pb-5"})
        brand_tags = brands_tag.find_all("div", {"class": "kc-brand"})

        for brand_tag in brand_tags:
            name = brand_tag.find("h1").text
            href = brand_tag.find("a")["href"]
            url = f"{self.base_url}{href}"
            df.loc[len(df)] = [name, url]

        return df

    def get_product_df(self):
        df = pd.DataFrame(columns=self.columns)
        brand_df = self.get_brand_df()

        for idx, row in brand_df.iterrows():
            url = row["URL"]
            print(f"URL: {url}")
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            products_tag = soup.find("div", {"class": "kc-product-items"})
            product_tags = products_tag.find_all(
                "div", {"class": "col-6 col-lg-3 col-md-4"}
            )

            for product_tag in product_tags:
                name = product_tag.find("h1").text
                details = product_tag.find_all("div", {"class": "mb-3"})
                # detail = "\n".join([self.clean_text(detail.text) for detail in details])
                spec = self.clean_text(details[1].text).replace("容量：", "")

                subcategory = self.mapping(name, "product2subcategory")
                category = self.mapping(name, "subcategory2category")

                df.loc[len(df)] = [
                    category,
                    subcategory,
                    "光泉",
                    row["品牌"],
                    name,
                    spec,  # detail,
                ]

        return df


if __name__ == "__main__":
    kc = KC()
    kc.save_csv()
