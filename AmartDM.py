from SQLClient import SQLClient
import os
import json
import requests
from bs4 import BeautifulSoup


class AmartDM:
    def __init__(self, data_dir="data", config_path="config.json"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.url = "https://www.fe-amart.com.tw/index.php/edm"
        self.config_path = config_path
        self.config = self.get_config()

        # self.db = SQLClient(
        #     server=self.config["db"]["server"],
        #     database=self.config["db"]["database"],
        #     user=self.config["db"]["user"],
        #     password=self.config["db"]["password"],
        #     port=self.config["db"]["port"],
        # )

    def get_config(self):
        with open(self.config_path, "r") as f:
            return json.load(f)

    def get_pdf_url(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, "html.parser")
        custom = soup.find("div", {"class": "custom"})
        iframe = custom.find("iframe")
        src = iframe.get("src")
        url = src.replace("preview", "view")
        return url

    def get_pdf_name(self, pdf_url):
        response = requests.get(pdf_url)
        soup = BeautifulSoup(response.text, "html.parser")
        name = soup.find("meta", attrs={"property": "og:title"}).get("content")
        return name

    def save_pdf(self):
        pdf_url = self.get_pdf_url()
        pdf_name = self.get_pdf_name(pdf_url)
        pdf_id = pdf_url.split("/")[-2]
        download_link = (
            f"https://drive.usercontent.google.com/uc?id={pdf_id}&export=download"
        )

        print(f"URL: {pdf_url}")
        print(f"Download Link: {download_link}")
        response = requests.get(download_link)
        pdf_path = os.path.join(self.data_dir, pdf_name)

        with open(pdf_path, "wb") as f:
            f.write(response.content)

        print(f"Saved: {pdf_path}")


if __name__ == "__main__":
    amart_dm = AmartDM()
    amart_dm.save_pdf()
