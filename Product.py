from WC import WC
from Uni import Uni
from Imei import Imei
from KC import KC
import pandas as pd


if __name__ == "__main__":
    wc = WC()
    uni = Uni()
    imei = Imei()

    df = pd.concat(
        [
            wc.get_product_df(),
            uni.get_product_df(),
            imei.get_product_df(),
        ],
        axis=0,
    )

    csv_path = "data/product.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"Saved: {csv_path}")
