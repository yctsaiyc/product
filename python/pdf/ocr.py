import cv2
import pytesseract
import os


def img2txt(img_path, lang="chi_tra"):
    img = cv2.imread(img_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    text = pytesseract.image_to_string(gray, lang=lang)

    txt_path = os.path.splitext(img_path)[0] + ".txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"Saved: {txt_path}")


if __name__ == "__main__":
    img2txt("test.png")
