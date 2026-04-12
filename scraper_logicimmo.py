import time
import random
import csv
import re
import os # Ajouté pour vérifier si le fichier existe
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

OUTPUT = "logic_immo_lyon.csv"
BASE_URL = "https://www.logic-immo.com/classified-search?distributionTypes=Buy&estateTypes=House,Apartment&locations=AD06FR70"
FIELDNAMES = ["prix", "type_de_bien", "details", "zone"]

def get_driver():
    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_text(carte, classe, keep_comma=False):
    els = carte.find_elements(By.CLASS_NAME, classe)
    if not els:
        return ""
    text = els[0].text.replace("\n", " ").strip()
    return text if keep_comma else text.replace(",", " ")

# --- INITIALISATION DU CSV (Ecriture du Header) ---
with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()

driver = get_driver()
wait = WebDriverWait(driver, 15)
total_sauvegarde = 0
page = 1

try:
    while True:
        url = BASE_URL if page == 1 else f"{BASE_URL}&page={page}"
        print(f"\nPage {page}")
        driver.get(url)

        if page == 1:
            try:
                btn_cookies = wait.until(EC.element_to_be_clickable((By.ID, "didomi-notice-disagree-button")))
                btn_cookies.click()
                print("  Cookies refusés")
                time.sleep(2)
            except:
                pass

        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "css-wb0bqw")))
        except:
            print("Page vide ou erreur de chargement → fin.")
            break

        cartes = driver.find_elements(By.CLASS_NAME, "css-wb0bqw")
        page_data = [] # On stocke temporairement les données de LA page

        for carte in cartes:
            prix_brut = get_text(carte, "css-1ilgral")
            match = re.search(r"([\d\s]+)€", prix_brut)
            prix = re.sub(r"\s", "", match.group(1)) if match else ""
            if not prix:
                continue

            annonce = {
                "prix":         prix,
                "type_de_bien": get_text(carte, "css-jeruic"),
                "details":      get_text(carte, "css-1okpm0e", keep_comma=True) or get_text(carte, "css-1mqggk6", keep_comma=True),
                "zone":         get_text(carte, "css-1bola4h"),
            }
            page_data.append(annonce)
        

        # --- SAUVEGARDE IMMEDIATE DE LA PAGE ---
        if page_data:
            with open(OUTPUT, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                writer.writerows(page_data)
            total_sauvegarde += len(page_data)
            print(f"  {len(page_data)} annonces ajoutées au CSV (Total: {total_sauvegarde})")

        page += 1
        time.sleep(random.uniform(4, 8)) 

finally:
    driver.quit()
    print(f"\nScraping terminé. Fichier final : {OUTPUT}")