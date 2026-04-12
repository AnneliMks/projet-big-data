import time
import random
import csv
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# --- CONFIG ---
#OUTPUT = "pap_lyon_direct2.csv"
#MAX_PAGES = 70 # Tu peux monter à 15 ou 20

def get_driver():
    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 ...")
    # options.add_argument("--headless=new") # Décommente pour ne plus voir la fenêtre
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def clean_text(text):
    if not text: return ""
    # Enlève les sauts de ligne et les virgules pour ne pas casser le CSV
    return text.replace("\n", " ").replace(",", " ").strip()

# --- MAIN ---
# --- CONFIG ---
OUTPUT = "pap_lyon_complet.csv"
page = 1
continuer = True
annonces_data = []

driver = get_driver()

try:
    while continuer:
        url = "https://www.pap.fr/annonce/vente-appartement-maison-rhone-69-g433" if page == 1 \
        else f"https://www.pap.fr/annonce/vente-appartement-maison-rhone-69-g433-{page}"
        
        print(f"Scraping Page {page}...")
        driver.get(url)
        time.sleep(random.uniform(4, 6))

        # On cherche les cartes
        cartes = driver.find_elements(By.CLASS_NAME, "search-list-item-alt")

        # --- CONDITION D'ARRÊT ---
        if not cartes: 
            print("Fin des pages détectée (plus d'annonces).")
            continuer = False
            break

        for carte in cartes:
            try:
                # Ton extraction habituelle
                prix_brut = carte.find_element(By.CLASS_NAME, "item-price").text
                if "à partir de" in prix_brut.lower(): continue
                
                prix = re.sub(r"[^\d]", "", prix_brut)
                titre = clean_text(carte.find_element(By.CLASS_NAME, "h1").text)
                details = clean_text(carte.find_element(By.CLASS_NAME, "item-tags").text)
                description = clean_text(carte.find_element(By.CLASS_NAME, "item-description").text)

                if prix:
                    annonces_data.append({
                        "titre": titre,
                        "prix": prix,
                        "details": details,
                        "description": description
                    })
            except:
                continue
        
        print(f"Total actuel : {len(annonces_data)} annonces")
        page += 1 # On passe à la page suivante

    # --- SAUVEGARDE ---
    if annonces_data:
        with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["titre", "prix", "details", "description"])
            writer.writeheader()
            writer.writerows(annonces_data)
        print(f"\n✅ Terminé ! Fichier sauvegardé.")

finally:
    driver.quit()