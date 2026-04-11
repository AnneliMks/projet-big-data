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

OUTPUT = "pap_idf_avec_description.csv"
MAX_PAGES = 50 

def get_driver():
    options = Options()
    # options.add_argument("--headless=new") # Décommente pour passer en mode invisible
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def clean_text(text):
    if not text: return ""
    # On enlève les retours à la ligne et les virgules pour ne pas décaler le CSV
    return text.replace("\n", " ").replace(",", " ").strip()

# --- MAIN ---
driver = get_driver()
annonces_data = []

try:
    for page in range(1, MAX_PAGES + 1):
        # URL Île-de-France (g439)
        url = "https://www.pap.fr/annonce/vente-appartement-maison-ile-de-france-g439" if page == 1 \
        else f"https://www.pap.fr/annonce/vente-appartement-maison-ile-de-france-g439-{page}"
        
        print(f"Page {page} en cours")
        driver.get(url)
        time.sleep(random.uniform(5, 7)) 

        cartes = driver.find_elements(By.CLASS_NAME, "search-list-item-alt")

        for carte in cartes:
            try:
                # 1. on ne prends pas les annonces ou y a "a partir de"
                prix_brut = carte.find_element(By.CLASS_NAME, "item-price").text
                if "à partir de" in prix_brut.lower():
                    continue 
                
                prix = re.sub(r"[^\d]", "", prix_brut)

                # 2. Titre / Ville
                titre = clean_text(carte.find_element(By.CLASS_NAME, "h1").text)
                
                # 3. Détails (Pièces, Nb chambre, superficies)
                details = clean_text(carte.find_element(By.CLASS_NAME, "item-tags").text)

                # 4. Description (Le texte qui est coupé) 
                description = clean_text(carte.find_element(By.CLASS_NAME, "item-description").text)

                # On garde Paris pour l'instant
                if prix:
                    annonces_data.append({
                        "titre": titre,
                        "prix": prix,
                        "details": details,
                        "description": description
                    })
            except:
                continue

    # --- SAUVEGARDE ---
    if annonces_data:
        with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
            # On rajoute 'description' dans les entêtes
            writer = csv.DictWriter(f, fieldnames=["titre", "prix", "details", "description"])
            writer.writeheader()
            writer.writerows(annonces_data)
        print(f"\n✅ Terminé ! {len(annonces_data)} annonces enregistrées dans {OUTPUT}")

finally:
    driver.quit()