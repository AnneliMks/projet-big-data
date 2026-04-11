#Chercher les données sur l'API DVF

# %%
import requests
from pyspark.sql import Row, SparkSession

# Initialisation Spark
spark = SparkSession.builder.appName("Projet_Immo_Lyon").getOrCreate()

def get_dvf_etalab():
    # Liste des codes INSEE Lyon
    codes_insee = [69381, 69382, 69383, 69384, 69385, 69386, 69387, 69388, 69389]
    collecte = []

    for code in codes_insee: 
        print(f"========== Récupération INSEE {code} (Etalab) ==========")
        
        # URL de l'API officielle
        url = f"https://dvf-api.etalab.gouv.fr/api/mutations/?code_commune={code}"
        
        try:
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                # Ici, on cherche la clé 'results'
                if 'results' in data and data['results']:
                    for mutation in data['results']:
                        # On transforme directement le dictionnaire en Row Spark
                        collecte.append(Row(**mutation))
                    print(f"Succès : {len(data['results'])} lignes récupérées.")
                else:
                    print(f"Pas de résultats pour le code {code}")
            else:
                print(f"Erreur API : {response.status_code}")
                    
        except Exception as e:
            print(f"Erreur de connexion : {e}")

    return collecte

# --- EXECUTION ---
donnees_list = get_dvf_etalab()

if donnees_list:
    df_spark = spark.createDataFrame(donnees_list)
    print("\nDataFrame créé avec succès !")
    df_spark.select("valeur_fonciere", "type_local", "surface_reelle_bati").show(5)
else:
    print("La collecte est vide, vérifie ta connexion.")

            