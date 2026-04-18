# projet-big-data

Ce projet porte sur l'analyse des données immobilières dans le département du Rhône.  
Il se compose de plusieurs documents, dont les principaux sont :

## Web scraping
- `scrapper_logic_immo.py`  
- `cloudscrapper_rhone.ipynb`  
Ces scripts ont permis de constituer deux bases de données : `logic_immo_lyon.csv` et `annonces_rhone.csv`
⚠️ Il est inutile de lancer `scrapper_logic_immo.py` : il est très long à exécuter et ne produit d'ailleurs pas de sorties.  

## Nettoyage des données
- `nettoyage_données-notebook.ipynb`  
- `cleaning.ipynb`
- `fusion_bdd`
  - Ces notebooks peuvent être exécutés sans problème et ont donné lieu  a 3 bases de données : `logic_immo_lyon_propre.csv`, `etreproprio_rhone_propre.csv` et `bases_fusionnees.csv`

## Analyse et modélisation
- `data_analyse.ipynb`  
- `NLP.ipynb`  + `wordcount_immo.png`
- `Modele.ipynb`  
  - Scripts dédiés à l’analyse des données et à la création de modèles  
