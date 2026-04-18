#Nettoyage de données avec spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re

spark = SparkSession.builder \
    .appName("NettoyageImmoRhone") \
    .master("local[*]").getOrCreate()

print("OK")

df = spark.read.csv("logic_immo_lyon.csv", header=True, inferSchema=True)

df.show()
df.printSchema()

#Nettoyage globale des données

#Enlever les lignes si la ville et le prix sont manquants
df = df.dropna(subset=["prix", "details", "zone"])

#Supression des doublons excates
df = df.dropDuplicates()

#On ne garde que les données du Rhone
df = df.filter(F.col("zone").contains("(69"))
print(f"Nombre de lignes : {df.count()}")

#Création de colonnes

# %%
df = df.withColumn(
    "type_clean",
    F.when(F.lower(F.col("type_de_bien")).contains("duplex"), "appartement")
    .when(
        (F.lower(F.col("type_de_bien")).contains("demeure")) |
        (F.lower(F.col("type_de_bien")).contains("pavillon")),
        "maison"
    )
    .when(F.lower(F.col("type_de_bien")).contains("appartement"), "appartement")
    .when(F.lower(F.col("type_de_bien")).contains("maison"), "maison")
    .otherwise("autre"))

# %%
df = df.withColumn(
    "neuf_flag",
    F.when(F.lower(F.col("type_de_bien")).contains("neuf"), 1).otherwise(0)
)


# %%
df = df.withColumn("parts", F.split(F.col("details"), "·"))

df = df.withColumn(
    "pieces",
    F.regexp_extract(
        F.expr("try_element_at(filter(parts, x -> lower(x) like '%pièce%'), 1)"),
        r"(\d+)",
        1
    ).cast("int")
)

df = df.withColumn(
    "chambres",
    F.regexp_extract(
        F.expr("try_element_at(filter(parts, x -> lower(x) like '%chambre%'), 1)"),
        r"(\d+)",
        1
    ).cast("int")
)

# %%
# 1. On sépare les éléments "m²" de manière sécurisée (renvoie NULL si vide)
df = df.withColumn(
    "item_surface", 
    F.expr("try_element_at(filter(parts, x -> lower(x) like '%m²%' AND lower(x) NOT like '%terrain%'), 1)")
).withColumn(
    "item_terrain", 
    F.expr("try_element_at(filter(parts, x -> lower(x) like '%terrain%'), 1)")
)

# 2. Nettoyage et Cast pour la SURFACE
# On gère les chiffres, la virgule, et on cast proprement
df = df.withColumn(
    "surface",
    F.expr("""
        try_cast(
            regexp_replace(
                regexp_replace(lower(item_surface), '[^0-9,]', ''), 
                ',', '.'
            ) as float
        )
    """)
)

# 3. Nettoyage et Cast pour le TERRAIN
#df = df.withColumn(
#    "terrain",
#    F.expr("""
#        try_cast(
#            regexp_replace(
#                regexp_replace(lower(item_terrain), '[^0-9,]', ''), 
#                ',', '.'
#            ) as float
#        )
#    """)
#)

df = df.withColumn(
    "terrain",
    F.when(F.col("type_clean") == "appartement", 0.0) # Si appartement, direct 0
    .otherwise(
        F.expr("""
            try_cast(
                regexp_replace(
                    regexp_replace(lower(item_terrain), '[^0-9,]', ''), 
                    ',', '.'
                ) as float
            )
        """)
    ))

# 4. Suppression des colonnes temporaires
df = df.drop("item_surface", "item_terrain")


df = df.withColumn(
    "etage_raw",
    F.when(
        F.col("type_clean") == "appartement",
        F.regexp_extract(
            F.col("details"), 
            r"(?i)(RDC|(\d+)(?:er|ème)|(?:Étage|Etage)\s+(\d+))", 
            0
        )
    ).otherwise(None)
)

# 2. On nettoie cette colonne pour ne garder que le chiffre ou "RDC"
df = df.withColumn(
    "etage_clean",
    F.when(F.lower(F.col("etage_raw")).contains("rdc"), "RDC")
    # On extrait le premier groupe de chiffres trouvé dans notre zone "etage_raw"
    .otherwise(F.regexp_extract(F.col("etage_raw"), r"(\d+)", 1))
)

# On remplace les chaînes vides par None pour la clarté
df = df.withColumn(
    "etage_clean", 
    F.when(F.col("etage_clean") == "", None).otherwise(F.col("etage_clean"))
)


df = df.drop("etage_raw")
# %%


#liste des communes du rhones
liste_communes_brute = """Affoux (69001)
Aigueperse (69002)
Albigny-sur-Saône (69003)
Alix (69004)
Ambérieux (69005)
Amplepuis (69006)
Ampuis (69007)
Ancy (69008)
Anse (69009)
L'Arbresle (69010)
Les Ardillats (69012)
Arnas (69013)
Aveize (69014)
Azolette (69016)
Bagnols (69017)
Beaujeu (69018)
Beauvallon (69179)
Belleville-en-Beaujolais (69019)
Belmont-d'Azergues (69020)
Bessenay (69021)
Bibost (69022)
Blacé (69023)
Le Breuil (69026)
Brignais (69027)
Brindas (69028)
Bron (69029)
Brullioles (69030)
Brussieu (69031)
Bully (69032)
Cailloux-sur-Fontaines (69033)
Caluire-et-Cuire (69034)
Cenves (69035)
Cercié (69036)
Chabanière (69228)
Chambost-Allières (69037)
Chambost-Longessaigne (69038)
Chamelet (69039)
Champagne-au-Mont-d'Or (69040)
La Chapelle-sur-Coise (69042)
Chaponnay (69270)
Chaponost (69043)
Charbonnières-les-Bains (69044)
Charentay (69045)
Charly (69046)
Charnay (69047)
Chasselay (69049)
Chassieu (69271)
Châtillon (69050)
Chaussan (69051)
Chazay-d'Azergues (69052)
Chénas (69053)
Chénelette (69054)
Les Chères (69055)
Chessy (69056)
Chevinay (69057)
Chiroubles (69058)
Civrieux-d'Azergues (69059)
Claveisolles (69060)
Cogny (69061)
Coise (69062)
Collonges-au-Mont-d'Or (69063)
Colombier-Saugnieu (69299)
Communay (69272)
Condrieu (69064)
Corbas (69273)
Corcelles-en-Beaujolais (69065)
Cours (69066)
Courzieu (69067)
Couzon-au-Mont-d'Or (69068)
Craponne (69069)
Cublize (69070)
Curis-au-Mont-d'Or (69071)
Dardilly (69072)
Décines-Charpieu (69275)
Denicé (69074)
Deux-Grosnes (69135)
Dième (69075)
Dommartin (69076)
Dracé (69077)
Duerne (69078)
Échalas (69080)
Écully (69081)
Émeringes (69082)
Éveux (69083)
Feyzin (69276)
Fleurie (69084)
Fleurie-sur-Saône (69085)
Fleurieux-sur-l'Arbresle (69086)
Fontaines-Saint-Martin (69087)
Fontaines-sur-Saône (69088)
Francheville (69089)
Frontenas (69090)
Genas (69277)
Genay (69278)
Givors (69091)
Gleizé (69092)
Grandris (69093)
Grézieu-la-Varenne (69094)
Grézieu-le-Marché (69095)
Grigny-sur-Rhône (69096)
Les Haies (69097)
Les Halles (69098)
Haute-Rivoire (69099)
Irigny (69100)
Jonage (69279)
Jons (69280)
Joux (69102)
Juliénas (69103)
Jullié (69104)
Lacenas (69105)
Lachassagne (69106)
Lamure-sur-Azergues (69107)
Lancié (69108)
Lantignié (69109)
Larajasse (69110)
Légny (69111)
Lentilly (69112)
Létra (69113)
Limas (69115)
Limonest (69116)
Lissieu (69117)
Loire-sur-Rhône (69118)
Longes (69119)
Longessaigne (69120)
Lozanne (69121)
Lucenay (69122)
Lyon (69123)
Marchampt (69124)
Marcilly-d'Azergues (69125)
Marcy (69126)
Marcy-l'Étoile (69127)
Marennes (69281)
Meaux-la-Montagne (69130)
Messimy (69131)
Meys (69132)
Meyzieu (69282)
Millery (69133)
Mions (69283)
Moiré (69134)
Montagny (69136)
Montanay (69284)
Montmelas-Saint-Sorlin (69137)
Montromant (69138)
Montrottier (69139)
Morancé (69140)
Mornant (69141)
La Mulatière (69142)
Neuville-sur-Saône (69143)
Odenas (69145)
Orliénas (69148)
Oullins-Pierre-Bénite (69149)
Le Perréon (69151)
Poleymieux-au-Mont-d'Or (69153)
Pollionnay (69154)
Pomeys (69155)
Pommiers (69156)
Porte des Pierres Dorées (69114)
Poule-les-Écharmeaux (69160)
Propières (69161)
Pusignan (69285)
Quincié-en-Beaujolais (69162)
Quincieux (69163)
Ranchal (69164)
Régnié-Durette (69165)
Rillieux-la-Pape (69286)
Riverie (69166)
Rivolet (69167)
Rochetaillée-sur-Saône (69168)
Ronno (69169)
Rontalon (69170)
Sain-Bel (69171)
Saint-André-la-Côte (69180)
Saint-Appolinaire (69181)
Saint-Bonnet-de-Mure (69287)
Saint-Bonnet-des-Bruyères (69182)
Saint-Bonnet-le-Troncy (69183)
Saint-Clément-de-Vers (69186)
Saint-Clément-les-Places (69187)
Saint-Clément-sur-Valsonne (69188)
Saint-Cyr-au-Mont-d'Or (69191)
Saint-Cyr-le-Chatoux (69192)
Saint-Cyr-sur-le-Rhône (69193)
Saint-Didier-au-Mont-d'Or (69194)
Saint-Didier-sur-Beaujeu (69196)
Saint-Étienne-des-Oullières (69197)
Saint-Étienne-la-Varenne (69198)
Saint-Fons (69199)
Saint-Forgeux (69200)
Saint-Genis-l'Argentière (69203)
Saint-Genis-Laval (69204)
Saint-Genis-les-Ollières (69205)
Saint-Georges-de-Reneins (69206)
Saint-Germain-au-Mont-d'Or (69207)
Saint-Germain-Nuelles (69208)
Saint-Igny-de-Vers (69209)
Saint-Jean-des-Vignes (69212)
Saint-Jean-la-Bussière (69214)
Saint-Julien (69215)
Saint-Julien-sur-Bibost (69216)
Saint-Just-d'Avray (69217)
Saint-Lager (69218)
Saint-Laurent-d'Agny (69219)
Saint-Laurent-de-Chamousset (69220)
Saint-Laurent-de-Mure (69288)
Saint-Marcel-l'Éclairé (69225)
Saint-Martin-en-Haut (69227)
Saint-Nizier-d'Azergues (69229)
Saint-Pierre-de-Chandieu (69289)
Saint-Pierre-la-Palud (69231)
Saint-Priest (69290)
Saint-Romain-au-Mont-d'Or (69233)
Saint-Romain-de-Popey (69234)
Saint-Romain-en-Gal (69235)
Saint-Romain-en-Gier (69236)
Saint-Symphorien-d'Ozon (69291)
Saint-Symphorien-sur-Coise (69238)
Saint-Vérand (69239)
Saint-Vincent-de-Reins (69240)
Sainte-Catherine (69184)
Sainte-Colombe (69189)
Sainte-Consorce (69190)
Sainte-Foy-l'Argentière (69201)
Sainte-Foy-lès-Lyon (69202)
Sainte-Paule (69230)
Salles-Arbuissonnas-en-Beaujolais (69172)
Sarcey (69173)
Sathonay-Camp (69292)
Sathonay-Village (69293)
Les Sauvages (69174)
Savigny (69175)
Sérézin-du-Rhône (69294)
Simandres (69295)
Solaize (69296)
Soucieu-en-Jarrest (69176)
Sourcieux-les-Mines (69177)
Souzy (69178)
Taluyers (69241)
Taponas (69242)
Tarare (69243)
Tassin-la-Demi-Lune (69244)
Ternand (69245)
Ternay (69297)
Theizé (69246)
Thizy-les-Bourgs (69248)
Thurins (69249)
La Tour-de-Salvagny (69250)
Toussieu (69298)
Trèves (69252)
Tupin-et-Semons (69253)
Val d'Oingt (69024)
Valsonne (69254)
Vaugneray (69255)
Vaulx-en-Velin (69256)
Vaux-en-Beaujolais (69257)
Vauxrenard (69258)
Vénissieux (69259)
Vernaison (69260)
Vernay (69261)
Ville-sur-Jarnioux (69265)
Villechenève (69263)
Villefranche-sur-Saône (69264)
Villeurbanne (69266)
Villié-Morgon (69267)
Vindry-sur-Turdine (69157)
Vourles (69268)
Yzeron (69269)"""

# 2. Nettoyage de la liste : on enlève les codes (CP) et on trie par longueur
communes_nettes = [re.sub(r'\s*\(\d{5}\)', '', ligne).strip() for ligne in liste_communes_brute.strip().split('\n')]
communes_nettes.sort(key=len, reverse=True)

# 3. Création de la colonne ville
# On initialise avec "." comme tu l'as demandé
col_ville = F.lit(".")

for c in communes_nettes:
    col_ville = F.when(F.lower(F.col("zone")).contains(c.lower()), c).otherwise(col_ville)

df = df.withColumn("ville", col_ville)

# 4. Création de la colonne code_postal (extraction directe via regex)
df = df.withColumn(
    "code_postal",
    F.regexp_extract(F.col("zone"), r"(\d{5})", 1)
)


# Visualisation
df.select("zone", "ville", "code_postal").show(truncate=False)


df_erreurs = df.filter((F.col("ville") == ".") | (F.col("ville").isNull()))

# Affichage des colonnes zone et ville pour ces lignes
df_erreurs.select("zone", "ville").show(truncate=False)
print(f"Nombre de lignes non identifiées : {df_erreurs.count()}")

df = df.drop("details", "type_de_bien","zone", "parts")

#Remplace autre :
df = df.withColumn(
    "type_clean",
    F.when(
        F.col("type_clean") == "autre",
        F.when(
            (F.col("terrain").isNotNull()) & (F.col("etage_clean").isNull()),
            "maison"
        ).otherwise("appartement")
    ).otherwise(F.col("type_clean"))
)

print("Lignes avant chambre et pièces nulles", df.count())

#Enleve les lignes où chambres et pieces sont tous les deux manquants
df = df.filter(
    F.col("chambres").isNotNull() &
    F.col("pieces").isNotNull()
)

print("Nombre de lignes après nettoyage global :", df.count())
df.show()


#Traitement des valeurs manquantes

# %%
def count_missings(df):
    # Liste pour stocker nos expressions de comptage
    expressions = []
    
    for c, dtype in df.dtypes:
        # On commence par le check de base : est-ce que c'est NULL ?
        condition = F.col(c).isNull()
        
        # Si la colonne est numérique, on ajoute le check NaN
        if dtype in ['double', 'float']:
            condition = condition | F.isnan(F.col(c))
            
        expressions.append(F.count(F.when(condition, c)).alias(c))
    
    return df.select(expressions)

count_missings(df).show()

from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Discrétiser surface 
# %%
df = df.withColumn(
    "surface_bin",
    F.when(
        F.col("surface").isNotNull(),
        F.when(F.col("surface") < 40, "<40")
         .when((F.col("surface") >= 40) & (F.col("surface") < 70), "40-70")
         .when((F.col("surface") >= 70) & (F.col("surface") < 100), "70-100")
         .when((F.col("surface") >= 100) & (F.col("surface") < 150), "100-150")
         .otherwise("150+")
    )
)


# Remplacement NaN Pièce
df = df.withColumn(
    "pieces",
    F.when(
        F.col("pieces").isNull(),
        F.when(F.col("chambres").isNotNull(),
            F.when(F.col("chambres") == 1, 1)
             .otherwise(F.col("chambres") + 1)
        )
    ).otherwise(F.col("pieces"))
)

# Remplacement NaN chambres
median_chambres = df.groupBy("pieces").agg(
    F.expr("percentile_approx(chambres, 0.5)").alias("med_chambres")
)

df = df.join(median_chambres, "pieces", "left")

df = df.withColumn(
    "chambres",
    F.when(
        F.col("chambres").isNull() & F.col("pieces").isNotNull(),
        F.when(F.col("pieces") == 1, 1)
        .otherwise(F.col("med_chambres"))
    ).otherwise(F.col("chambres"))
).drop("med_chambres")


# Remplacement NaN Surface
median_surface = df.groupBy("type_clean", "chambres", "pieces").agg(
    F.expr("percentile_approx(surface, 0.5)").alias("med_surface")
)

df = df.join(median_surface, ["type_clean", "chambres","pieces"], "left")

df = df.withColumn(
    "surface",
    F.when(
        F.col("surface").isNull() & F.col("med_surface").isNotNull(),
        F.col("med_surface")
    ).otherwise(F.col("surface"))
).drop("med_surface")



# Remplacement NaN Terrain
from pyspark.sql.window import Window

# =========================================================
# 1. MODE GLOBAL (APPARTEMENTS)
# =========================================================
global_mode = df.filter(F.col("type_clean") == "appartement") \
    .groupBy("etage_clean") \
    .count() \
    .orderBy(F.desc("count")) \
    .first()["etage_clean"]


# =========================================================
# 2. MODE PAR PIECES
# =========================================================
mode_pieces = df.filter(F.col("type_clean") == "appartement") \
    .groupBy("pieces", "etage_clean") \
    .count()

w = Window.partitionBy("pieces").orderBy(F.desc("count"))

mode_pieces = mode_pieces.withColumn(
    "rn",
    F.row_number().over(w)
).filter(F.col("rn") == 1) \
 .select(
    "pieces",
    F.col("etage_clean").alias("mode_piece")
)

df = df.join(mode_pieces, "pieces", "left")


# =========================================================
# 3. IMPUTATION
# =========================================================

df = df.withColumn(
    "etage_clean",
    F.when(
        F.col("type_clean") == "maison",
        F.lit("MAISON")
    ).when(
        F.col("etage_clean").isNull() & F.col("mode_piece").isNotNull(),
        F.col("mode_piece")
    ).when(
        F.col("etage_clean").isNull(),
        F.lit(global_mode)
    ).otherwise(F.col("etage_clean"))
).drop("mode_piece")

#Si tjrs NaN on remplace par la médiane par type
medians = df.groupBy("type_clean").agg(
    F.expr("percentile_approx(surface, 0.5)").alias("med_surface"),
    F.expr("percentile_approx(pieces, 0.5)").alias("med_pieces"),
    F.expr("percentile_approx(chambres, 0.5)").alias("med_chambres"),
    F.expr("percentile_approx(terrain, 0.5)").alias("med_terrain")
)

df = df.join(medians, "type_clean", "left")

df = df.withColumn(
    "surface",
    F.when(F.col("surface").isNull(), F.col("med_surface"))
    .otherwise(F.col("surface"))
).withColumn(
    "pieces",
    F.when(F.col("pieces").isNull(), F.col("med_pieces"))
    .otherwise(F.col("pieces"))
).withColumn(
    "chambres",
    F.when(F.col("chambres").isNull(), F.col("med_chambres"))
    .otherwise(F.col("chambres"))
).withColumn(
    "terrain",
    F.when(F.col("terrain").isNull(), F.col("med_terrain"))
    .otherwise(F.col("terrain"))
)

df = df.drop("med_surface", "med_pieces", "med_chambres", "med_terrain", "surface_bin")
#Vérification finale
count_missings(df).show()

# Traitement des valeurs extrêmes 
#On regarde les lignes qui ont des valeurs au dela du 99eme percentiles pour chaque colonnes numériques
#(on ne prends pas en compte la cibles)

# %%
# %%
from pyspark.sql.types import IntegerType, DoubleType, FloatType, LongType

numeric_cols = [
    f.name for f in df.schema.fields
    if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType))
]

for c in numeric_cols:
    if c not in ["prix", "neuf_flag"]:
        p99 = df.approxQuantile(c, [0.99], 0.01)[0]

        print(f"\n=== {c} > 99e percentile ({p99}) ===")

        df.filter(F.col(c) >= p99).show(truncate=False)

#Comme y en a pas bcp on peut faire des changements à la main
#on corrige seulement les incohérences globales
df = df.withColumn(
    "chambres",
    F.when(
        (F.col("chambres") == 54) &
        (F.col("pieces") == 6) &
        (F.col("surface") == 130.3) &
        (F.col("ville") == "Dardilly"),
        F.lit(4)
    ).otherwise(F.col("chambres"))
)

# %%
def cap_percentile_by_group(df, col_name, group_cols, lower=0.01, upper=0.99):
    w = Window.partitionBy(*group_cols)
    df = df.withColumn(f"p_low",  F.expr(f"percentile_approx({col_name}, {lower})").over(w))
    df = df.withColumn(f"p_high", F.expr(f"percentile_approx({col_name}, {upper})").over(w))
    df = df.withColumn(
        col_name,
        F.greatest(F.col("p_low"), F.least(F.col("p_high"), F.col(col_name)))
    ).drop("p_low", "p_high")
    return df
#On ramène au 99eme percentile les valeurs extrêmes
df = cap_percentile_by_group(df, "surface",  ["type_clean", "pieces"])
df = cap_percentile_by_group(df, "terrain",  ["pieces"], lower=0.01, upper=0.99)
df = cap_percentile_by_group(df, "pieces",   ["type_clean"])
df = cap_percentile_by_group(df, "chambres", ["type_clean", "pieces"])

#Verification de la cohérence
# %%
df = df.withColumn("prix_m2", F.col("prix") / F.col("surface"))

# Rapport avant corrections
checks = {
    "Surface < 7m² par pièce"         : df.filter(F.col("surface") / F.col("pieces") < 7),
    "Chambres >= pièces"               : df.filter(F.col("chambres") >= F.col("pieces")),
    "Prix/m² < 500 ou > 25 000"        : df.filter((F.col("prix_m2") < 500) | (F.col("prix_m2") > 25000)),
    "Appartement avec terrain > 0"     : df.filter((F.col("type_clean") == "appartement") & (F.col("terrain") > 0)),
    "Maison avec étage non MAISON"     : df.filter((F.col("type_clean") == "maison") & (F.col("etage_clean") != "MAISON")),
}

print("=" * 55)
print("RAPPORT DE COHÉRENCE")
print("=" * 55)
for label, df_check in checks.items():
    n = df_check.count()
    flag = "⚠" if n > 0 else "✓"
    print(f"{flag}  {label:<40} : {n} ligne(s)")
print("=" * 55)

#Rectifications des incohérences suite a la windsorisation ou non
#Méthodologie chambres >= pieces: 
# si chambre > pieces => on fait en sorte que chambre : pieces -1
# si égalité mais nb de pièces < 4 on laisse comme tel
#Sinon on choisi de modifier le nb de chambre pieces -1
df = df.withColumn(
    "chambres",
    F.when(
        F.col("chambres") > F.col("pieces"),
        F.col("pieces") - 1
    ).when(
        (F.col("chambres") == F.col("pieces")) & (F.col("pieces") > 4),
        F.col("pieces") -1 
    ).otherwise(F.col("chambres"))
)

# Prix/m² hors bornes → suppression
df = df.withColumn("prix_m2", F.col("prix") / F.col("surface"))
df = df.filter((F.col("prix_m2") >= 500) & (F.col("prix_m2") <= 25000))
df = df.drop("prix_m2")

print(f"Lignes restantes : {df.count()}")

#csv
#df.toPandas().to_csv("logic_immo_lyon_propre.csv", index=False, sep=";", encoding="utf-8-sig")