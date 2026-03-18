import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import sys

print('Connexion à la base de données...')
USERNAME = "tgonzalezbon"
PASSWORD = "bonjour"

try:
    conn = psycopg2.connect(
        host='localhost', 
        port=5444,
        dbname=USERNAME,
        user=USERNAME,
        password=PASSWORD,
        connect_timeout=10
    )
    cur = conn.cursor()
    print('Connecté !')
except Exception as e:
    exit("Connexion impossible : " + str(e))

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 

#----------------3.1---------------------

# command = """SELECT dep, valeur, indicateur
#     FROM dep_eco 
#     WHERE indicateur LIKE '%en vélo (%)%' 
   
#     AND valeur::float BETWEEN 1.0 AND 4.0 
#     ORDER BY valeur DESC;
# """


# cur.execute(command)
# rows = cur.fetchall()
# if not rows:
#     print("aucuns département avec ce critère")
# else:   
#     print("voici la liste des départements dont les transport en velo en 2024 est entre 1% et 4%")

#     for r in rows:
#         print (r['dep'], "avec une valeur de :", r['valeur'], "%")


#----------------3.2---------------------

#Quels sont les départements dont la région avait une Part des diplômés  strictement 
# supérieur à 78 % en 2017 ? 
# Afficher aussi le taux d'emploi en 2017 et 2022 pour ces départements

# command1 = """
# SELECT 
#     d.libelle AS dep ,re.indicateur, re.valeur AS val_reg FROM region_eco re 
# JOIN region r ON r.libelle = re.reg
# JOIN departement d ON r.reg = d.reg

# WHERE re.indicateur LIKE '%%Part des diplômés%%' 
#   AND re.annee LIKE '%%2017%%' 
#   AND re.valeur NOT LIKE '%%N/A%%'
#   AND re.valeur::float > 78
# ORDER BY re.valeur::float DESC;
# """

# command2 = """
# SELECT valeur 
# FROM dep_eco 
# WHERE dep = %s 
#   AND indicateur LIKE '%%Taux d''emploi%%' 
#   AND annee LIKE '%%2022%%';
# """

# command3 = """
# SELECT valeur 
# FROM dep_eco 
# WHERE dep = %s 
#   AND indicateur LIKE '%%Taux d''emploi%%' 
#   AND annee LIKE '%%2014%%';
# """'

# try:
#     cur.execute(command1)
#     rows = cur.fetchall()

#     if not rows:
#         print("Aucun département ne correspond aux critères.")
#     else:
#         for r in rows:
#             nom_dep = r['dep']  
#             valeur_reg = r['val_reg'] 
            
#             print(f"\n--- {nom_dep} ---")
#             print(f"Diplômés région : {valeur_reg}%")

#             cur.execute(command2, (nom_dep,))
            
#             # On récupère UNE SEULE ligne de résultat
#             res = cur.fetchone() 

#             if res:
#                 taux_emploi = res['valeur']
#                 print("Taux d'emploi en 2017 :", taux_emploi,"%")
#             else:
#                 print("Taux d'emploi en 2014 : Donnée non disponible dans dep_eco")

#             cur.execute(command3, (nom_dep,))
#             res2=cur.fetchone()

#             if res2:
#                 taux_emploi2 = res2['valeur']
#                 print("Taux d'emploi en 2014 :", taux_emploi2,"%")
#             else:
#                 print("Taux d'emploi en 2014 : Donnée non disponible dans dep_eco")

  

# except Exception as e:
#     print(f"Erreur lors de l'affichage : {e}")


#Afficher la différence entre l’espérance de vie des hommes et des 
# femmes en 2019 pour tous les départements de la région ayant le 
# plus grand taux de pauvreté en 2018.


# command4 = """
# SELECT re.reg FROM region_social re WHERE re.indicateur LIKE '%Taux de pauvreté%'
# AND re.valeur NOT LIKE '%%nd%%'
# AND re.valeur NOT LIKE '%%N/A%%'
# AND re.annee like '%2018%'
# ORDER BY re.valeur::float DESC LIMIT 1;
# """


# cur.execute(command4)
# res = cur.fetchone()
# if res:

#     reg_pauvre = res['reg'] 
#     print (f"la region {reg_pauvre} est la plus pauvre en 2018")

#     command5 = """
#     SELECT 
#         d.libelle AS nom_dep,
#         f.valeur::float AS esp_f,
#         h.valeur::float AS esp_h,
#         (f.valeur::float - h.valeur::float) AS ecart
#     FROM departement d
#     JOIN region r ON d.reg = r.reg
#     JOIN dep_social f ON d.dep = f.num AND f.indicateur LIKE '%%femmes%%' AND f.annee = '2019'
#     JOIN dep_social h ON d.dep = h.num AND h.indicateur LIKE '%%hommes%%' AND h.annee = '2019'
#     WHERE r.libelle = %s
#     ORDER BY ecart DESC;
#     """

#     cur.execute(command5, (reg_pauvre,))
#     deps = cur.fetchall()

#     print(f"Écarts d'espérance de vie en 2019 pour {reg_pauvre} par departements:")
#     print("-" * 50)
#     for d in deps:
#         print(f"{d['nom_dep']} --> Femme: {d['esp_f']} | Homme: {d['esp_h']} --> Diff: {d['ecart']:.2f} ans")




#Quelle est l’estimation de population en 
# 2023 de tous les départements où 
# l’espérance de vie des hommes était 
# supérieure ou égale à 80 ans en 2024 



# command6 = """
# SELECT d.dep FROM dep_social d WHERE d.indicateur 
# LIKE '%%Espérance de vie des hommes à la naissance (années)%%'
# AND d.valeur NOT LIKE '%%nd%%'
# AND d.valeur NOT LIKE '%%N/A%%'
# AND d.dep NOT LIKE '%%France%%'
# AND d.valeur::float  > 80 
# AND d.annee like '%2024%'
# """
# command7 = """
# SELECT valeur 
# FROM pop_dep
# WHERE dep = %s 
# AND annee LIKE '%%Estimations de population au 1er janvier 2023%%';
# """

# cur.execute(command6)
# rows = cur.fetchall()

# for r in rows:
#     dep = r['dep'] 
#     print (f" {dep} à une esperance de vie > 80")



# try:
#     cur.execute(command6)
#     rows = cur.fetchall()

#     if not rows:
#         print("Aucun département ne correspond aux critères.")
#     else:
#         for r in rows:
#             nom_dep = r['dep']  
            
#             print(f"\n--- {nom_dep} ---")

#             cur.execute(command7, (nom_dep,))
            
#             # On récupère UNE SEULE ligne de résultat
#             res = cur.fetchone() 

#             if res:
#                 estimation = res['valeur']
#                 print("Estimation de la population en milliers (année 2023) :", estimation)
#             else:
#                 print("Donnée non disponible dans pop_dep")


# except Exception as e:
#     print(f"Erreur lors de l'affichage : {e}")

#Quelle était l’espérance de vie des femmes et des hommes 
# en 2024 dans les régions où le taux d’activité était 
# inférieur ou égal a 73.5% en 2019 et où le Poids de 
# l'économie sociale dans les emplois salariés du territoire 
# était de plus de 10% (strictement) en 2022. 


# command_regions = """
# SELECT DISTINCT re.reg 
# FROM region_eco re
# WHERE re.reg IN (
#     -- Critère 1 : Taux d'activité <= 73.5 en 2019
#     SELECT reg FROM region_eco 
#     WHERE (indicateur ILIKE '%%Taux%%activité%%' OR indicateur ILIKE '%%Activité%%')
#       AND annee::text LIKE '%%2019%%' 
#       AND valeur NOT LIKE '%%nd%%'
#       AND valeur::float <= 73.5
# )
# AND re.reg IN (
#     -- Critère 2 : Économie sociale > 10% en 2022
#     SELECT reg FROM region_eco 
#     WHERE (indicateur ILIKE '%%économie sociale%%' OR indicateur ILIKE '%%ESS%%')
#       AND annee::text LIKE '%%2022%%' 
#       AND valeur NOT LIKE '%%nd%%'
#       AND valeur::float > 10
# );
# """

# command_vie = """
# SELECT 
#     indicateur, 
#     valeur 
# FROM region_social 
# WHERE reg = %s 
#   AND annee LIKE '%%2024%%' 
#   AND (indicateur ILIKE '%%femmes%%' OR indicateur ILIKE '%%hommes%%')
#   AND indicateur ILIKE '%%Espérance de vie%%';
# """

# try:
#     # 1. On cherche les régions qui remplissent les deux critères économiques
#     cur.execute(command_regions)
#     regions_cibles = cur.fetchall()

#     if not regions_cibles:
#         print("Aucune région ne correspond à ces critères économiques.")
#     else:


#         for r in regions_cibles:
#             nom_reg = r['reg']
            
#             # 2. Pour chaque région, on cherche l'espérance de vie
#             cur.execute(command_vie, (nom_reg,))
#             resultats_vie = cur.fetchall()

#             if resultats_vie:
#                 for v in resultats_vie:
#                     # On nettoie un peu le nom de l'indicateur pour l'affichage (Homme/Femme)
#                     genre = "Femmes" if "femmes" in v['indicateur'].lower() else "Hommes"
#                     print(f"{nom_reg} --> {genre} , espérance de vie : {v['valeur']} ans")
#             else:
#                 print(f"{nom_reg} | Données d'espérance de vie non trouvées.")

# except Exception as e:
#     print(f"Erreur lors de la requête : {e}")

command_positive = """
SELECT r.libelle AS region
FROM region r
WHERE NOT EXISTS (
    SELECT 1 
    FROM departement d
    JOIN pop_dep p ON d.libelle = p.dep
    WHERE d.reg = r.reg
      AND p.indicateur LIKE '%%Variation relative annuelle 2020-2023 (en %%)%%'
      AND p.annee LIKE '%%Totale%%'
      AND p.valeur::float <= 0
)
AND EXISTS (SELECT 1 FROM departement d2 WHERE d2.reg = r.reg);
"""

command_negative = """
SELECT r.libelle AS region
FROM region r
WHERE NOT EXISTS (
    -- On cherche s'il existe au moins UN département qui n'est PAS négatif
    SELECT 1 
    FROM departement d
    JOIN pop_dep p ON d.libelle = p.dep
    WHERE d.reg = r.reg
      AND p.indicateur LIKE '%%Variation relative annuelle 2020-2023 (en %%)%%'
      AND p.annee LIKE '%%Totale%%'
      AND p.valeur::float >= 0
)
AND EXISTS (SELECT 1 FROM departement d2 WHERE d2.reg = r.reg);
"""

def afficher_resultats(titre, commande):
    print("\n")
    print(f"--- {titre} ---")
    print("\n")
    cur.execute(commande)
    rows = cur.fetchall()
    if not rows:
        print("Aucune région ne correspond à ce critère.")
    else:
        for r in rows:
            print(f"-> {r['region']}")

try:
    # Exécution pour le cas positif
    afficher_resultats("Régions dont TOUS les départements sont en CROISSANCE", command_positive)
    
    # Exécution pour le cas négatif
    afficher_resultats("Régions dont TOUS les départements sont en DÉCLIN", command_negative)

except Exception as e:
    print(f"Erreur technique : {e}")