import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import sys

def afficheMenu():

    print("1 : afficher la liste des région")
    print("2 : choisir une région et afficher les départements de la région choisie")
    print("3 : choisir un département de la region choisi et afficher les années disponibles pour le département choisi")
    print("4 : choisir une année et afficher les information sur population du département cette année-là (ou couvrant cette anné-là), si les données sont disponibles (sinon afficher « données non disponibles")
    print("5 : choisir thème :social ou economique et afficher les donnés disponible pour l’année choisie")
    print("0 : Quitter")






def affiche_reg():


    command = """
        SELECT  libelle
        FROM region

    """

def affiche_reg():
    command = "SELECT libelle FROM region ;" 
    try:
        cur.execute(command) 
        rows = cur.fetchall()
        
        print("Voici la liste des régions : ")
        if not rows:
            print("Aucune région trouvée.")
        else:
            for r in rows:
                print(r['libelle'])

        
    except Exception:
        print("Erreur lors de la récupération des régions ")


def affiche_dep():

    affiche_reg()
    
    choix = input("Saisissez le nom exact d'une région de la liste ci-dessus ")
    
    
    command = """SELECT d.libelle FROM departement d JOIN region r ON d.reg = r.reg 
        WHERE r.libelle = %s;
    """
    
    try:
        cur.execute(command, (choix,)) 
        rows = cur.fetchall()
            
        if not rows:
            print("Aucun département trouvé pour",choix)
        else:
            print("Voici la liste des départements de", choix )
            for r in rows:
                print(r['libelle'])
            print("") 
            
    except Exception :
        print("Erreur lors de la récupération des départements ")

def annee():
    affiche_dep()
    choix = input("choisir un parmis cette liste departement")

    command = """
            SELECT annee FROM dep_eco JOIN departement ON dep_eco.num = departement.dep WHERE departement.libelle = %s
            UNION
            SELECT annee FROM dep_social JOIN departement ON dep_social.num = departement.dep WHERE departement.libelle = %s
            UNION
            SELECT SUBSTRING(annee FROM '[0-9]{4}') FROM pop_dep JOIN departement ON pop_dep.num = departement.dep WHERE departement.libelle = %s
            ORDER BY annee;
        """

    try:
        cur.execute(command, (choix, choix, choix))
        rows = cur.fetchall()
    
        if not rows:
            print("Aucune année disponible dans les bases (éco, social, pop) pour ",choix)
        else:
            print(f"\nAnnées disponibles pour le département {choix} (tous thèmes confondus) :")
            for r in rows:
                print(r['annee'])
            print("")

    except Exception :
        print("Erreur lors de la récupération des années ")


def info_pop():
    dep = input("Entrez le nom du département : ")
    annee= input("Entrez l'année souhaitée : ")


    command = """
        SELECT p.indicateur, p.annee, p.valeur FROM pop_dep p JOIN departement d ON p.num = d.dep
        WHERE d.libelle = %s 
        AND p.annee LIKE %s;
    """

    try:

        pattern = f"%{annee}%"
        
        cur.execute(command, (dep, pattern))
        rows = cur.fetchall()

        if not rows:
            print("Données non disponibles pour", dep, "en", annee)
        else:
            print("Informations Population : ",dep, "(",annee,") ---")

            for r in rows:
                print("Indicateur : ",r['indicateur'])
                print("Précision  : ", r['annee'])
                print("Valeur     : ", r['valeur'])
                print("-" * 40)
            
    except Exception:
        print("Erreur lors de la recherche de population ")




def affiche_donnees_theme():
    annee_choisie = input("Entrez l'année: ")
    
    print("Choisissez un thème :")
    print("S : Social (Table dep_social)")
    print("E : Economique (Table dep_eco)")
    theme = input("Votre choix (S/E) : ").upper()

    if theme == "S":
        table= "dep_social"
        nom= "Social"
    elif theme == "E":
        table = "dep_eco"
        nom = "Economique"
    else:
        print("Thème invalide. Retour au menu.")
        return

    # 3. Requête SQL (Notez que table_cible est injectée via f-string car c'est un identifiant structurel)
    # L'année est filtrée avec LIKE pour gérer les éventuels ".1", ".2" ou textes
    command = f"""
        SELECT d.libelle as departement, t.indicateur, t.valeur
        FROM {table} t
        JOIN departement d ON t.num = d.dep
        WHERE t.annee LIKE %s
        ORDER BY d.libelle;
    """

    try:
        pattern = f"%{annee_choisie}%"
        cur.execute(command, (pattern,))
        rows = cur.fetchall()

        if not rows:
            print("Aucune donnée disponible pour le thème" ,nom ,"en", annee_choisie)
        else:
            for r in rows:
                print("Département : ",r['departement'])
                print("Indicateur  : ",r['indicateur'])
                print("Valeur      : ",r['valeur'])
                print("-" * 30)
                
    except Exception :
        print("Erreur lors de la récupération des données thématiques ")

#Programme principale-------------------------------------------------------------------------------------------------------------------------------------



#Création de la tanière 
#Dictionnaire où chaque clé est le num de la pièce et les valeurs les voisins





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

rep = 1
while rep != 0:
    afficheMenu()
    rep=int(input())

    if rep == 1:
        affiche_reg()



    if rep == 2:
        affiche_dep()

            

    
    if rep == 3:
        annee()

    

    if rep == 4:
        info_pop()

        
    if rep == 5:
        affiche_donnees_theme()
        
    if rep == 0:
        print("A bientôt !")
        sys.exit() 