import pandas as pd 
import psycopg2 
import psycopg2.extras
import xlrd


df1 = pd.read_csv("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/region2020.csv",sep =',')
df1.set_index(df1['reg'],inplace = True)
df2 = pd.read_csv("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/departement2020.csv",sep =',')
df2.set_index(df2['dep'],inplace = True)

#------------region social ------------------
region_social = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/DD-indic-reg-dep_2008_2019_2024.xlsx"
                           ,sheet_name=1,header = [4,5], nrows = 20 )

new_columns = region_social.columns.to_list()
new_columns[0] = ('Région', 'Région')
new_columns[1] = ('Nom', 'Nom')
region_social.columns = pd.MultiIndex.from_tuples(new_columns)


#------------departement social------------------

dep_social = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/DD-indic-reg-dep_2008_2019_2024.xlsx"
                           ,sheet_name=1, header = [30,31], nrows = 104 )
new_columns = dep_social.columns.to_list()
new_columns[0] = ('Région', 'Région')
new_columns[1] = ('Nom', 'Nom')
dep_social.columns = pd.MultiIndex.from_tuples(new_columns)

print(dep_social)


#------------region eco ------------------

region_eco = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/DD-indic-reg-dep_2008_2019_2024.xlsx"
                        ,sheet_name=2, header = [5,6], nrows = 20)
new_columns = region_eco.columns.to_list()
new_columns[0] = ('Région', 'Région')
new_columns[1] = ('Nom', 'Nom')
region_eco.columns = pd.MultiIndex.from_tuples(new_columns)




#------------departement eco ------------------


dep_eco = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/DD-indic-reg-dep_2008_2019_2024.xlsx"
                        ,sheet_name=2, header = [33,34], nrows = 104)

new_columns = dep_eco.columns.to_list()
new_columns[0] = ('Région', 'Région')
new_columns[1] = ('Nom', 'Nom')
dep_eco.columns = pd.MultiIndex.from_tuples(new_columns)



evo_dep = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/Evolution_population_2012-2023.xlsx"
                      ,sheet_name=0, header = [2,3] )
evo_reg = pd.read_excel("/net/cremi/tgonzalezbon/Bureau/S2/BDD/projet/Fichiers fournis-20260305/Evolution_population_2012-2023.xlsx"
                        ,sheet_name=1, header = [2,3] )



# print(region_social) 
# print(region_social.head(1))


print('Connexion à la base de données...')
      
USERNAME="tgonzalezbon"
PASSWORD="bonjour" # `a remplacer par le mot de passe d’acc`es aux bases
try:
    conn = psycopg2.connect(host='pgsql', dbname=USERNAME,user=USERNAME,
password=PASSWORD)
except Exception as e :
    exit("Connexion impossible `a la base de donn´ees: " + str(e))

print('Connecté à la base de données')
# préparation de l’ex´ecution des requ^etes (`a ne faire qu’une fois)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 



# # # cur.execute("CREATE TABLE public.region(reg INT NOT NULL , cheflieu TEXT NOT NULL, \
# # # tncc INT  NOT NULL, ncc TEXT  NOT NULL,nccenr TEXT  NOT NULL, libelle TEXT  NOT NULL);")
# # # print("Table parc créée avec succès dans PostgreSQL")


# # # cur.execute("CREATE TABLE public.departement(dep TEXT NOT NULL,reg INT NOT NULL , cheflieu TEXT NOT NULL, \
# # # tncc INT  NOT NULL, ncc TEXT  NOT NULL,nccenr TEXT  NOT NULL, libelle TEXT  NOT NULL);")
# # # print("Table arretsbus créée avec succès dans PostgreSQL")



# # df1 = df1.astype(object)

# # for i in range (len(df1)):
# #     command = "INSERT INTO region (reg, cheflieu, tncc, ncc, nccenr, libelle) VALUES (%s,%s,%s,%s,%s,%s)" 

# #     values_1 = values_1 = tuple(df1.iloc[i].tolist())

# #     print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
# #     try:
# #     # Lancement de la requete.
# #         cur.execute(command, values_1)


# # df1 = df1.astype(object)

# # for i in range (len(df1)):
# #     command = "INSERT INTO region (reg, cheflieu, tncc, ncc, nccenr, libelle) VALUES (%s,%s,%s,%s,%s,%s)" 

# #     values_1 = values_1 = tuple(df1.iloc[i].tolist())

# #     print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
# #     try:
# #     # Lancement de la requete.
# #         cur.execute(command, values_1)
        
# #     except Exception as e :
# #     # en cas d’erreur, fermeture de la connexion
# #         cur.close()
# #         conn.close()
# #         exit("error when running: " + command + " : " + str(e))

# # df2 = df2.astype(object)

# # for i in range (len(df2)):
# #     command_2 = "INSERT INTO departement (dep , reg,cheflieu, tncc,ncc,nccenr,libelle) VALUES (%s,%s,%s,%s,%s,%s,%s)" 

# #     values_2 = tuple(df2.iloc[i].tolist())

# #     print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_2)
# #     try:
# #     # Lancement de la requete.
# #         cur.execute(command_2,values_2)
        
# #     except Exception as e :
# #     # en cas d’erreur, fermeture de la connexion
# #         cur.close()
# #         conn.close()
# #         exit("error when running: " + command + " : " + str(e))



region_social_melt = region_social.melt(
    id_vars=[('Région', 'Région'), ('Nom', 'Nom')],
    var_name=['indicateur', 'annee'],
    value_name='valeur'
)

region_social_melt.columns = ['num', 'reg', 'indicateur', 'annee', 'valeur']


cur.execute("""CREATE TABLE IF NOT EXISTS public.region_social (num TEXT NOT NULL,reg TEXT NOT NULL,indicateur TEXT NOT NULL,annee TEXT NOT NULL,valeur FLOAT NOT NULL);
""")



region_social_melt = region_social_melt.astype(object)

for i in range (len(region_social)):
    command =     """INSERT INTO public.region_social (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""

    values_1 = values_1 = tuple(region_social_melt.iloc[i].tolist())

    print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
    try:
    # Lancement de la requete.
        cur.execute(command, values_1)


        
    except Exception as e :
    # en cas d’erreur, fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))


print(region_social_melt)



#-----------------------creation dep social-----------------

dep_social_melt = dep_social.melt(
    id_vars=[('Région', 'Région'), ('Nom', 'Nom')],
    var_name=['indicateur', 'annee'],
    value_name='valeur'
)


dep_social_melt.columns = ['num', 'nom', 'indicateur', 'annee', 'valeur']


for i in range (len(region_social)):
    command =     """INSERT INTO public.dep_social (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""


dep_social_melt = dep_social_melt.astype(object)

for i in range (len(dep_social)):
    command = """INSERT INTO public.region_social (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""

    values_1 = values_1 = tuple(dep_social_melt.iloc[i].tolist())

    print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
    try:
    # Lancement de la requete.
        cur.execute(command, values_1)


        
    except Exception as e :
    # en cas d’erreur, fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))


print(dep_social_melt)






#-----------------------creation region  eco -----------------

region_eco_melt = region_eco.melt(
    id_vars=[('Région', 'Région'), ('Nom', 'Nom')],
    var_name=['indicateur', 'annee'],
    value_name='valeur'
)


region_eco_melt.columns = ['num', 'reg', 'indicateur', 'annee', 'valeur']


cur.execute("""CREATE TABLE IF NOT EXISTS public.region_eco (num TEXT NOT NULL,reg TEXT NOT NULL,indicateur TEXT NOT NULL,annee TEXT NOT NULL,valeur FLOAT NOT NULL);
""")



region_eco_melt = region_eco_melt.astype(object)

for i in range (len(region_eco_melt)):
    command =     """INSERT INTO public.region_eco (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""
    values_1 = values_1 = tuple(region_eco_melt.iloc[i].tolist())

    print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
    try:
    # Lancement de la requete.
        cur.execute(command, values_1)


        
    except Exception as e :
    # en cas d’erreur, fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))


print("ok")


#-----------------------creation dep eco-----------------

dep_eco_melt = dep_eco.melt(
    id_vars=[('Région', 'Région'), ('Nom', 'Nom')],
    var_name=['indicateur', 'annee'],
    value_name='valeur'
)


dep_eco_melt.columns = ['num', 'nom', 'indicateur', 'annee', 'valeur']


for i in range (len(dep_eco)):
    command =     """INSERT INTO public.dep_social (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""


dep_eco_melt = dep_eco_melt.astype(object)

for i in range (len(dep_eco)):
    command = """INSERT INTO public.region_social (num, reg, indicateur, annee, valeur)
    VALUES (%s, %s, %s, %s, %s);
"""

    values_1 = values_1 = tuple(dep_eco_melt.iloc[i].tolist())

    print("Exécution sur la base de données de la commande d’insertion avec les valeurs", values_1)
    try:
    # Lancement de la requete.
        cur.execute(command, values_1)


        
    except Exception as e :
    # en cas d’erreur, fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))



