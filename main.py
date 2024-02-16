import pandas as pd
from neo4j import GraphDatabase
from functions import create_node, create_relationship, update_node_properties

# Lire le fichier CSV
df = pd.read_csv('communes-departement-region.csv')

# On récupère toutes les régions avec nom et code
df_regions = df[['nom_region','code_region']].drop_duplicates()
df_regions = df_regions.reset_index(drop=True)
df_regions  = df_regions.dropna()

# On récupère tous les départements avec nom, code et code du département
df_departements = df[['nom_departement','code_departement','code_region']].drop_duplicates()
df_departements = df_departements.reset_index(drop=True)
df_departements = df_departements.dropna()

# On récupère toutes les communes avec nom, code, code du département
df_communes = df[['nom_commune','code_commune','code_departement']].drop_duplicates()
df_communes = df_communes.reset_index(drop=True)
df_communes = df_communes.dropna()


print(df.columns)

# Créer une instance de GraphDatabase
db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))


# Créer une session
with db.session() as session:

    # -- Supprimer tous les nodes et relations à chaque fois que l'on relance le programme
    session.run("MATCH (n:region) DETACH DELETE n")
    session.run("MATCH (n:departement) DETACH DELETE n")
    session.run("MATCH (n:commune) DETACH DELETE n")

    # Créer tous les nodes régions
    for index, row in df_regions.iterrows():
        session.run("CREATE (n:region {name: $name, code: $code})", name=row['nom_region'], code=row['code_region'])

    # Créer tous les nodes départements
    for index, row in df_departements.iterrows():
        session.run("MATCH (r:region {code: $code_region}) CREATE (n:departement {name: $name, code: $code, code_region: $code_region})", name=row['nom_departement'], code=row['code_departement'], code_region=row['code_region'])

    # Créer tous les nodes communes
    for index, row in df_communes.iterrows():
        session.run("MATCH (d:departement {code: $code_departement}) CREATE (n:commune {name: $name, code: $code, code_departement: $code_departement})", name=row['nom_commune'], code=row['code_commune'], code_departement=row['code_departement'])

    # Lier toutes les communes à leur département
    session.run("MATCH (d:departement),(c:commune) WHERE d.code = c.code_departement CREATE (d)-[A:APPARTIENT_A]->(c)")

    # Lier tous les départements à leur région
    session.run("MATCH (r:region),(d:departement) WHERE r.code = d.code_region CREATE (r)-[A:FAIT_PARTI_DE]->(d)")



    # # Commencer une transaction
    # with session.begin_transaction() as tx:
    #
    #     # Créer tous les nodes regions
    #     for index, row in df_regions.iterrows():
    #         create_node(tx, {"name": row['nom_region'], "code": row['code_region'], "type": "region"})

        # # Parcourir chaque ligne du DataFrame
        # for index, row in df.iterrows():
        #     # Créer des nœuds pour la commune, le département et la région
        #     commune = create_node(tx, {"name": row['commune'], "type": "Commune"})
        #     departement = create_node(tx, {"name": row['departement'], "type": "Departement"})
        #     region = create_node(tx, {"name": row['region'], "type": "Region"})
        #
        #     # Créer des relations entre les nœuds
        #     create_relationship(tx, commune, departement, "PART_OF")
        #     create_relationship(tx, departement, region, "PART_OF")
        #
        #     # Ajouter des propriétés supplémentaires aux nœuds
        #     # Remplacez 'equipements', 'bilan_comptable', 'maire', 'habitants' par les colonnes réelles de votre fichier CSV
        #     update_node_properties(tx, commune, {"equipements": row['equipements'], "bilan_comptable": row['bilan_comptable'], "maire": row['maire'], "habitants": row['habitants']})