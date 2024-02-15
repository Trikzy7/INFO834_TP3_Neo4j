from neo4j import GraphDatabase


# ---- Exemple de fonctions pour interagir avec la base de données
def get_all_nodes(tx):
    """
    Récupère tous les noeuds de la base de données
    :param tx:
    :return:
    """
    result = tx.run("MATCH (n) RETURN n")
    return [record["n"] for record in result]

def get_by_id(tx, id):
    """
    Récupère un noeud par son id
    :param tx:
    :param id:
    :return:
    """
    result = tx.run("MATCH (n) WHERE id(n) = $id RETURN n", id=id)
    return result.single()[0]


def get_nodes_by_property(tx, key, value):
    """
    Récupère tous les noeuds de la base de données qui ont une propriété spécifique
    :param tx:
    :param key:
    :param value:
    :return:
    """
    result = tx.run("MATCH (n) WHERE n.$key = $value RETURN n", key=key, value=value)
    return [record["n"] for record in result]


def get_all_relationships(tx):
    """
    Récupère toutes les relations de la base de données
    :param tx:
    :return:
    """
    result = tx.run("MATCH (n)-[r]->(m) RETURN r")
    return [record["r"] for record in result]





def create_node(tx, properties):
    query = "CREATE (n {props}) RETURN n"

    print(query)
    result = tx.run(query, props=properties)
    return result.single()[0]


def create_relationship(tx, id1, id2, relationship_type, properties=None):
    properties = properties or {}
    query = """
    MATCH (a), (b)
    WHERE id(a) = $id1 AND id(b) = $id2
    CREATE (a)-[r:{type} {props}]->(b)
    RETURN r
    """.format(type=relationship_type, props=properties)
    result = tx.run(query, id1=id1, id2=id2)
    return result.single()[0]


def delete_node_by_id(tx, id):
    tx.run("MATCH (n) WHERE id(n) = $id DELETE n", id=id)


def update_node_properties(tx, id, properties):
    for key, value in properties.items():
        tx.run("MATCH (n) WHERE id(n) = $id SET n.$key = $value", id=id, key=key, value=value)

