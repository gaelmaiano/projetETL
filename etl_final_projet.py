import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text
import os
import mysql.connector
from datetime import datetime
import subprocess

# === CONFIGURATION ===
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
EXPORT_DIR = './exports'
os.makedirs(EXPORT_DIR, exist_ok=True)

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# === FONCTION : Créer l'utilisateur MySQL avec droits ===
def creer_utilisateur_mysql():
    """Crée l'utilisateur 'appuser'@'localhost' et lui attribue les droits nécessaires"""
    logging.info("🔧 Vérification/création de l'utilisateur MySQL 'appuser'")

    # Connexion admin (root ou utilisateur avec privilèges)
    config_admin = {
        "host": MYSQL_HOST,
        "port": int(MYSQL_PORT),
        "user": "root",           # 🔐 Change si tu utilises un autre admin
        "password": "example_password",       # 🔥 Change ton mot de passe root ici
        "database": MYSQL_DB
    }

    try:
        with mysql.connector.connect(**config_admin) as conn:
            cursor = conn.cursor()

            # Créer l'utilisateur s'il n'existe pas
            try:
                cursor.execute("CREATE USER 'appuser'@'localhost' IDENTIFIED BY 'example_password';")
                logging.info("✅ Utilisateur 'appuser'@'localhost' créé")
            except mysql.connector.Error as e:
                if e.errno == 1396:
                    logging.info("ℹ️  L'utilisateur 'appuser'@'localhost' existe déjà")
                else:
                    raise e

            # Attribuer les droits nécessaires
            privileges = (
                "SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, "
                "SHOW VIEW, EVENT, TRIGGER"
            )
            cursor.execute(f"GRANT {privileges} ON {MYSQL_DB}.* TO 'appuser'@'localhost';")
            cursor.execute("FLUSH PRIVILEGES;")
            logging.info(f"✅ Droits attribués à 'appuser'@'localhost' sur `{MYSQL_DB}`")

    except mysql.connector.Error as e:
        logging.error(f"❌ Impossible de configurer l'utilisateur MySQL : {e}")
        raise


# === FONCTION : Créer les tables si elles n'existent pas ===
def create_table_if_not_exists(engine, create_table_sql):
    with engine.connect() as connection:
        try:
            connection.execute(text(create_table_sql))
            connection.commit()
            logging.info("✅ Table créée ou existe déjà")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la création de la table : {e}")
            raise


# === FONCTION : Extraire CSV ===
def extract_csv(path):
    logging.info("📥 Extraction du fichier CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Fichier CSV introuvable : {path}")
    df = pd.read_csv(path)
    logging.info(f"✅ {len(df)} lignes extraites du CSV")
    return df


# === FONCTION : Extraire SQLite ===
def extract_sqlite(db_path):
    logging.info(f"🗄️  Connexion à la base SQLite : {db_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ Base SQLite introuvable : {db_path}")

    with sqlite3.connect(db_path) as conn:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        data = {}
        for table in tables['name']:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            data[table] = df
            logging.info(f"✅ Table '{table}' : {len(df)} lignes")
    return data


# === FONCTION : Charger avec anti-doublons ===
def load_to_mysql_deduplicated(df, table_name, engine, pk_column, index_as_pk=False):
    logging.info(f"🔁 Chargement dans MySQL (anti-doublons) : '{table_name}'")
    with engine.connect() as conn:
        # Vérifier si la table existe
        try:
            conn.execute(text(f"SELECT 1 FROM `{table_name}` LIMIT 1"))
            has_table = True
        except Exception:
            has_table = False

        if has_table and pk_column:
            try:
                existing = pd.read_sql(f"SELECT `{pk_column}` FROM `{table_name}`", conn)
                existing_ids = existing[pk_column].dropna().tolist()
                df = df[~df[pk_column].isin(existing_ids)]
                logging.info(f"➡️  {len(df)} nouvelles lignes après filtrage des doublons")
            except Exception as e:
                logging.warning(f"⚠️  Impossible de lire les IDs existants dans '{table_name}' : {e}")

    # Définir les types SQL
    dtype_mapping = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            dtype_mapping[col] = types.String(255)
        elif df[col].dtype == 'int64':
            dtype_mapping[col] = types.BigInteger()
        elif df[col].dtype == 'float64':
            dtype_mapping[col] = types.Float()
        elif df[col].dtype == 'datetime64[ns]':
            dtype_mapping[col] = types.DateTime()

    if not df.empty:
        try:
            df.to_sql(
                table_name,
                con=engine,
                if_exists='append',
                index=index_as_pk,
                dtype=dtype_mapping
            )
            logging.info(f"✅ {len(df)} lignes insérées dans '{table_name}'")
        except Exception as e:
            logging.error(f"❌ Échec du chargement dans '{table_name}' : {e}")
            raise
    else:
        logging.info(f"🟡 Aucune nouvelle ligne à insérer dans '{table_name}'")


# === FONCTION : Export SQL complet ===
def export_sql_complet():
    logging.info("📦 Démarrage de l'export SQL complet...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{EXPORT_DIR}/distributech_full_backup_{timestamp}.sql"

    try:
        cmd = [
            "mysqldump",
            f"--host={MYSQL_HOST}",
            f"--port={MYSQL_PORT}",
            "--single-transaction",
            "--routines",
            "--triggers",
            f"--user={MYSQL_USER}",
            f"--password={MYSQL_PASSWORD}",
            MYSQL_DB
        ]
        with open(output_file, "w", encoding="utf-8") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        logging.info(f"✅ Export SQL terminé : {output_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Échec de mysqldump : {e.stderr}")
    except Exception as e:
        logging.error(f"❌ Erreur inattendue lors de l'export SQL : {e}")


# === FONCTION : Export état des stocks ===
def export_etat_stocks(engine):
    logging.info("📊 Génération de l'état des stocks par produit...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{EXPORT_DIR}/etat_des_stocks_{timestamp}.csv"

    query = """
    SELECT
        p.produit_id,
        p.nom_produit,
        COALESCE(SUM(prod.quantite_produite), 0) AS quantite_produite,
        COALESCE(SUM(lc.quantite), 0) AS quantite_vendue,
        COALESCE(SUM(prod.quantite_produite), 0) - COALESCE(SUM(lc.quantite), 0) AS stock_disponible
    FROM Produits p
    LEFT JOIN Productions prod ON p.produit_id = prod.product_id
    LEFT JOIN LignesCommande lc ON p.produit_id = lc.produit_id
    GROUP BY p.produit_id, p.nom_produit
    ORDER BY p.produit_id;
    """

    try:
        df_stock = pd.read_sql(query, engine)
        df_stock.to_csv(output_file, index=False)
        logging.info(f"✅ État des stocks exporté : {output_file}")
        logging.info(f"📈 {len(df_stock)} produits dans le rapport")
    except Exception as e:
        logging.error(f"❌ Échec de génération de l'état des stocks : {e}")


# === MAIN ===
def main():
    logging.info("🚀 Démarrage du script ETL")

    # --- 1. Créer l'utilisateur MySQL ---
    creer_utilisateur_mysql()

    # --- 2. Créer l'engine SQLAlchemy ---
    mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    engine = create_engine(mysql_url)

    # --- 3. Extraire les données ---
    df_csv = extract_csv(CSV_PATH)
    sqlite_data = extract_sqlite(SQLITE_DB_PATH)

    # --- 4. Créer les tables ---
    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS Regions (
        region_id INT PRIMARY KEY,
        nom_region VARCHAR(255)
    )""")

    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS Revendeurs (
        revendeur_id INT PRIMARY KEY,
        nom_revendeur VARCHAR(255),
        region_id INT,
        email_contact VARCHAR(255),
        FOREIGN KEY (region_id) REFERENCES Regions(region_id)
    )""")

    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS Produits (
        produit_id INT PRIMARY KEY,
        nom_produit VARCHAR(255),
        prix_unitaire FLOAT
    )""")

    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS Productions (
        production_id INT PRIMARY KEY,
        product_id INT,
        quantite_produite INT,
        date DATE
    )""")

    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS Commandes (
        commande_id INT PRIMARY KEY,
        numero_commande VARCHAR(255),
        date_commande DATETIME,
        revendeur_id INT,
        FOREIGN KEY (revendeur_id) REFERENCES Revendeurs(revendeur_id)
    )""")

    create_table_if_not_exists(engine, """
    CREATE TABLE IF NOT EXISTS LignesCommande (
        ligne_id INT PRIMARY KEY,
        commande_id INT,
        produit_id INT,
        quantite INT,
        prix_unitaire_vente FLOAT,
        FOREIGN KEY (commande_id) REFERENCES Commandes(commande_id),
        FOREIGN KEY (produit_id) REFERENCES Produits(produit_id)
    )""")

    # --- 5. Charger les données SQLite ---
    if 'region' in sqlite_data:
        df = sqlite_data['region'].rename(columns={'region_name': 'nom_region'})
        load_to_mysql_deduplicated(df, 'Regions', engine, pk_column='region_id')

    if 'revendeur' in sqlite_data:
        df = sqlite_data['revendeur'].rename(columns={'revendeur_name': 'nom_revendeur'})
        df['email_contact'] = df['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
        load_to_mysql_deduplicated(df, 'Revendeurs', engine, pk_column='revendeur_id')

    if 'produit' in sqlite_data:
        df = sqlite_data['produit'].rename(columns={
            'product_name': 'nom_produit',
            'cout_unitaire': 'prix_unitaire',
            'product_id': 'produit_id'
        })
        load_to_mysql_deduplicated(df, 'Produits', engine, pk_column='produit_id')

    if 'production' in sqlite_data:
        df = sqlite_data['production'].rename(columns={
            'quantity': 'quantite_produite',
            'date_production': 'date',
            'product_id': 'product_id'
        })
        df = df.reset_index()
        load_to_mysql_deduplicated(df, 'Productions', engine, pk_column='production_id')

    # --- 6. Traiter les commandes ---
    df_csv = df_csv.rename(columns={
        'numero_commande': 'numero_commande',
        'commande_date': 'date_commande',
        'quantity': 'quantite',
        'unit_price': 'prix_unitaire_vente'
    })

    # Générer un ID unique par commande
    df_csv['commande_id'] = df_csv.groupby(['numero_commande', 'date_commande']).ngroup() + 1

    # Charger Commandes
    commandes = df_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
    commandes['date_commande'] = pd.to_datetime(commandes['date_commande'])
    load_to_mysql_deduplicated(commandes, 'Commandes', engine, pk_column='commande_id')

    # Charger LignesCommande
    lignes = df_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']].copy()
    lignes.loc[:, 'ligne_id'] = range(1, len(lignes) + 1)
    lignes = lignes.rename(columns={'product_id': 'produit_id'})
    load_to_mysql_deduplicated(lignes, 'LignesCommande', engine, pk_column='ligne_id')

    # --- 7. Exporter les rapports ---
    logging.info("📤 Génération des exports finaux")
    export_sql_complet()
    export_etat_stocks(engine)

    logging.info("✅ Script ETL terminé avec succès")


if __name__ == "__main__":
    main()