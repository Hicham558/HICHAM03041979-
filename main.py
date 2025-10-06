import sqlite3
import tempfile
import base64
from contextlib import contextmanager
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import logging
import os
from psycopg2.extras import RealDictCursor
from psycopg2 import Error as Psycopg2Error
from datetime import datetime,timedelta,date,time


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
# CORS(app, origins=["https://hicham558.github.io","https://firepoz-s7tl.vercel.app"])  # Autoriser les requêtes depuis ton front-end
app.debug = True  # Activer le mode debug pour voir les erreurs

# Connexion à la base de données (compatible avec Railway)
def get_conn():
    url = os.environ['DATABASE_URL']
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode='require')

# Vérification de l'utilisateur (X-User-ID)
def validate_user_id():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401
    return user_id

# Configurez le logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Route pour vérifier que l'API est en ligne
@app.route('/', methods=['GET'])
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}', 500



import base64
import logging
import os
import tempfile
import sqlite3
import psycopg2
from psycopg2 import extras
from contextlib import contextmanager
from flask import jsonify, request

@contextmanager
def temp_sqlite_db():
    """Context manager for temporary SQLite database"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
    sqlite_path = tmpfile.name
    tmpfile.close()
    
    try:
        conn = sqlite3.connect(sqlite_path)
        yield conn, sqlite_path
    finally:
        conn.close()
        if os.path.exists(sqlite_path):
            os.unlink(sqlite_path)

def get_table_structure_info(pg_cur, table_name):
    """Get detailed structure info including identity columns"""
    # Get basic column info
    pg_cur.execute("""
        SELECT 
            column_name, 
            data_type, 
            column_default, 
            is_nullable,
            character_maximum_length, 
            numeric_precision, 
            numeric_scale,
            is_identity,
            identity_generation
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = pg_cur.fetchall()
    
    # Get primary key info
    pg_cur.execute("""
        SELECT column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public' 
        AND tc.table_name = %s
        AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """, (table_name,))
    
    primary_keys = [row['column_name'] for row in pg_cur.fetchall()]
    
    # Alternative check for sequences (older PostgreSQL versions)
    identity_columns = []
    for col in columns:
        if (col['is_identity'] == 'YES' or 
            ('nextval(' in str(col['column_default']).lower() and 
             '_seq' in str(col['column_default']).lower())):
            identity_columns.append(col['column_name'])
    
    return columns, primary_keys, identity_columns

def map_postgres_to_sqlite_type_v2(pg_type, column_default, column_name, char_max_length, is_identity, is_pk):
    """Enhanced mapping with explicit identity detection"""
    pg_type = pg_type.lower()
    
    # Handle IDENTITY columns explicitly
    if is_identity:
        return "INTEGER PRIMARY KEY AUTOINCREMENT", None
    
    # Map regular types
    if pg_type in ['smallint', 'integer', 'int', 'int2', 'int4']:
        sqlite_type = "INTEGER"
    elif pg_type in ['bigint', 'int8']:
        sqlite_type = "INTEGER"
    elif pg_type in ['decimal', 'numeric', 'real', 'float4', 'float8']:
        sqlite_type = "REAL"
    elif pg_type == 'double precision':
        sqlite_type = "DOUBLE PRECISION"
    elif pg_type == 'boolean':
        sqlite_type = "INTEGER"  # Use INTEGER for boolean
    elif pg_type in ['date', 'timestamp', 'timestamptz', 'time', 'timetz']:
        sqlite_type = "TEXT"
    elif pg_type in ['json', 'jsonb']:
        sqlite_type = "TEXT"
    elif pg_type.startswith('varchar'):
        if char_max_length:
            sqlite_type = f"VARCHAR({char_max_length})"
        else:
            sqlite_type = "VARCHAR(30)"
    elif pg_type in ['text']:
        sqlite_type = "TEXT"
    elif pg_type.startswith('char') or pg_type == 'character':
        if char_max_length:
            sqlite_type = f"CHAR({char_max_length})"
        else:
            sqlite_type = "CHAR(1)"
    elif pg_type == 'uuid':
        sqlite_type = "TEXT"
    elif pg_type.startswith('bytea'):
        sqlite_type = "BLOB"
    else:
        sqlite_type = "VARCHAR(30)"
    
    # Add PRIMARY KEY if this column is a primary key (but not identity)
    if is_pk and not is_identity:
        sqlite_type += " PRIMARY KEY"
    
    # Handle default values
    default_clause = None
    if column_default:
        default_str = str(column_default).lower()
        if 'true' in default_str:
            default_clause = "DEFAULT 1"
        elif 'false' in default_str:
            default_clause = "DEFAULT 0"
        elif not ('nextval(' in default_str or 'identity' in default_str):
            # Handle other default values (strings, numbers) - exclude sequences
            clean_default = column_default.replace('::text', '').replace("'", "''")
            default_clause = f"DEFAULT '{clean_default}'"
    
    return sqlite_type, default_clause

@app.route('/export', methods=['GET'])
def export_db():
    """Export PostgreSQL database to SQLite and return as base64"""
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        logging.error("Aucun en-tête X-User-ID fourni")
        return jsonify({'error': 'Missing X-User-ID header'}), 401

    pg_conn = None
    
    try:
        # Get PostgreSQL connection
        pg_conn = get_conn()  # Assumes get_conn() is defined elsewhere
        pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        with temp_sqlite_db() as (sqlite_conn, sqlite_path):
            sqlite_cur = sqlite_conn.cursor()
            
            # Enable foreign keys in SQLite
            sqlite_cur.execute("PRAGMA foreign_keys = ON")
            
            # Define expected tables
            expected_tables = [
                'categorie', 'salle', 'tables', 'utilisateur', 'fournisseur', 'comande',
                'item', 'attache', 'mouvement', 'attache2', 'attachetmp', 'client',
                'cloture', 'codebar', 'encaisse', 'item_composition', 'mouvementc',
                'observation', 'tmp', 'tva'
            ]
            
            # Get actual tables from PostgreSQL
            pg_cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            
            tables = [row['table_name'].lower() for row in pg_cur.fetchall()]
            logging.info(f"Tables trouvées dans PostgreSQL : {tables}")
            
            # Identify missing tables
            missing_tables = [t for t in expected_tables if t.lower() not in tables]
            if missing_tables:
                logging.warning(f"Tables manquantes dans PostgreSQL : {missing_tables}")
            
            if not tables:
                return jsonify({'message': 'No tables found to export'}), 200
            
            # Process each table
            exported_tables = []
            table_contents = {}
            for table in expected_tables:
                try:
                    row_count = export_table(pg_cur, sqlite_cur, table, user_id)
                    exported_tables.append(table)
                    table_contents[table] = row_count
                    logging.info(f"Table {table} exportée avec {row_count} lignes")
                except Exception as table_error:
                    logging.error(f"Erreur lors de l'exportation de la table {table}: {str(table_error)}")
                    continue
            
            sqlite_conn.commit()
            
            # Verify created tables in SQLite
            sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            created_tables = [row[0].lower() for row in sqlite_cur.fetchall()]
            logging.info(f"Tables créées dans SQLite : {created_tables}")
            
            # Read and encode the SQLite file
            with open(sqlite_path, "rb") as f:
                file_size = os.path.getsize(sqlite_path)
                
                # Check file size (limit to ~50MB base64 encoded)
                max_size = 37 * 1024 * 1024  # ~37MB raw = ~50MB base64
                if file_size > max_size:
                    return jsonify({
                        'error': f'Database too large for export: {file_size} bytes'
                    }), 413
                
                b64_db = base64.b64encode(f.read()).decode("utf-8")
            
            return jsonify({
                "db": b64_db,
                "tables_exported": exported_tables,
                "total_tables": len(exported_tables),
                "expected_tables": expected_tables,
                "missing_tables": missing_tables,
                "created_tables": created_tables,
                "table_contents": table_contents,
                "size_bytes": file_size
            })
            
    except psycopg2.Error as db_error:
        logging.error(f"Database error during export: {str(db_error)}")
        return jsonify({'error': 'Database connection error'}), 500
        
    except Exception as e:
        logging.error(f"Unexpected error during export: {str(e)}")
        return jsonify({'error': 'Export failed'}), 500
        
    finally:
        if pg_conn:
            pg_conn.close()

def export_table(pg_cur, sqlite_cur, table_name, user_id):
    """Export a single table from PostgreSQL to SQLite with user_id filtering and NUMERO_UTIL exclusion"""
    
    # Define tables that should be filtered by user_id
    user_id_tables = ['utilisateur', 'comande', 'mouvement', 'mouvementc']
    
    # Get table structure, excluding NUMERO_UTIL
    columns, primary_keys, identity_columns = get_table_structure_info(pg_cur, table_name)
    columns = [col for col in columns if col['column_name'].lower() != 'numero_util']
    
    if not columns:
        logging.warning(f"No columns found for table {table_name} after excluding NUMERO_UTIL")
        return 0
    
    logging.info(f"Table {table_name}: PK={primary_keys}, Identity={identity_columns}")
    
    # Build CREATE TABLE statement
    col_defs = []
    column_info = []
    
    for col in columns:
        col_name = col['column_name']
        is_identity = col_name in identity_columns
        is_pk = col_name in primary_keys
        
        sqlite_type, default_clause = map_postgres_to_sqlite_type_v2(
            col['data_type'], 
            col['column_default'],
            col_name,
            col['character_maximum_length'],
            is_identity,
            is_pk
        )
        
        # Build column definition with UPPERCASE column name
        col_def = f'{col_name.upper()} {sqlite_type}'
        
        # Handle NOT NULL and DEFAULT for non-identity, non-pk columns
        if not is_identity and not is_pk:
            if (col['is_nullable'] == 'NO' and 
                col['column_default'] is not None and 
                not ('nextval(' in str(col['column_default']).lower())):
                col_def += " NOT NULL"
            
            if default_clause:
                col_def += f" {default_clause}"
        
        col_defs.append(col_def)
        column_info.append({
            'name': col_name,
            'type': col['data_type'],
            'default': col['column_default'],
            'nullable': col['is_nullable'],
            'is_identity': is_identity
        })
    
    # Create table with UPPERCASE table name - quote if reserved word
    table_name_upper = table_name.upper()
    if table_name_upper in ['TABLES', 'ORDER', 'GROUP', 'INDEX', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']:
        table_name_quoted = f'"{table_name_upper}"'
    else:
        table_name_quoted = table_name_upper
    
    create_sql = f'CREATE TABLE {table_name_quoted} ({", ".join(col_defs)})'
    
    try:
        sqlite_cur.execute(create_sql)
        logging.info(f"Created table: {table_name_upper}")
        logging.info(f"SQL: {create_sql}")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {str(e)}")
        logging.error(f"SQL: {create_sql}")
        raise
    
    # Copy data in batches with user_id filtering
    batch_size = 1000
    offset = 0
    total_rows = 0
    
    # Adjust SELECT query based on whether table needs user_id filtering
    if table_name.lower() in user_id_tables:
        query = f'SELECT {", ".join([f"\"{col['name']}\"" for col in columns])} FROM "{table_name}" WHERE user_id = %s LIMIT %s OFFSET %s'
        params = (user_id, batch_size, offset)
    else:
        query = f'SELECT {", ".join([f"\"{col['name']}\"" for col in columns])} FROM "{table_name}" LIMIT %s OFFSET %s'
        params = (batch_size, offset)
    
    while True:
        pg_cur.execute(query, params)
        rows = pg_cur.fetchall()
        
        if not rows:
            break
        
        # Process rows
        processed_rows = []
        for row in rows:
            processed_row = []
            for col_info in column_info:
                col_name = col_info['name']
                value = row[col_name]
                
                # Handle None/NULL values with defaults
                if value is None:
                    if col_info['default'] and not col_info['is_identity']:
                        default_str = str(col_info['default']).lower()
                        if 'true' in default_str:
                            value = 1
                        elif 'false' in default_str:
                            value = 0
                
                # Convert boolean values to integers
                elif isinstance(value, bool):
                    value = 1 if value else 0
                
                processed_row.append(value)
            
            processed_rows.append(tuple(processed_row))
        
        # Insert batch using UPPERCASE table name
        placeholders = ",".join(["?"] * len(column_info))
        try:
            sqlite_cur.executemany(
                f'INSERT INTO {table_name_quoted} VALUES ({placeholders})',
                processed_rows
            )
            total_rows += len(processed_rows)
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {str(e)}")
            logging.error(f"Sample row: {processed_rows[0] if processed_rows else 'No rows'}")
            raise
        
        offset += batch_size
        
        if offset % 10000 == 0:
            logging.info(f"Exported {offset} rows from {table_name_upper}")
    
    logging.info(f"Successfully exported {total_rows} rows from table {table_name_upper}")
    return total_rows
    
    @app.route('/valider_vendeur', methods=['POST'])
def valider_vendeur():
    """
    Endpoint pour valider un vendeur en vérifiant son nom et son mot de passe.
    Reçoit un JSON avec 'nom' et 'password2'.
    Retourne les informations du vendeur si valide, sinon une erreur.
    """
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id  # Retourne l'erreur 401 si user_id invalide

    data = request.get_json()
    if not data or 'nom' not in data or 'password2' not in data:
        logger.error("Données invalides: 'nom' ou 'password2' manquant")
        return jsonify({"erreur": "Le nom et le mot de passe sont requis"}), 400

    nom = data.get('nom')
    password2 = data.get('password2')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Requête pour vérifier l'utilisateur
        cur.execute("""
            SELECT numero_util, nom, statue 
            FROM utilisateur 
            WHERE nom = %s AND password2 = %s AND user_id = %s
        """, (nom, password2, user_id))
        
        utilisateur = cur.fetchone()
        
        cur.close()
        conn.close()

        if not utilisateur:
            logger.error(f"Échec authentification: nom={nom}, user_id={user_id}")
            return jsonify({"erreur": "Nom ou mot de passe incorrect"}), 401

        logger.info(f"Vendeur validé: numero_util={utilisateur['numero_util']}, nom={nom}")
        return jsonify({
            "statut": "Vendeur validé",
            "utilisateur": {
                "numero_util": utilisateur['numero_util'],
                "nom": utilisateur['nom'],
                "statut": utilisateur['statue']
            }
        }), 200

    except Exception as e:
        logger.error(f"Erreur lors de la validation du vendeur: {str(e)}", exc_info=True)
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()
        return jsonify({"erreur": str(e)}), 500


@app.route('/rechercher_produit_codebar', methods=['GET'])
def rechercher_produit_codebar():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    codebar = request.args.get('codebar')
    if not codebar:
        return jsonify({'erreur': 'Code-barres requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Rechercher d'abord par code-barres principal dans item
        cur.execute("""
            SELECT numero_item, bar, designation, prix, prixba, qte
            FROM item
            WHERE bar = %s AND user_id = %s
        """, (codebar, user_id))
        produit = cur.fetchone()

        if produit:
            cur.close()
            conn.close()
            return jsonify({
                'statut': 'trouvé',
                'type': 'principal',
                'produit': produit
            }), 200

        # Si non trouvé, rechercher dans codebar pour un code-barres lié
        cur.execute("""
            SELECT i.numero_item, i.bar, i.designation, i.prix, i.prixba, i.qte
            FROM codebar c
            JOIN item i ON c.bar = i.numero_item::varchar
            WHERE c.bar2 = %s AND i.user_id = %s
        """, (codebar, user_id))
        produit = cur.fetchone()

        if produit:
            cur.close()
            conn.close()
            return jsonify({
                'statut': 'trouvé',
                'type': 'lié',
                'produit': produit
            }), 200

        cur.close()
        conn.close()
        return jsonify({'erreur': 'Produit non trouvé'}), 404

    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500
@app.route('/ajouter_codebar_lie', methods=['POST'])
def ajouter_codebar_lie():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('barcode')

    if not numero_item:
        return jsonify({'erreur': 'numero_item est requis'}), 400

    try:
        numero_item = int(numero_item)
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier que l'item existe
        cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Vérifier que bar2 n'existe pas déjà
        if bar2:
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar2, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres lié existe déjà pour cet utilisateur'}), 409

        # Générer un bar2 si non fourni
        cur.execute("SELECT bar2 FROM codebar WHERE user_id = %s", (user_id,))
        existing_barcodes = cur.fetchall()
        used_numbers = []
        for code in existing_barcodes:
            bar_num = int(code['bar2'][1:12]) if code['bar2'].startswith('1') and len(code['bar2']) == 13 and code['bar2'][1:12].isdigit() else 0
            used_numbers.append(bar_num)

        next_number = 1
        used_numbers = sorted(set(used_numbers))
        for num in used_numbers:
            if num == next_number:
                next_number += 1
            elif num > next_number:
                break

        if not bar2:
            code12 = f"1{next_number:011d}"
            check_digit = calculate_ean13_check_digit(code12)
            bar2 = f"{code12}{check_digit}"
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar2, user_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà pour cet utilisateur'}), 409

        cur.execute("LOCK TABLE codebar IN EXCLUSIVE MODE")
        cur.execute(
            "INSERT INTO codebar (bar2, bar, user_id) VALUES (%s, %s, %s) RETURNING n",
            (bar2, numero_item, user_id)
        )
        codebar_id = cur.fetchone()['n']

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Code-barres lié ajouté', 'id': codebar_id, 'bar2': bar2}), 201
    except ValueError:
        conn.rollback()
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500



@app.route('/liste_codebar_lies', methods=['GET'])
def liste_codebar_lies():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    numero_item = request.args.get('numero_item')
    if not numero_item:
        return jsonify({'erreur': 'numero_item est requis'}), 400

    try:
        numero_item = int(numero_item)
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier que l'item existe
        cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Récupérer les codes-barres liés en castant bar en INTEGER
        cur.execute("SELECT bar2 FROM codebar WHERE bar::INTEGER = %s AND user_id = %s ORDER BY n", (numero_item, user_id))
        linked_barcodes = [row['bar2'] for row in cur.fetchall()]

        cur.close()
        conn.close()
        return jsonify({'linked_barcodes': linked_barcodes}), 200
    except ValueError:
        if conn:
            conn.close()
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500


@app.route('/supprimer_codebar_lie', methods=['POST'])
def supprimer_codebar_lie():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('bar2')

    if not numero_item or not bar2:
        return jsonify({'erreur': 'numero_item et bar2 sont requis'}), 400

    try:
        # Convert numero_item to string to match VARCHAR type of bar in codebar
        numero_item_str = str(numero_item)

        conn = get_conn()
        conn.autocommit = False
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Vérifier que l'item existe (numero_item dans item est INTEGER)
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (int(numero_item), user_id))
            item = cur.fetchone()
            if not item:
                raise Exception('Produit non trouvé')

            # Vérifier que le code-barres lié existe (bar est VARCHAR)
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))
            if not cur.fetchone():
                raise Exception('Code-barres lié non trouvé pour ce produit')

            # Supprimer le code-barres lié
            cur.execute("DELETE FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))

            conn.commit()
            return jsonify({'statut': 'Code-barres lié supprimé'}), 200
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500
# --- Clients ---
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):  # Si validate_user_id retourne une erreur
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        clients = [
            {
                'numero_clt': row[0],
                'nom': row[1],
                'solde': float(row[2]) if row[2] is not None else 0.0,
                'reference': row[3],
                'contact': row[4] or '',
                'adresse': row[5] or ''
            }
            for row in rows
        ]
        return jsonify(clients)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Compter les clients pour cet user_id
        cur.execute("SELECT COUNT(*) FROM client WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        reference = f"C{count + 1}"

        # Insérer avec solde = '0.00'
        cur.execute(
            "INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_clt",
            (nom, '0.00', reference, contact, adresse, user_id)
        )
        client_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté', 'id': client_id, 'reference': reference}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500


@app.route('/modifier_client/<numero_clt>', methods=['PUT'])
def modifier_client(numero_clt):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE client SET nom = %s, contact = %s, adresse = %s WHERE numero_clt = %s AND user_id = %s RETURNING numero_clt",
            (nom, contact, adresse, numero_clt, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Client non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client modifié'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500


# --- Suppression Client ---
@app.route('/supprimer_client/<numero_clt>', methods=['DELETE'])
def supprimer_client(numero_clt):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM client WHERE numero_clt = %s AND user_id = %s", (numero_clt, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Client non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500



# --- Fournisseurs ---
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        fournisseurs = [
            {
                'numero_fou': row[0],
                'nom': row[1],
                'solde': float(row[2]) if row[2] is not None else 0.0,
                'reference': row[3],
                'contact': row[4] or '',
                'adresse': row[5] or ''
            }
            for row in rows
        ]
        return jsonify(fournisseurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_fournisseur/<numero_fou>', methods=['PUT'])
def modifier_fournisseur(numero_fou):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE fournisseur SET nom = %s, contact = %s, adresse = %s WHERE numero_fou = %s AND user_id = %s RETURNING numero_fou",
            (nom, contact, adresse, numero_fou, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Fournisseur non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur modifié'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_fournisseur', methods=['POST'])
def ajouter_fournisseur():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Compter les fournisseurs
        cur.execute("SELECT COUNT(*) FROM fournisseur WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        reference = f"F{count + 1}"

        # Insérer avec solde = '0.00'
        cur.execute(
            "INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_fou",
            (nom, '0.00', reference, contact, adresse, user_id)
        )
        fournisseur_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur ajouté', 'id': fournisseur_id, 'reference': reference}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500



# --- Suppression fournisseur ---
@app.route('/supprimer_fournisseur/<numero_fou>', methods=['DELETE'])
def supprimer_fournisseur(numero_fou):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_fou, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Client non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500


# --- Produits ---
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_item, bar, designation, qte, prix, prixba, ref FROM item WHERE user_id = %s ORDER BY designation", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        produits = [
            {
                'NUMERO_ITEM': row[0],
                'BAR': row[1],
                'DESIGNATION': row[2],
                'QTE': row[3],
                'PRIX': float(row[4]) if row[4] is not None else 0.0,
                'PRIXBA': row[5] or '0.00',
                'REF': row[6] or ''
            }
            for row in rows
        ]
        return jsonify(produits)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_item/<numero_item>', methods=['PUT'])
def modifier_item(numero_item):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')

    if not all([designation, bar, prix is not None, qte is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, bar, prix, qte)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        cur = conn.cursor()
        # Vérifier l'unicité de bar (sauf pour cet item)
        cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s AND numero_item != %s", (bar, user_id, numero_item))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Ce code-barres est déjà utilisé'}), 409

        cur.execute(
            "UPDATE item SET designation = %s, bar = %s, prix = %s, qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s RETURNING numero_item",
            (designation, bar, prix, qte, prixba or '0.00', numero_item, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Produit modifié'}), 200
    except ValueError:
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# Calcul du chiffre de contrôle EAN-13
def calculate_ean13_check_digit(code12):
    """Calcule le chiffre de contrôle pour un code EAN-13 à partir d'un code de 12 chiffres."""
    digits = [int(d) for d in code12]
    odd_sum = sum(digits[0::2])
    even_sum = sum(digits[1::2])
    total = odd_sum * 3 + even_sum
    next_multiple_of_10 = (total + 9) // 10 * 10
    check_digit = next_multiple_of_10 - total
    return check_digit

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')

    if not all([designation, prix is not None, qte is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, prix, qte)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Verrouiller pour éviter les conflits
        cur.execute("LOCK TABLE item IN EXCLUSIVE MODE")

        # Si bar est fourni, vérifier son unicité pour ce user_id dans la table item
        if bar:
            cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres existe déjà pour cet utilisateur'}), 409

            # Vérifier si le code-barres existe dans la table codebar pour ce user_id
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres existe déjà comme code-barres lié pour cet utilisateur'}), 409

        # Trouver le prochain numéro disponible pour ref et bar
        cur.execute("SELECT ref, bar FROM item WHERE user_id = %s ORDER BY ref", (user_id,))
        existing_items = cur.fetchall()
        used_numbers = []
        for item in existing_items:
            # Extraire le numéro de ref (ex. P3 → 3)
            ref_num = int(item['ref'][1:]) if item['ref'].startswith('P') and item['ref'][1:].isdigit() else 0
            # Extraire le numéro de bar (ex. 100000000003X → 3)
            bar_num = int(item['bar'][1:12]) if item['bar'].startswith('1') and len(item['bar']) == 13 and item['bar'][1:12].isdigit() else 0
            used_numbers.append(max(ref_num, bar_num))

        # Trouver le plus petit numéro non utilisé à partir de 1
        next_number = 1
        used_numbers = sorted(set(used_numbers))
        for num in used_numbers:
            if num == next_number:
                next_number += 1
            elif num > next_number:
                break

        # Générer ref
        ref = f"P{next_number}"

        # Insérer le produit (utiliser une valeur temporaire pour bar si vide)
        temp_bar = bar if bar else 'TEMP_BAR'
        cur.execute(
            "INSERT INTO item (designation, bar, prix, qte, prixba, ref, user_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING numero_item",
            (designation, temp_bar, prix, qte, prixba or '0.00', ref, user_id)
        )
        item_id = cur.fetchone()['numero_item']

        # Si bar est vide, générer un code EAN-13 basé sur next_number
        if not bar:
            # Créer un code de 12 chiffres
            code12 = f"1{next_number:011d}"  # Ex. next_number=1 → "100000000001"
            check_digit = calculate_ean13_check_digit(code12)
            bar = f"{code12}{check_digit}"  # Ex. "1000000000016"

            # Vérifier l'unicité du code EAN-13 généré dans item
            cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s AND numero_item != %s", 
                       (bar, user_id, item_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà pour cet utilisateur'}), 409

            # Vérifier l'unicité du code EAN-13 généré dans codebar
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà comme code-barres lié pour cet utilisateur'}), 409

            # Mettre à jour l'enregistrement avec le code EAN-13
            cur.execute(
                "UPDATE item SET bar = %s WHERE numero_item = %s AND user_id = %s",
                (bar, item_id, user_id)
            )

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Item ajouté', 'id': item_id, 'ref': ref, 'bar': bar}), 201
    except ValueError:
        conn.rollback()
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

# --- Suppression Produit ---
@app.route('/supprimer_item/<numero_item>', methods=['DELETE'])
def supprimer_item(numero_item):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Produit supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Retourne l'erreur 401 si user_id est invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de vente invalides, utilisateur ou mot de passe manquant")
        return jsonify({"error": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')  # Par défaut "espece"
    amount_paid = float(data.get('amount_paid', 0))  # Montant versé, 0 par défaut
    lignes = data['lignes']
    numero_util = data['numero_util']
    password2 = data['password2']
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    # Validation du mode de paiement et du client
    if payment_mode == 'a_terme' and numero_table == 0:
        print("Erreur: Vente à terme sans client sélectionné")
        return jsonify({"error": "Veuillez sélectionner un client pour une vente à terme"}), 400

    if payment_mode == 'a_terme' and amount_paid < 0:
        print("Erreur: Montant versé négatif")
        return jsonify({"error": "Le montant versé ne peut pas être négatif"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("""
            SELECT Password2
            FROM utilisateur
            WHERE numero_util = %s
        """, (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer le dernier compteur pour cette nature
        cur.execute("""
            SELECT COALESCE(MAX(compteur), 0) as max_compteur
            FROM comande
            WHERE nature = %s
        """, (nature,))
        compteur = cur.fetchone()['max_compteur'] + 1
        print(f"Compteur calculé: nature={nature}, compteur={compteur}")

        # Insérer la commande avec numero_util
        cur.execute("""
            INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id, numero_util)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_comande
        """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id, numero_util))
        numero_comande = cur.fetchone()['numero_comande']
        print(f"Commande insérée: numero_comande={numero_comande}, nature={nature}, connection1=-1, compteur={compteur}, numero_util={numero_util}")

        # Insérer les lignes et mettre à jour le stock
        for ligne in lignes:
            cur.execute("""
                INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id,
                  numero_comande,
                  ligne.get('numero_item'),
                  ligne.get('quantite'),
                  ligne.get('prixt'),
                  ligne.get('remarque'),
                  ligne.get('prixbh'),
                  0))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s", (ligne.get('quantite'), ligne.get('numero_item')))

        # Mise à jour du solde du client si vente à terme
        if payment_mode == 'a_terme' and numero_table != 0:
            total_sale = sum(float(ligne.get('prixt', 0)) for ligne in lignes)
            solde_change = amount_paid - total_sale  # Dette = montant versé - total (négatif si dette)

            # Récupérer le solde actuel du client
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s", (numero_table,))
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client avec numero_clt={numero_table} non trouvé")

            # Convertir le solde (VARCHAR) en float, ou 0 si vide/invalide
            current_solde = float(client['solde']) if client['solde'] and client['solde'].strip() else 0.0
            new_solde = current_solde + solde_change  # Soustraire la dette (solde_change est négatif)

            # Convertir le nouveau solde en chaîne avec 2 décimales
            new_solde_str = f"{new_solde:.2f}"

            # Mettre à jour le solde dans la table client
            cur.execute("""
                UPDATE client
                SET solde = %s
                WHERE numero_clt = %s
            """, (new_solde_str, numero_table))
            print(f"Solde client mis à jour: numero_clt={numero_table}, solde_change={solde_change}, amount_paid={amount_paid}, new_solde={new_solde_str}")

        conn.commit()
        print(f"Vente validée: numero_comande={numero_comande}, {len(lignes)} lignes")
        return jsonify({"numero_comande": numero_comande}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur validation vente: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/client_solde', methods=['GET'])
def client_solde():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT numero_clt, COALESCE(solde, '0.00') as solde
            FROM client
            WHERE user_id = %s
        """, (user_id,))
        soldes = cur.fetchall()
        print(f"Soldes récupérés: {len(soldes)} clients")
        return jsonify(soldes), 200
    except Exception as e:
        print(f"Erreur récupération soldes: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                c.numero_comande,
                c.date_comande,
                c.nature,
                c.numero_table,
                cl.nom AS client_nom,
                c.numero_util,
                u.nom AS utilisateur_nom,
                a.numero_item,
                a.quantite,
                CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) AS prixt,
                a.remarque,
                i.designation
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            LEFT JOIN utilisateur u ON c.numero_util = u.numero_util
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        if numero_util:
            if numero_util == '0':
                pass
            else:
                query += " AND c.numero_util = %s"
                params.append(int(numero_util))

        query += " ORDER BY c.numero_comande DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        tickets = []
        bons = []
        total = 0.0
        ventes_map = {}

        for row in rows:
            if row['numero_comande'] not in ventes_map:
                ventes_map[row['numero_comande']] = {
                    'numero_comande': row['numero_comande'],
                    'date_comande': row['date_comande'].isoformat(),
                    'nature': row['nature'],
                    'client_nom': 'Comptoir' if row['numero_table'] == 0 else row['client_nom'],
                    'utilisateur_nom': row['utilisateur_nom'] or 'N/A',
                    'lignes': []
                }

            ventes_map[row['numero_comande']]['lignes'].append({
                'numero_item': row['numero_item'],
                'designation': row['designation'],
                'quantite': row['quantite'],
                'prixt': str(row['prixt']),  # Retourne en tant que chaîne pour respecter le type d'origine
                'remarque': row['remarque'] or ''
            })

            total += float(row['prixt'])  # Conversion en float pour le calcul

        for vente in ventes_map.values():
            if vente['nature'] == 'TICKET':
                tickets.append(vente)
            elif vente['nature'] == 'BON DE L.':
                bons.append(vente)

        cur.close()
        conn.close()

        return jsonify({
            'tickets': tickets,
            'bons': bons,
            'total': f"{total:.2f}"
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération ventes du jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Articles les plus vendus ---
@app.route('/articles_plus_vendus', methods=['GET'])
def articles_plus_vendus():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        # Construire la requête SQL
        query = """
            SELECT 
                i.numero_item,
                i.designation,
                SUM(a.quantite) AS quantite,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)) AS total_vente
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        # Filtre par client
        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        # Filtre par utilisateur
        if numero_util and numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(int(numero_util))

        query += """
            GROUP BY i.numero_item, i.designation
            ORDER BY quantite DESC
            LIMIT 10
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        # Formater la réponse
        articles = [
            {
                'numero_item': row['numero_item'],
                'designation': row['designation'] or 'N/A',
                'quantite': int(row['quantite'] or 0),
                'total_vente': f"{float(row['total_vente'] or 0):.2f}"
            }
            for row in rows
        ]

        return jsonify(articles), 200

    except Exception as e:
        print(f"Erreur récupération articles plus vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util', '0')  # Par défaut : Tous les utilisateurs

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates (30 jours si aucune date spécifique)
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = date_end - timedelta(days=30)

        # Construire la requête SQL
        query = """
            SELECT 
                DATE(c.date_comande) AS date,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0') AS FLOAT))) AS profit
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        # Filtre par client
        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        # Filtre par utilisateur : uniquement si numero_util n'est pas '0'
        if numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(int(numero_util))

        query += """
            GROUP BY DATE(c.date_comande)
            ORDER BY DATE(c.date_comande) DESC
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        # Formater la réponse
        profits = [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'profit': f"{float(row['profit'] or 0):.2f}"
            }
            for row in rows
        ]

        return jsonify(profits), 200

    except Exception as e:
        print(f"Erreur récupération profit par date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/dashboard', methods=['GET'])
def dashboard():
    userId = validate_user_id()
    if not isinstance(userId, str):
        return userId

    period = request.args.get('period', 'day')
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Define the date range
        if period == 'week':
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        # Query for main KPIs
        query_kpi = """
            SELECT 
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS total_ca,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0') AS FLOAT))), 0) AS total_profit,
                COUNT(DISTINCT c.numero_comande) AS sales_count
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
        """
        cur.execute(query_kpi, (userId, date_start, date_end))
        kpi_data = cur.fetchone()

        # Query for low stock items
        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE user_id = %s AND qte < 10", (userId,))
        low_stock_count = cur.fetchone()['low_stock']

        # Query for top client
        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS client_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY cl.nom
            ORDER BY client_ca DESC
            LIMIT 1
        """
        cur.execute(query_top_client, (userId, date_start, date_end))
        top_client = cur.fetchone()

        # Query for chart data (CA per day)
        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        cur.execute(query_chart, (userId, date_start, date_end))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

        # Format chart data
        chart_labels = []
        chart_values = []
        current_date = date_start
        while current_date <= date_end:
            chart_labels.append(current_date.strftime('%Y-%m-%d'))
            daily_ca = next((row['daily_ca'] for row in chart_data if row['sale_date'].strftime('%Y-%m-%d') == current_date.strftime('%Y-%m-%d')), 0)
            chart_values.append(float(daily_ca))
            current_date += timedelta(days=1)

        return jsonify({
            'total_ca': float(kpi_data['total_ca'] or 0),
            'total_profit': float(kpi_data['total_profit'] or 0),
            'sales_count': int(kpi_data['sales_count'] or 0),
            'low_stock_items': int(low_stock_count or 0),
            'top_client': {
                'name': top_client['nom'] if top_client else 'N/A',
                'ca': float(top_client['client_ca'] or 0) if top_client else 0
            },
            'chart_data': {
                'labels': chart_labels,
                'values': chart_values
            }
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération KPI: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
# GET /liste_utilisateurs
@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):  # Si validate_user_id retourne une erreur
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT numero_util, nom, statue 
            FROM utilisateur 
            WHERE user_id = %s 
            ORDER BY nom
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        utilisateurs = [
            {
                'numero': row[0],  # Changed from 'id' to 'numero' to match frontend
                'nom': row[1],
                'statut': row[2]  # statue becomes statut
            }
            for row in rows
        ]
        return jsonify(utilisateurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500
@app.route('/modifier_utilisateur/<int:numero_util>', methods=['PUT'])
def modifier_utilisateur(numero_util):
    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')  # Optional
    statue = data.get('statue')
    user_id = data.get('user_id')  # New field from payload

    if not all([nom, statue, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    # Optional: Verify user_id matches X-User-ID header
    x_user_id = request.headers.get('X-User-ID')
    if x_user_id and x_user_id != user_id:
        return jsonify({'erreur': 'user_id non autorisé'}), 403

    try:
        conn = get_conn()
        cur = conn.cursor()
        if password2:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, password2 = %s, statue = %s, user_id = %s WHERE numero_util = %s",
                (nom, password2, statue, user_id, numero_util)
            )
        else:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, statue = %s, user_id = %s WHERE numero_util = %s",
                (nom, statue, user_id, numero_util)
            )
        if cur.rowcount == 0:
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur modifié'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')
    statue = data.get('statue')
    user_id = data.get('user_id')  # New field from payload

    # Validate all required fields
    if not all([nom, password2, statue, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password2, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    # Optional: Verify user_id matches X-User-ID header for security
    x_user_id = request.headers.get('X-User-ID')
    if x_user_id and x_user_id != user_id:
        return jsonify({'erreur': 'user_id non autorisé'}), 403

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO utilisateur (nom, password2, statue, user_id) VALUES (%s, %s, %s, %s) RETURNING numero_util",
            (nom, password2, statue, user_id)
        )
        numero_util = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur ajouté', 'id': numero_util}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# DELETE /supprimer_utilisateur/<numero_util>
@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM utilisateur WHERE numero_util = %s", (numero_util,))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/stock_value', methods=['GET'])
def valeur_stock():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                SUM(COALESCE(CAST(NULLIF(prixba, '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_achat,
                SUM(COALESCE(CAST(NULLIF(prix, '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_vente
            FROM item 
            WHERE user_id = %s
        """, (user_id,))
        result = cur.fetchone()

        # Vérifier si result est None (aucune donnée)
        if result is None:
            valeur_achat = 0.0
            valeur_vente = 0.0
        else:
            valeur_achat = float(result['valeur_achat'] or 0)
            valeur_vente = float(result['valeur_vente'] or 0)

        zakat = valeur_vente * 0.025  # 2.5% de la valeur de vente

        return jsonify({
            'valeur_achat': f"{valeur_achat:.2f}",
            'valeur_vente': f"{valeur_vente:.2f}",
            'zakat': f"{zakat:.2f}"
        }), 200
    except Exception as e:
        print(f"Erreur récupération valeur stock: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/valider_reception', methods=['POST'])
def valider_reception():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de réception invalides")
        return jsonify({"error": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    lignes = data['lignes']
    nature = "Bon de réception"

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("SELECT numero_fou FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        if not cur.fetchone():
            print(f"Erreur: Fournisseur {numero_four} non trouvé")
            return jsonify({"error": "Fournisseur non trouvé"}), 400

        # Insérer le mouvement principal
        cur.execute("""
            INSERT INTO mouvement (date_m, etat_m, numero_four, refdoc, vers, nature, connection1, numero_util, cheque, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mouvement
        """, (datetime.utcnow(), "clôture", numero_four, "", "", nature, 0, numero_util, "", user_id))
        numero_mouvement = cur.fetchone()['numero_mouvement']

        # Mettre à jour refdoc
        cur.execute("UPDATE mouvement SET refdoc = %s WHERE numero_mouvement = %s", 
                    (str(numero_mouvement), numero_mouvement))

        # Calculer le coût total
        total_cost = 0.0
        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if qtea <= 0:
                raise Exception("La quantité ajoutée doit être positive")

            # Vérifier l'article
            cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            prixba = float(item['prixba'] or 0)

            nqte = current_qte + qtea
            total_cost += qtea * prixbh

            # Insérer les détails dans ATTACHE2 (avec user_id ajouté)
            cur.execute("""
                INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (numero_item, numero_mouvement, qtea, nqte, str(prixbh)[:30], str(prixba)[:30], True, user_id))

            # Mettre à jour le stock et le prix d'achat
            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (nqte, str(prixbh), numero_item, user_id))

        # Mettre à jour le solde du fournisseur
        cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            raise Exception(f"Fournisseur {numero_four} non trouvé")

        current_solde = float(fournisseur['solde']) if fournisseur['solde'] else 0.0
        new_solde = current_solde - total_cost
        new_solde_str = f"{new_solde:.2f}"

        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        print(f"Solde fournisseur mis à jour: numero_fou={numero_four}, total_cost={total_cost}, new_solde={new_solde_str}")

        conn.commit()
        print(f"Réception validée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"numero_mouvement": numero_mouvement}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur validation réception: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/receptions_jour', methods=['GET'])
def receptions_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_util = request.args.get('numero_util')
    numero_four = request.args.get('numero_four', '')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                m.numero_mouvement,
                m.date_m,
                m.nature,
                m.numero_four,
                f.nom AS fournisseur_nom,
                m.numero_util,
                u.nom AS utilisateur_nom,
                a2.numero_item,
                a2.qtea,
                CAST(COALESCE(NULLIF(a2.nprix, ''), '0') AS FLOAT) AS nprix,
                i.designation
            FROM mouvement m
            LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou
            LEFT JOIN utilisateur u ON m.numero_util = u.numero_util
            JOIN attache2 a2 ON m.numero_mouvement = a2.numero_mouvement
            JOIN item i ON a2.numero_item = i.numero_item
            WHERE m.user_id = %s 
            AND m.date_m >= %s 
            AND m.date_m <= %s
            AND m.nature = 'Bon de réception'
        """
        params = [user_id, date_start, date_end]

        if numero_util and numero_util != '0':
            query += " AND m.numero_util = %s"
            params.append(int(numero_util))
        if numero_four and numero_four != '':
            query += " AND m.numero_four = %s"
            params.append(numero_four)

        query += " ORDER BY m.numero_mouvement DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        receptions = []
        total = 0.0
        receptions_map = {}

        for row in rows:
            if row['numero_mouvement'] not in receptions_map:
                receptions_map[row['numero_mouvement']] = {
                    'numero_mouvement': row['numero_mouvement'],
                    'date_m': row['date_m'].isoformat(),
                    'nature': row['nature'],
                    'fournisseur_nom': row['fournisseur_nom'] or 'N/A',
                    'utilisateur_nom': row['utilisateur_nom'] or 'N/A',
                    'lignes': []
                }

            receptions_map[row['numero_mouvement']]['lignes'].append({
                'numero_item': row['numero_item'],
                'designation': row['designation'],
                'qtea': row['qtea'],
                'nprix': str(row['nprix']),
                'total_ligne': str(float(row['qtea']) * float(row['nprix']))
            })

            total += float(row['qtea']) * float(row['nprix'])

        receptions = list(receptions_map.values())

        cur.close()
        conn.close()

        return jsonify({
            'receptions': receptions,
            'total': f"{total:.2f}"
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération réceptions: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Versements ---

@app.route('/ajouter_versement', methods=['POST'])
def ajouter_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de versement invalides")
        return jsonify({"error": "Type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    type_versement = data.get('type')  # 'C' pour client, 'F' pour fournisseur
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            return jsonify({"error": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier client ou fournisseur
        if type_versement == 'C':
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:  # 'F'
            cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        if not entity:
            print(f"Erreur: {'Client' if type_versement == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"error": f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Mettre à jour le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde + montant  # Montant peut être positif ou négatif
        new_solde_str = f"{new_solde:.2f}"

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Insérer le versement dans MOUVEMENTC
        now = datetime.utcnow()  # Ex. 2025-05-15 02:12:00.123456
        cur.execute(
            """
            INSERT INTO MOUVEMENTC (date_mc, time_mc, montant, justificatif, numero_util, origine, cf, numero_cf, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mc
            """,
            (now.date(), now, f"{montant:.2f}", justificatif,
             numero_util, origine, type_versement, numero_cf, user_id)
        )
        numero_mc = cur.fetchone()['numero_mc']

        conn.commit()
        print(f"Versement ajouté: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"numero_mc": numero_mc, "statut": "Versement ajouté"}), 201

    except ValueError:
        return jsonify({"error": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur ajout versement: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/historique_versements', methods=['GET'])
def historique_versements():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    type_versement = request.args.get('type')  # 'C', 'F', ou vide pour tous

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                mc.numero_mc,
                mc.date_mc,
                mc.montant,
                mc.justificatif,
                mc.cf,
                mc.numero_cf,
                mc.numero_util,
                COALESCE(cl.nom, f.nom) AS nom_cf,
                u.nom AS utilisateur_nom
            FROM MOUVEMENTC mc
            LEFT JOIN client cl ON mc.cf = 'C' AND mc.numero_cf = cl.numero_clt
            LEFT JOIN fournisseur f ON mc.cf = 'F' AND mc.numero_cf = f.numero_fou
            LEFT JOIN utilisateur u ON mc.numero_util = u.numero_util
            WHERE mc.user_id = %s
            AND mc.date_mc >= %s
            AND mc.date_mc <= %s
            AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
        """
        params = [user_id, date_start, date_end]

        if type_versement in ['C', 'F']:
            query += " AND mc.cf = %s"
            params.append(type_versement)

        query += " ORDER BY mc.date_mc DESC, mc.time_mc DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': str(row['montant']),
                'justificatif': row['justificatif'] or '',
                'type': 'Client' if row['cf'] == 'C' else 'Fournisseur',
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        cur.close()
        conn.close()
        return jsonify(versements), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération historique versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

@app.route('/annuler_versement', methods=['DELETE'])
def annuler_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'numero_mc' not in data or 'type' not in data or 'numero_cf' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données d'annulation invalides")
        return jsonify({"error": "Numéro de versement, type, numéro client/fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer le versement
        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')", 
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            print(f"Erreur: Versement {numero_mc} non trouvé")
            return jsonify({"error": "Versement non trouvé"}), 404

        montant = float(versement['montant'])

        # Déterminer la table et la colonne ID
        if versement['cf'] == 'C':
            table = 'client'
            id_column = 'numero_clt'
        else:  # 'F'
            table = 'fournisseur'
            id_column = 'numero_fou'

        # Vérifier l'entité
        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            print(f"Erreur: {'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"error": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Restaurer le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - montant  # Inverser l'effet du versement
        new_solde_str = f"{new_solde:.2f}"

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Supprimer le versement
        cur.execute("DELETE FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s", (numero_mc, user_id))

        conn.commit()
        print(f"Versement annulé: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"statut": "Versement annulé"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur annulation versement: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_versement', methods=['PUT'])
def modifier_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'numero_mc' not in data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de modification invalides")
        return jsonify({"error": "Numéro de versement, type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            return jsonify({"error": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer le versement existant
        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')", 
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            print(f"Erreur: Versement {numero_mc} non trouvé")
            return jsonify({"error": "Versement non trouvé"}), 404

        old_montant = float(versement['montant'])

        # Déterminer la table et la colonne ID
        if versement['cf'] == 'C':
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:  # 'F'
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        # Vérifier l'entité
        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            print(f"Erreur: {'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"error": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Ajuster le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - old_montant + montant  # Annuler l'ancien montant et appliquer le nouveau
        new_solde_str = f"{new_solde:.2f}"

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Mettre à jour le versement
        now = datetime.utcnow()
        cur.execute(
            """
            UPDATE MOUVEMENTC 
            SET montant = %s, justificatif = %s, date_mc = %s, time_mc = %s
            WHERE numero_mc = %s AND user_id = %s
            """,
            (f"{montant:.2f}", justificatif, now.date(), now, numero_mc, user_id)
        )

        conn.commit()
        print(f"Versement modifié: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"statut": "Versement modifié"}), 200

    except ValueError:
        return jsonify({"error": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur modification versement: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()



@app.route('/situation_versements', methods=['GET'])
def situation_versements():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    type_versement = request.args.get('type')  # 'C' ou 'F'
    numero_cf = request.args.get('numero_cf')  # ID du client ou fournisseur

    if not type_versement or type_versement not in ['C', 'F']:
        return jsonify({'erreur': "Paramètre 'type' requis et doit être 'C' ou 'F'"}), 400
    if not numero_cf:
        return jsonify({'erreur': "Paramètre 'numero_cf' requis"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT 
                mc.numero_mc,
                mc.date_mc,
                mc.montant,
                mc.justificatif,
                mc.cf,
                mc.numero_cf,
                mc.numero_util,
                COALESCE(cl.nom, f.nom) AS nom_cf,
                u.nom AS utilisateur_nom
            FROM MOUVEMENTC mc
            LEFT JOIN client cl ON mc.cf = 'C' AND mc.numero_cf = cl.numero_clt
            LEFT JOIN fournisseur f ON mc.cf = 'F' AND mc.numero_cf = f.numero_fou
            LEFT JOIN utilisateur u ON mc.numero_util = u.numero_util
            WHERE mc.user_id = %s
            AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
            AND mc.cf = %s
            AND mc.numero_cf = %s
            ORDER BY mc.date_mc DESC, mc.time_mc DESC
        """
        params = [user_id, type_versement, numero_cf]

        cur.execute(query, params)
        rows = cur.fetchall()

        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': str(row['montant']),
                'justificatif': row['justificatif'] or '',
                'cf': row['cf'],
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        cur.close()
        conn.close()
        print(f"Situation versements: type={type_versement}, numero_cf={numero_cf}, {len(versements)} versements")
        return jsonify(versements), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération situation versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

@app.route('/annuler_vente', methods=['POST'])
def annuler_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'numero_comande' not in data or 'password2' not in data:
        print("Erreur: Données d'annulation vente invalides")
        return jsonify({"error": "Numéro de commande ou mot de passe manquant"}), 400

    numero_comande = data.get('numero_comande')
    password2 = data.get('password2')

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'existence de la commande et récupérer l'utilisateur
        cur.execute("""
            SELECT c.numero_table, c.nature, c.numero_util, u.password2 
            FROM comande c
            JOIN utilisateur u ON c.numero_util = u.numero_util
            WHERE c.numero_comande = %s AND c.user_id = %s
        """, (numero_comande, user_id))
        commande = cur.fetchone()
        if not commande:
            print(f"Erreur: Commande {numero_comande} non trouvée")
            return jsonify({"error": "Commande non trouvée"}), 404

        # Vérifier le mot de passe
        if commande['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour annuler la commande {numero_comande}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la vente
        cur.execute("""
            SELECT numero_item, quantite, prixt
            FROM attache 
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_comande, user_id))
        lignes = cur.fetchall()

        if not lignes:
            print(f"Erreur: Aucune ligne trouvée pour la commande {numero_comande}")
            return jsonify({"error": "Aucune ligne de vente trouvée"}), 404

        # Restaurer le stock dans item
        for ligne in lignes:
            cur.execute("""
                UPDATE item 
                SET qte = qte + %s 
                WHERE numero_item = %s AND user_id = %s
            """, (ligne['quantite'], ligne['numero_item'], user_id))

        # Si vente à terme (numero_table != 0), ajuster le solde du client
        if commande['numero_table'] != 0:
            total_sale = sum(float(ligne['prixt']) for ligne in lignes)
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                        (commande['numero_table'], user_id))
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client {commande['numero_table']} non trouvé")
            
            current_solde = float(client['solde'] or '0.0')
            new_solde = current_solde - total_sale  # Réduire la dette (inverser la vente)
            new_solde_str = f"{new_solde:.2f}"
            
            cur.execute("""
                UPDATE client 
                SET solde = %s 
                WHERE numero_clt = %s AND user_id = %s
            """, (new_solde_str, commande['numero_table'], user_id))
            print(f"Solde client mis à jour: numero_clt={commande['numero_table']}, total_sale={total_sale}, new_solde={new_solde_str}")

        # Supprimer les lignes de attache
        cur.execute("""
            DELETE FROM attache 
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_comande, user_id))

        # Supprimer la commande
        cur.execute("""
            DELETE FROM comande 
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_comande, user_id))

        conn.commit()
        print(f"Vente annulée: numero_comande={numero_comande}, {len(lignes)} lignes")
        return jsonify({"statut": "Vente annulée"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur annulation vente: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/annuler_reception', methods=['POST'])
def annuler_reception():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'numero_mouvement' not in data or 'password2' not in data:
        print("Erreur: Données d'annulation réception invalides")
        return jsonify({"error": "Numéro de mouvement ou mot de passe manquant"}), 400

    numero_mouvement = data.get('numero_mouvement')
    password2 = data.get('password2')

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'existence du mouvement et récupérer l'utilisateur
        cur.execute("""
            SELECT m.numero_four, m.numero_util, u.password2 
            FROM mouvement m
            JOIN utilisateur u ON m.numero_util = u.numero_util
            WHERE m.numero_mouvement = %s AND m.user_id = %s AND m.nature = 'Bon de réception'
        """, (numero_mouvement, user_id))
        mouvement = cur.fetchone()
        if not mouvement:
            print(f"Erreur: Mouvement {numero_mouvement} non trouvé")
            return jsonify({"error": "Mouvement non trouvé"}), 404

        # Vérifier le mot de passe
        if mouvement['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour annuler le mouvement {numero_mouvement}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la réception
        cur.execute("""
            SELECT numero_item, qtea, nprix 
            FROM attache2 
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))
        lignes = cur.fetchall()

        if not lignes:
            print(f"Erreur: Aucune ligne trouvée pour le mouvement {numero_mouvement}")
            return jsonify({"error": "Aucune ligne de réception trouvée"}), 404

        # Calculer le coût total de la réception
        total_cost = sum(float(ligne['qtea']) * float(ligne['nprix']) for ligne in lignes)

        # Restaurer le stock dans item
        for ligne in lignes:
            cur.execute("""
                UPDATE item 
                SET qte = qte - %s 
                WHERE numero_item = %s AND user_id = %s
            """, (ligne['qtea'], ligne['numero_item'], user_id))

        # Mettre à jour le solde du fournisseur
        cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", 
                    (mouvement['numero_four'], user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            raise Exception(f"Fournisseur {mouvement['numero_four']} non trouvé")

        current_solde = float(fournisseur['solde'] or '0.0')
        new_solde = current_solde + total_cost  # Inverser l'effet de la réception
        new_solde_str = f"{new_solde:.2f}"

        cur.execute("""
            UPDATE fournisseur 
            SET solde = %s 
            WHERE numero_fou = %s AND user_id = %s
        """, (new_solde_str, mouvement['numero_four'], user_id))
        print(f"Solde fournisseur mis à jour: numero_fou={mouvement['numero_four']}, total_cost={total_cost}, new_solde={new_solde_str}")

        # Supprimer les lignes de attache2
        cur.execute("""
            DELETE FROM attache2 
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))

        # Supprimer le mouvement
        cur.execute("""
            DELETE FROM mouvement 
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))

        conn.commit()
        print(f"Réception annulée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"statut": "Réception annulée"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur annulation réception: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_vente/<int:numero_comande>', methods=['PUT'])
def modifier_vente(numero_comande):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        return jsonify({"error": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    lignes = data['lignes']
    numero_util = data['numero_util']
    password2 = data['password2']
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur or utilisateur['password2'] != password2:
            return jsonify({"error": "Utilisateur ou mot de passe incorrect"}), 401

        # Vérifier l'existence de la commande
        cur.execute("SELECT * FROM comande WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))
        if not cur.fetchone():
            return jsonify({"error": "Commande non trouvée"}), 404

        # Restaurer le stock des anciens articles
        cur.execute("SELECT numero_item, quantite FROM attache WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))
        old_lignes = cur.fetchall()
        for ligne in old_lignes:
            cur.execute("UPDATE item SET qte = qte + %s WHERE numero_item = %s AND user_id = %s",
                        (ligne['quantite'], ligne['numero_item'], user_id))

        # Supprimer les anciennes lignes
        cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))

        # Mettre à jour la commande (sans toucher au solde)
        cur.execute("""
            UPDATE comande 
            SET numero_table = %s, date_comande = %s, nature = %s, numero_util = %s
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_table, date_comande, nature, numero_util, numero_comande, user_id))

        # Insérer les nouvelles lignes et ajuster le stock
        for ligne in lignes:
            cur.execute("""
                INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, numero_comande, ligne.get('numero_item'), ligne.get('quantite'), ligne.get('prixt'),
                  ligne.get('remarque', ''), ligne.get('prixbh', '0.00'), 0))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s",
                        (ligne.get('quantite'), ligne.get('numero_item'), user_id))

        conn.commit()
        return jsonify({"numero_comande": numero_comande, "statut": "Vente modifiée"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()


@app.route('/vente/<int:numero_comande>', methods=['GET'])
def get_vente(numero_comande):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Récupérer les détails de la commande
        cur.execute("""
            SELECT c.numero_comande, c.numero_table, c.date_comande, c.nature, c.numero_util,
                   cl.nom AS client_nom, u.nom AS utilisateur_nom
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            LEFT JOIN utilisateur u ON c.numero_util = u.numero_util
            WHERE c.numero_comande = %s AND c.user_id = %s
        """, (numero_comande, user_id))
        commande = cur.fetchone()

        if not commande:
            return jsonify({"error": "Commande non trouvée"}), 404

        # Récupérer les lignes de la commande
        cur.execute("""
            SELECT a.numero_item, a.quantite, a.prixt, a.remarque, a.prixbh, i.designation
            FROM attache a
            JOIN item i ON a.numero_item = i.numero_item
            WHERE a.numero_comande = %s AND a.user_id = %s
        """, (numero_comande, user_id))
        lignes = cur.fetchall()

        # Formater la réponse
        response = {
            'numero_comande': commande['numero_comande'],
            'numero_table': commande['numero_table'],
            'date_comande': commande['date_comande'].isoformat(),
            'nature': commande['nature'],
            'client_nom': commande['client_nom'] or 'Comptoir',
            'utilisateur_nom': commande['utilisateur_nom'] or 'N/A',
            'lignes': [
                {
                    'numero_item': ligne['numero_item'],
                    'designation': ligne['designation'],
                    'quantite': ligne['quantite'],
                    'prixt': str(ligne['prixt']),
                    'remarque': ligne['remarque'] or '',
                    'prixbh': str(ligne['prixbh'])
                }
                for ligne in lignes
            ]
        }

        cur.close()
        conn.close()
        return jsonify(response), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération vente: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/reception/<int:numero_mouvement>', methods=['GET'])
def get_reception(numero_mouvement):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Récupérer les détails du mouvement
        cur.execute("""
            SELECT m.numero_mouvement, m.numero_four, m.date_m, m.nature, m.numero_util,
                   f.nom AS fournisseur_nom, u.nom AS utilisateur_nom
            FROM mouvement m
            LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou
            LEFT JOIN utilisateur u ON m.numero_util = u.numero_util
            WHERE m.numero_mouvement = %s AND m.user_id = %s AND m.nature = 'Bon de réception'
        """, (numero_mouvement, user_id))
        mouvement = cur.fetchone()

        if not mouvement:
            return jsonify({"error": "Mouvement non trouvé"}), 404

        # Récupérer les lignes du mouvement
        cur.execute("""
            SELECT a2.numero_item, a2.qtea, a2.nprix, a2.nqte, a2.pump, i.designation
            FROM attache2 a2
            JOIN item i ON a2.numero_item = i.numero_item
            WHERE a2.numero_mouvement = %s AND a2.user_id = %s
        """, (numero_mouvement, user_id))
        lignes = cur.fetchall()

        # Formater la réponse
        response = {
            'numero_mouvement': mouvement['numero_mouvement'],
            'numero_four': mouvement['numero_four'],
            'date_m': mouvement['date_m'].isoformat(),
            'nature': mouvement['nature'],
            'fournisseur_nom': mouvement['fournisseur_nom'] or 'N/A',
            'utilisateur_nom': mouvement['utilisateur_nom'] or 'N/A',
            'lignes': [
                {
                    'numero_item': ligne['numero_item'],
                    'designation': ligne['designation'],
                    'qtea': ligne['qtea'],
                    'nprix': str(ligne['nprix']),
                    'nqte': ligne['nqte'],
                    'pump': str(ligne['pump'])
                }
                for ligne in lignes
            ]
        }

        cur.close()
        conn.close()
        return jsonify(response), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération réception: {str(e)}")
        return jsonify({"error": str(e)}), 500



@app.route('/modifier_reception/<int:numero_mouvement>', methods=['PUT'])
def modifier_reception(numero_mouvement):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de réception invalides")
        return jsonify({"error": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    lignes = data['lignes']

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            print(f"Erreur: Fournisseur {numero_four} non trouvé")
            return jsonify({"error": "Fournisseur non trouvé"}), 400

        # Vérifier que la réception existe
        cur.execute("SELECT numero_mouvement, numero_four FROM mouvement WHERE numero_mouvement = %s AND user_id = %s", 
                    (numero_mouvement, user_id))
        mouvement = cur.fetchone()
        if not mouvement:
            print(f"Erreur: Réception {numero_mouvement} non trouvée")
            return jsonify({"error": "Réception non trouvée"}), 404

        # Récupérer les lignes précédentes de la réception (quantités et prix)
        cur.execute("""
            SELECT numero_item, qtea, nprix
            FROM attache2
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))
        old_lines = cur.fetchall()
        old_lines_dict = {line['numero_item']: line for line in old_lines}
        old_total_cost = sum(float(line['qtea']) * float(line['nprix']) for line in old_lines)
        print(f"Coût total réception précédente: {old_total_cost}")

        # Restaurer le solde initial (annuler l'effet de la réception précédente)
        current_solde = float(fournisseur['solde']) if fournisseur['solde'] else 0.0
        restored_solde = current_solde + old_total_cost
        print(f"Solde restauré: {restored_solde}")

        # Calculer le nouveau coût total et préparer les mises à jour du stock
        new_total_cost = 0.0
        stock_updates = {}  # {numero_item: {old_qtea, new_qtea, prixbh}}

        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            new_qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if new_qtea < 0:
                raise Exception("La quantité ajoutée ne peut pas être négative")
            if prixbh < 0:
                raise Exception("Le prix d'achat ne peut pas être négatif")

            # Vérifier l'article
            cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            old_qtea = float(old_lines_dict.get(numero_item, {}).get('qtea', 0))

            # Calculer le coût de la ligne
            new_total_cost += new_qtea * prixbh

            # Stocker les informations pour la mise à jour du stock
            stock_updates[numero_item] = {
                'old_qtea': old_qtea,
                'new_qtea': new_qtea,
                'prixbh': prixbh,
                'current_qte': current_qte
            }

        # Traiter les articles supprimés (présents dans old_lines mais absents dans lignes)
        for numero_item, old_line in old_lines_dict.items():
            if numero_item not in stock_updates:
                stock_updates[numero_item] = {
                    'old_qtea': float(old_line['qtea']),
                    'new_qtea': 0,
                    'prixbh': 0,
                    'current_qte': float(cur.execute(
                        "SELECT qte FROM item WHERE numero_item = %s AND user_id = %s", 
                        (numero_item, user_id)
                    ).fetchone()['qte'] or 0)
                }

        # Mettre à jour le solde du fournisseur
        new_solde = restored_solde - new_total_cost
        new_solde_str = f"{new_solde:.2f}"
        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        print(f"Solde fournisseur mis à jour: numero_fou={numero_four}, new_total_cost={new_total_cost}, new_solde={new_solde_str}")

        # Supprimer les anciennes lignes de la réception
        cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", (numero_mouvement, user_id))

        # Insérer les nouvelles lignes et mettre à jour le stock
        for numero_item, update_info in stock_updates.items():
            old_qtea = update_info['old_qtea']
            new_qtea = update_info['new_qtea']
            prixbh = update_info['prixbh']
            current_qte = update_info['current_qte']

            # Restaurer le stock initial (annuler l'ancienne quantité)
            restored_qte = current_qte - old_qtea
            # Appliquer la nouvelle quantité
            new_qte = restored_qte + new_qtea

            if new_qte < 0:
                raise Exception(f"Stock négatif pour l'article {numero_item}: {new_qte}")

            # Si l'article est dans les nouvelles lignes, insérer dans attache2
            if new_qtea > 0:
                cur.execute("""
                    INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (numero_item, numero_mouvement, new_qtea, new_qte, str(prixbh)[:30], str(prixbh)[:30], True, user_id))

            # Mettre à jour le stock et le prix d'achat
            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (new_qte, str(prixbh)[:30] if new_qtea > 0 else str(update_info.get('current_prixba', 0)), 
                         numero_item, user_id))
            print(f"Stock mis à jour: numero_item={numero_item}, old_qtea={old_qtea}, new_qtea={new_qtea}, new_qte={new_qte}")

        # Mettre à jour le mouvement
        cur.execute("""
            UPDATE mouvement 
            SET numero_four = %s, numero_util = %s, date_m = %s
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_four, numero_util, datetime.utcnow(), numero_mouvement, user_id))

        conn.commit()
        print(f"Réception modifiée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"numero_mouvement": numero_mouvement}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur modification réception: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()
# --- Categories ---
@app.route('/liste_categories', methods=['GET'])
def liste_categories():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numer_categorie, description_c FROM categorie WHERE user_id = %s ORDER BY description_c", (user_id,))
        categories = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(categories), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_categorie', methods=['POST'])
def ajouter_categorie():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    description_c = data.get('description_c')
    if not description_c:
        return jsonify({'erreur': 'Description requise'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO categorie (description_c, user_id) VALUES (%s, %s) RETURNING numer_categorie",
            (description_c, user_id)
        )
        category_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Catégorie ajoutée', 'id': category_id}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_categorie/<int:numer_categorie>', methods=['PUT'])
def modifier_categorie(numer_categorie):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    description_c = data.get('description_c')
    if not description_c:
        return jsonify({'erreur': 'Description requise'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE categorie SET description_c = %s WHERE numer_categorie = %s AND user_id = %s RETURNING numer_categorie",
            (description_c, numer_categorie, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Catégorie non trouvée'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Catégorie modifiée'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_categorie/<int:numer_categorie>', methods=['DELETE'])
def supprimer_categorie(numer_categorie):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Check if category is used by any item
        cur.execute("SELECT 1 FROM item WHERE numero_categorie = %s AND user_id = %s", (numer_categorie, user_id))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Catégorie utilisée par des produits'}), 400
        cur.execute("DELETE FROM categorie WHERE numer_categorie = %s AND user_id = %s", (numer_categorie, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Catégorie non trouvée'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Catégorie supprimée'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/assigner_categorie', methods=['POST'])
def assigner_categorie():
    try:
        user_id = validate_user_id()
        if isinstance(user_id, tuple):
            logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
            return user_id

        data = request.get_json()
        if not data:
            logger.error("Données JSON manquantes dans la requête")
            return jsonify({'erreur': 'Données JSON requises'}), 400

        numero_item = data.get('numero_item')
        numero_categorie = data.get('numer_categorie')

        logger.debug(f"Requête reçue: numero_item={numero_item}, numer_categorie={numero_categorie}, user_id={user_id}")

        if numero_item is None:
            logger.error("numero_item manquant dans la requête")
            return jsonify({'erreur': 'Numéro d\'article requis'}), 400

        try:
            numero_item = int(numero_item)
        except (ValueError, TypeError) as e:
            logger.error(f"numero_item invalide: {numero_item}, erreur: {str(e)}")
            return jsonify({'erreur': 'Numéro d\'article doit être un entier'}), 400

        if numero_categorie is not None:
            try:
                numero_categorie = int(numero_categorie)
            except (ValueError, TypeError) as e:
                logger.error(f"numero_categorie invalide: {numero_categorie}, erreur: {str(e)}")
                return jsonify({'erreur': 'Numéro de catégorie doit être un entier'}), 400

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT numero_item, designation FROM item WHERE numero_item = %s AND user_id = %s",
            (numero_item, user_id)
        )
        item = cur.fetchone()
        if not item:
            logger.error(f"Article non trouvé: numero_item={numero_item}, user_id={user_id}")
            cur.close()
            conn.close()
            return jsonify({'erreur': f'Article {numero_item} non trouvé pour cet utilisateur'}), 404

        if numero_categorie is not None:
            cur.execute(
                "SELECT numer_categorie, description_c FROM categorie WHERE numer_categorie = %s AND user_id = %s",
                (numero_categorie, user_id)
            )
            category = cur.fetchone()
            if not category:
                logger.error(f"Catégorie non trouvée: numer_categorie={numero_categorie}, user_id={user_id}")
                cur.close()
                conn.close()
                return jsonify({'erreur': f'Catégorie {numero_categorie} non trouvée pour cet utilisateur'}), 404

        cur.execute(
            "UPDATE item SET numero_categorie = %s WHERE numero_item = %s AND user_id = %s RETURNING numero_categorie",
            (numero_categorie, numero_item, user_id)
        )
        updated = cur.fetchone()
        if cur.rowcount == 0:
            logger.error(f"Aucun article mis à jour: numero_item={numero_item}, user_id={user_id}")
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Aucun article mis à jour, vérifiez les données'}), 404

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({
            'statut': 'Catégorie assignée',
            'numero_item': numero_item,
            'numer_categorie': numero_categorie
        }), 200

    except Exception as e:
        logger.error(f"Erreur dans assigner_categorie: {str(e)}", exc_info=True)
        if 'conn' in locals() and conn:
            conn.rollback()
            cur.close()
            conn.close()
        return jsonify({'erreur': f'Erreur serveur: {str(e)}'}), 500

@app.route('/liste_produits_par_categorie', methods=['GET'])
def liste_produits_par_categorie():
    try:
        user_id = validate_user_id()
        if isinstance(user_id, tuple):
            logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
            return user_id
        numero_categorie = request.args.get('numero_categorie', type=int)
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if numero_categorie is None and 'numero_categorie' in request.args:
            cur.execute(
                "SELECT numero_item, designation FROM item WHERE numero_categorie IS NULL AND user_id = %s",
                (user_id,)
            )
            produits = cur.fetchall()
            cur.close()
            conn.close()
            return jsonify({'produits': produits}), 200
        else:
            cur.execute("""
                SELECT c.numer_categorie, c.description_c, i.numero_item, i.designation
                FROM categorie c
                LEFT JOIN item i ON c.numer_categorie = i.numero_categorie AND i.user_id = %s
                WHERE c.user_id = %s AND (c.numer_categorie = %s OR %s IS NULL)
            """, (user_id, user_id, numero_categorie, numero_categorie))
            rows = cur.fetchall()
            categories = {}
            for row in rows:
                cat_id = row['numer_categorie']
                if cat_id not in categories:
                    categories[cat_id] = {'numero_categorie': cat_id, 'description_c': row['description_c'], 'produits': []}
                if row['numero_item']:
                    categories[cat_id]['produits'].append({
                        'numero_item': row['numero_item'],
                        'designation': row['designation']
                    })
            cur.close()
            conn.close()
            return jsonify({'categories': list(categories.values())}), 200
    except Exception as e:
        logger.error(f"Erreur dans liste_produits_par_categorie: {str(e)}", exc_info=True)
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
