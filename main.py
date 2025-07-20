
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import logging
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.debug = True

# Configurez le logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Fonction pour obtenir la configuration de la base de données locale d'un client
def get_local_db_config(user_id):
    try:
        # Connexion à Supabase pour récupérer la config locale
        supabase_conn = get_conn()  # Toujours Supabase pour cette requête
        cur = supabase_conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT local_db_host, local_db_name, local_db_user, 
                   local_db_password, local_db_port 
            FROM client_config 
            WHERE user_id = %s
        """, (user_id,))
        
        config = cur.fetchone()
        cur.close()
        supabase_conn.close()
        
        return config if config else None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la config locale: {str(e)}")
        return None

# Fonction pour obtenir une connexion (Supabase ou locale)
def get_conn(user_id=None):
    if user_id:
        config = get_local_db_config(user_id)
        if config and config['local_db_host']:
            try:
                # Connexion à la base locale du client
                return psycopg2.connect(
                    host=config['local_db_host'],
                    database=config['local_db_name'],
                    user=config['local_db_user'],
                    password=config['local_db_password'],
                    port=config['local_db_port']
                )
            except Exception as e:
                logger.warning(f"Échec de la connexion locale pour user_id {user_id}: {str(e)}. Retour à Supabase.")
    
    # Connexion par défaut à Supabase
    url = os.environ['DATABASE_URL']
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode='require')

# Vérification de l'utilisateur
def validate_user():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401
    return user_id

# Route pour vérifier que l'API est en ligne
@app.route('/', methods=['GET'])
def index():
    try:
        user_id = validate_user()
        if isinstance(user_id, tuple):
            return user_id
        conn = get_conn(user_id)
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return jsonify({'erreur': f'Erreur connexion DB : {str(e)}'}), 500

# Route pour tester la connexion à une base de données publique
@app.route('/test_public_db', methods=['POST'])
def test_public_db():
    logger.info("Requête reçue pour /test_public_db")
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    required_fields = ['local_db_host', 'local_db_name', 'local_db_user', 'local_db_password', 'local_db_port']
    for field in required_fields:
        if not data.get(field):
            logger.error(f"Paramètre manquant: {field}")
            return jsonify({'erreur': f'Missing {field} in request body'}), 400

    try:
        logger.info(f"Tentative de connexion à {data['local_db_host']}:{data['local_db_port']}/{data['local_db_name']}")
        conn = psycopg2.connect(
            host=data['local_db_host'],
            database=data['local_db_name'],
            user=data['local_db_user'],
            password=data['local_db_password'],
            port=data['local_db_port'],
            connect_timeout=5
        )
        conn.close()
        logger.info("Connexion réussie")
        return "Connexion à la base de données publique réussie", 200
    except Exception as e:
        logger.error(f"Échec de la connexion: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Fonctions utilitaires ---
def calculate_ean13_check_digit(code12):
    """Calcule le chiffre de contrôle pour un code EAN-13 à partir d'un code de 12 chiffres."""
    digits = [int(d) for d in code12]
    odd_sum = sum(digits[0::2])
    even_sum = sum(digits[1::2])
    total = odd_sum * 3 + even_sum
    next_multiple_of_10 = (total + 9) // 10 * 10
    check_digit = next_multiple_of_10 - total
    return check_digit

# --- Route pour gérer la configuration de la base de données ---
@app.route('/get_client_config', methods=['GET'])
def get_client_config():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()  # Toujours Supabase pour cette requête
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT local_db_host, local_db_name, local_db_user, local_db_password, local_db_port FROM client_config WHERE user_id = %s",
            (user_id,)
        )
        config = cur.fetchone()
        cur.close()
        conn.close()

        if config:
            return jsonify(dict(config)), 200
        else:
            return jsonify({'erreur': 'Aucune configuration trouvée pour cet utilisateur'}), 404
    except Exception as e:
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/update_client_config', methods=['POST'])
def update_client_config():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    local_db_host = data.get('local_db_host', '')
    local_db_name = data.get('local_db_name', 'restocafee')
    local_db_user = data.get('local_db_user', 'postgres')
    local_db_password = data.get('local_db_password', 'masterkey')
    local_db_port = data.get('local_db_port', '5432')

    try:
        conn = get_conn()  # Toujours Supabase pour cette opération
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si une configuration existe déjà
        cur.execute("SELECT 1 FROM client_config WHERE user_id = %s", (user_id,))
        exists = cur.fetchone()

        if exists:
            # Mise à jour
            cur.execute(
                """
                UPDATE client_config 
                SET local_db_host = %s, local_db_name = %s, local_db_user = %s, 
                    local_db_password = %s, local_db_port = %s
                WHERE user_id = %s
                RETURNING user_id
                """,
                (local_db_host, local_db_name, local_db_user, local_db_password, local_db_port, user_id)
            )
        else:
            # Insertion
            cur.execute(
                """
                INSERT INTO client_config (user_id, local_db_host, local_db_name, local_db_user, local_db_password, local_db_port)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING user_id
                """,
                (user_id, local_db_host, local_db_name, local_db_user, local_db_password, local_db_port)
            )

        result = cur.fetchone()
        if not result:
            raise Exception("Échec de la mise à jour/insertion de la configuration")

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Configuration de la base de données mise à jour', 'user_id': user_id}), 200
    except Exception as e:
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500
# --- Produits ---

@app.route('/rechercher_produit_codebar', methods=['GET'])
def rechercher_produit_codebar():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    codebar = request.args.get('codebar')
    if not codebar:
        return jsonify({'erreur': 'Code-barres requis'}), 400

    try:
        logger.debug(f"Recherche produit avec codebar: {codebar} pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Requête adaptée selon le type de connexion
        if is_local:
            cur.execute("""
                SELECT numero_item, bar, designation, prix, prixba, qte
                FROM item
                WHERE bar = %s
            """, (codebar,))
        else:
            cur.execute("""
                SELECT numero_item, bar, designation, prix, prixba, qte
                FROM item
                WHERE bar = %s AND user_id = %s
            """, (codebar, user_id))

        produit = cur.fetchone()

        if produit:
            # Recherche dans codebar si non trouvé dans item
            if is_local:
                cur.execute("""
                    SELECT i.numero_item, i.bar, i.designation, i.prix, i.prixba, i.qte
                    FROM codebar c
                    JOIN item i ON c.bar = i.numero_item::varchar
                    WHERE c.bar2 = %s
                """, (codebar,))
            else:
                cur.execute("""
                    SELECT i.numero_item, i.bar, i.designation, i.prix, i.prixba, i.qte
                    FROM codebar c
                    JOIN item i ON c.bar = i.numero_item::varchar
                    WHERE c.bar2 = %s AND i.user_id = %s
                """, (codebar, user_id))

            produit_lie = cur.fetchone()
            
            cur.close()
            conn.close()
            
            if produit_lie:
                logger.debug(f"Produit lié trouvé pour codebar: {codebar}")
                return jsonify({
                    'statut': 'trouvé',
                    'type': 'lié',
                    'produit': {
                        'numero_item': produit_lie['numero_item'],
                        'bar': produit_lie['bar'],
                        'designation': produit_lie['designation'],
                        'prix': float(produit_lie['prix']) if produit_lie['prix'] is not None else 0.0,
                        'prixba': produit_lie['prixba'] or '0.00',
                        'qte': produit_lie['qte']
                    }
                }), 200
            else:
                logger.debug(f"Produit principal trouvé pour codebar: {codebar}")
                return jsonify({
                    'statut': 'trouvé',
                    'type': 'principal',
                    'produit': {
                        'numero_item': produit['numero_item'],
                        'bar': produit['bar'],
                        'designation': produit['designation'],
                        'prix': float(produit['prix']) if produit['prix'] is not None else 0.0,
                        'prixba': produit['prixba'] or '0.00',
                        'qte': produit['qte']
                    }
                }), 200

        cur.close()
        conn.close()
        logger.debug(f"Produit non trouvé pour codebar: {codebar}")
        return jsonify({'erreur': 'Produit non trouvé'}), 404

    except Exception as e:
        logger.error(f"Erreur dans rechercher_produit_codebar: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_codebar_lie', methods=['POST'])
def ajouter_codebar_lie():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('barcode')

    if not numero_item:
        return jsonify({'erreur': 'numero_item est requis'}), 400

    try:
        numero_item = int(numero_item)
        logger.debug(f"Ajout code-barres lié pour numero_item: {numero_item}, bar2: {bar2}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier que l'item existe
        if is_local:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s", (numero_item,))
        else:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Vérifier que bar2 n'existe pas déjà
        if bar2:
            if is_local:
                cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s", (bar2,))
            else:
                cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar2, user_id))
            
            if cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres lié existe déjà'}), 409

        # Générer un bar2 si non fourni
        if is_local:
            cur.execute("SELECT bar2 FROM codebar")
        else:
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
            
            if is_local:
                cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s", (bar2,))
            else:
                cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar2, user_id))
            
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà'}), 409

        cur.execute("LOCK TABLE codebar IN EXCLUSIVE MODE")
        if is_local:
            cur.execute(
                "INSERT INTO codebar (bar2, bar) VALUES (%s, %s) RETURNING n",
                (bar2, numero_item)
            )
        else:
            cur.execute(
                "INSERT INTO codebar (bar2, bar, user_id) VALUES (%s, %s, %s) RETURNING n",
                (bar2, numero_item, user_id)
            )
        
        codebar_id = cur.fetchone()['n']
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Code-barres lié ajouté: id={codebar_id}, bar2={bar2}")
        return jsonify({'statut': 'Code-barres lié ajouté', 'id': codebar_id, 'bar2': bar2}), 201
    
    except ValueError:
        logger.error(f"Erreur ValueError dans ajouter_codebar_lie: numero_item doit être un nombre")
        if 'conn' in locals() and conn:
            conn.rollback()
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        logger.error(f"Erreur dans ajouter_codebar_lie: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_codebar_lies', methods=['GET'])
def liste_codebar_lies():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    numero_item = request.args.get('numero_item')
    if not numero_item:
        return jsonify({'erreur': 'numero_item est requis'}), 400

    try:
        numero_item = int(numero_item)
        logger.debug(f"Récupération des codes-barres liés pour numero_item: {numero_item}, user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier que l'item existe
        if is_local:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s", (numero_item,))
        else:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Récupérer les codes-barres liés
        if is_local:
            cur.execute("SELECT bar2 FROM codebar WHERE bar::INTEGER = %s ORDER BY n", (numero_item,))
        else:
            cur.execute("SELECT bar2 FROM codebar WHERE bar::INTEGER = %s AND user_id = %s ORDER BY n", (numero_item, user_id))
        
        linked_barcodes = [row['bar2'] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        logger.debug(f"Codes-barres liés trouvés: {linked_barcodes}")
        return jsonify({'linked_barcodes': linked_barcodes}), 200
    
    except ValueError:
        logger.error(f"Erreur ValueError dans liste_codebar_lies: numero_item doit être un nombre")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        logger.error(f"Erreur dans liste_codebar_lies: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_codebar_lie', methods=['POST'])
def supprimer_codebar_lie():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('bar2')

    if not numero_item or not bar2:
        return jsonify({'erreur': 'numero_item et bar2 sont requis'}), 400

    try:
        numero_item_str = str(numero_item)
        logger.debug(f"Suppression code-barres lié pour numero_item: {numero_item}, bar2: {bar2}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier que l'item existe
        if is_local:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s", (int(numero_item),))
        else:
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (int(numero_item), user_id))
        
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Vérifier que le code-barres lié existe
        if is_local:
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND bar = %s", (bar2, numero_item_str))
        else:
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))
        
        if not cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Code-barres lié non trouvé pour ce produit'}), 404

        # Supprimer le code-barres lié
        if is_local:
            cur.execute("DELETE FROM codebar WHERE bar2 = %s AND bar = %s", (bar2, numero_item_str))
        else:
            cur.execute("DELETE FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))

        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Code-barres lié supprimé: bar2={bar2}")
        return jsonify({'statut': 'Code-barres lié supprimé'}), 200
    
    except Exception as e:
        logger.error(f"Erreur dans supprimer_codebar_lie: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

# --- Clients ---

@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Tentative de récupération des clients pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")
        
        if is_local:
            cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client ORDER BY nom")
        else:
            cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s ORDER BY nom", (user_id,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        logger.debug(f"Nombre de clients trouvés: {len(rows)}")

        clients = [
            {
                'numero_clt': row['numero_clt'],
                'nom': row['nom'],
                'solde': float(row['solde']) if row['solde'] is not None else 0.0,
                'reference': row['reference'],
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]
        return jsonify(clients)
    except Exception as e:
        logger.error(f"Erreur dans liste_clients: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        logger.debug(f"Ajout client pour user_id: {user_id}, nom: {nom}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("SELECT COUNT(*) FROM client")
            reference = f"C{cur.fetchone()['count'] + 1}"
            
            cur.execute(
                "INSERT INTO client (nom, solde, reference, contact, adresse) VALUES (%s, %s, %s, %s, %s) RETURNING numero_clt",
                (nom, '0.00', reference, contact, adresse)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM client WHERE user_id = %s", (user_id,))
            reference = f"C{cur.fetchone()['count'] + 1}"
            
            cur.execute(
                "INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_clt",
                (nom, '0.00', reference, contact, adresse, user_id)
            )
        
        client_id = cur.fetchone()['numero_clt']
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Client ajouté: id={client_id}, reference={reference}")
        return jsonify({'statut': 'Client ajouté', 'id': client_id, 'reference': reference}), 201
    except Exception as e:
        logger.error(f"Erreur dans ajouter_client: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_client/<numero_clt>', methods=['PUT'])
def modifier_client(numero_clt):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        logger.debug(f"Modification client numero_clt: {numero_clt}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute(
                "UPDATE client SET nom = %s, contact = %s, adresse = %s WHERE numero_clt = %s RETURNING numero_clt",
                (nom, contact, adresse, numero_clt)
            )
        else:
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
        logger.debug(f"Client modifié: numero_clt={numero_clt}")
        return jsonify({'statut': 'Client modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur dans modifier_client: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_client/<numero_clt>', methods=['DELETE'])
def supprimer_client(numero_clt):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Suppression client numero_clt: {numero_clt}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("DELETE FROM client WHERE numero_clt = %s", (numero_clt,))
        else:
            cur.execute("DELETE FROM client WHERE numero_clt = %s AND user_id = %s", (numero_clt, user_id))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Client non trouvé'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Client supprimé: numero_clt={numero_clt}")
        return jsonify({'statut': 'Client supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur dans supprimer_client: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

# --- Fournisseurs ---

@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Tentative de récupération des fournisseurs pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")
        
        if is_local:
            cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur ORDER BY nom")
        else:
            cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s ORDER BY nom", (user_id,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        logger.debug(f"Nombre de fournisseurs trouvés: {len(rows)}")

        fournisseurs = [
            {
                'numero_fou': row['numero_fou'],
                'nom': row['nom'],
                'solde': float(row['solde']) if row['solde'] is not None else 0.0,
                'reference': row['reference'],
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]
        return jsonify(fournisseurs)
    except Exception as e:
        logger.error(f"Erreur dans liste_fournisseurs: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_fournisseur', methods=['POST'])
def ajouter_fournisseur():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        logger.debug(f"Ajout fournisseur pour user_id: {user_id}, nom: {nom}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("SELECT COUNT(*) FROM fournisseur")
            reference = f"F{cur.fetchone()['count'] + 1}"
            
            cur.execute(
                "INSERT INTO fournisseur (nom, solde, reference, contact, adresse) VALUES (%s, %s, %s, %s, %s) RETURNING numero_fou",
                (nom, '0.00', reference, contact, adresse)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM fournisseur WHERE user_id = %s", (user_id,))
            reference = f"F{cur.fetchone()['count'] + 1}"
            
            cur.execute(
                "INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_fou",
                (nom, '0.00', reference, contact, adresse, user_id)
            )
        
        fournisseur_id = cur.fetchone()['numero_fou']
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Fournisseur ajouté: id={fournisseur_id}, reference={reference}")
        return jsonify({'statut': 'Fournisseur ajouté', 'id': fournisseur_id, 'reference': reference}), 201
    except Exception as e:
        logger.error(f"Erreur dans ajouter_fournisseur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_fournisseur/<numero_fou>', methods=['PUT'])
def modifier_fournisseur(numero_fou):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not nom:
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        logger.debug(f"Modification fournisseur numero_fou: {numero_fou}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute(
                "UPDATE fournisseur SET nom = %s, contact = %s, adresse = %s WHERE numero_fou = %s RETURNING numero_fou",
                (nom, contact, adresse, numero_fou)
            )
        else:
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
        logger.debug(f"Fournisseur modifié: numero_fou={numero_fou}")
        return jsonify({'statut': 'Fournisseur modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur dans modifier_fournisseur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_fournisseur/<numero_fou>', methods=['DELETE'])
def supprimer_fournisseur(numero_fou):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Suppression fournisseur numero_fou: {numero_fou}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("DELETE FROM fournisseur WHERE numero_fou = %s", (numero_fou,))
        else:
            cur.execute("DELETE FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_fou, user_id))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Fournisseur non trouvé'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Fournisseur supprimé: numero_fou={numero_fou}")
        return jsonify({'statut': 'Fournisseur supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur dans supprimer_fournisseur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

# --- Produits ---


@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        logger.error(f"Erreur validation utilisateur: {user_id}")
        return user_id

    conn = None
    try:
        logger.debug(f"Tentative de récupération des produits pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")
        
        if is_local:
            logger.debug("Exécution de la requête SQL pour la base locale")
            cur.execute("SELECT numero_item, bar, designation, qte, prix, prixba, ref FROM item ORDER BY designation")
        else:
            logger.debug(f"Exécution de la requête SQL pour Supabase avec user_id: {user_id}")
            cur.execute("SELECT numero_item, bar, designation, qte, prix, prixba, ref FROM item WHERE user_id = %s ORDER BY designation", (user_id,))
        
        rows = cur.fetchall()
        logger.debug(f"Nombre de produits trouvés: {len(rows)}")
        
        produits = []
        for row in rows:
            try:
                # Gérer le champ prix (remplacer virgule par point si nécessaire)
                prix_str = str(row['prix']).replace(',', '.') if row['prix'] is not None else '0.0'
                prix = float(prix_str) if prix_str else 0.0
                
                # Gérer le champ prixba (garder comme chaîne pour cohérence avec le format)
                prixba = str(row['prixba']).replace(',', '.') if row['prixba'] is not None else '0.00'
                
                produits.append({
                    'NUMERO_ITEM': row['numero_item'],
                    'BAR': row['bar'] or '',
                    'DESIGNATION': row['designation'] or '',
                    'QTE': float(row['qte']) if row['qte'] is not None else 0.0,
                    'PRIX': prix,
                    'PRIXBA': prixba,
                    'REF': row['ref'] or ''
                })
            except (ValueError, TypeError) as e:
                logger.error(f"Erreur de conversion pour produit numero_item={row['numero_item']}: {str(e)}")
                continue  # Ignorer la ligne problématique mais continuer avec les autres
        
        logger.debug(f"Produits traités: {len(produits)}")
        return jsonify(produits), 200

    except Exception as e:
        logger.error(f"Erreur dans liste_produits: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    user_id = validate_user()
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

        logger.debug(f"Ajout item pour user_id: {user_id}, designation: {designation}, bar: {bar}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        cur.execute("LOCK TABLE item IN EXCLUSIVE MODE")

        # Vérifier l'unicité du code-barres
        if bar:
            if is_local:
                cur.execute("SELECT 1 FROM item WHERE bar = %s", (bar,))
            else:
                cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
            
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres existe déjà'}), 409

        # Trouver le prochain numéro disponible pour ref
        if is_local:
            cur.execute("SELECT ref FROM item ORDER BY ref")
        else:
            cur.execute("SELECT ref FROM item WHERE user_id = %s ORDER BY ref", (user_id,))
        
        existing_refs = cur.fetchall()
        used_numbers = []
        for ref in existing_refs:
            ref_num = int(ref['ref'][1:]) if ref['ref'].startswith('P') and ref['ref'][1:].isdigit() else 0
            used_numbers.append(ref_num)

        next_number = 1
        used_numbers = sorted(set(used_numbers))
        for num in used_numbers:
            if num == next_number:
                next_number += 1
            elif num > next_number:
                break

        ref = f"P{next_number}"

        # Si bar est vide, générer un code EAN-13
        if not bar:
            code12 = f"1{next_number:011d}"
            check_digit = calculate_ean13_check_digit(code12)
            bar = f"{code12}{check_digit}"

        # Insérer le produit
        if is_local:
            cur.execute(
                "INSERT INTO item (designation, bar, prix, qte, prixba, ref) "
                "VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_item",
                (designation, bar, prix, qte, prixba or '0.00', ref)
            )
        else:
            cur.execute(
                "INSERT INTO item (designation, bar, prix, qte, prixba, ref, user_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING numero_item",
                (designation, bar, prix, qte, prixba or '0.00', ref, user_id)
            )
        
        item_id = cur.fetchone()['numero_item']
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Item ajouté: id={item_id}, ref={ref}, bar={bar}")
        return jsonify({'statut': 'Item ajouté', 'id': item_id, 'ref': ref, 'bar': bar}), 201
    
    except ValueError:
        logger.error(f"Erreur ValueError dans ajouter_item: prix ou qte invalide")
        if 'conn' in locals() and conn:
            conn.rollback()
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        logger.error(f"Erreur dans ajouter_item: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_item/<numero_item>', methods=['PUT'])
def modifier_item(numero_item):
    user_id = validate_user()
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

        logger.debug(f"Modification item numero_item: {numero_item}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'unicité de bar (sauf pour cet item)
        if is_local:
            cur.execute("SELECT 1 FROM item WHERE bar = %s AND numero_item != %s", (bar, numero_item))
        else:
            cur.execute("SELECT 1 FROM item WHERE bar = %s AND numero_item != %s AND user_id = %s", (bar, numero_item, user_id))
        
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Ce code-barres est déjà utilisé'}), 409

        # Mettre à jour l'item
        if is_local:
            cur.execute(
                "UPDATE item SET designation = %s, bar = %s, prix = %s, qte = %s, prixba = %s WHERE numero_item = %s RETURNING numero_item",
                (designation, bar, prix, qte, prixba or '0.00', numero_item)
            )
        else:
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
        logger.debug(f"Item modifié: numero_item={numero_item}")
        return jsonify({'statut': 'Produit modifié'}), 200
    
    except ValueError:
        logger.error(f"Erreur ValueError dans modifier_item: prix ou qte invalide")
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        logger.error(f"Erreur dans modifier_item: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_item/<numero_item>', methods=['DELETE'])
def supprimer_item(numero_item):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Suppression item numero_item: {numero_item}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("DELETE FROM item WHERE numero_item = %s", (numero_item,))
        else:
            cur.execute("DELETE FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Item supprimé: numero_item={numero_item}")
        return jsonify({'statut': 'Produit supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur dans supprimer_item: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500
		
@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        return jsonify({"erreur": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')
    amount_paid = float(data.get('amount_paid', 0))
    lignes = data['lignes']
    numero_util = data['numero_util']
    password2 = data['password2']
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    conn = None
    try:
        logger.debug(f"Validation vente pour user_id: {user_id}, numero_util: {numero_util}, nature: {nature}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Verify user and password
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        
        utilisateur = cur.fetchone()
        if not utilisateur:
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Get the last counter for this nature
        if is_local:
            cur.execute("SELECT COALESCE(MAX(compteur), 0) as max_compteur FROM comande WHERE nature = %s", (nature,))
        else:
            cur.execute("SELECT COALESCE(MAX(compteur), 0) as max_compteur FROM comande WHERE nature = %s AND user_id = %s", (nature, user_id))
        
        compteur = cur.fetchone()['max_compteur'] + 1

        # Insert the order
        if is_local:
            cur.execute("""
                INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, numero_util)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING numero_comande
            """, (numero_table, date_comande, 'cloture', nature, -1, compteur, numero_util))
        else:
            cur.execute("""
                INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id, numero_util)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING numero_comande
            """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id, numero_util))
        
        numero_comande = cur.fetchone()['numero_comande']
        logger.debug(f"Commande insérée: numero_comande={numero_comande}")

        # Insert lines and update stock
        for ligne in lignes:
            if is_local:
                cur.execute("""
                    INSERT INTO attache (numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (numero_comande, ligne.get('numero_item'), ligne.get('quantite'), ligne.get('prixt'),
                      ligne.get('remarque'), ligne.get('prixbh'), 0))
                
                cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s", 
                           (ligne.get('quantite'), ligne.get('numero_item')))
            else:
                cur.execute("""
                    INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, numero_comande, ligne.get('numero_item'), ligne.get('quantite'), ligne.get('prixt'),
                      ligne.get('remarque'), ligne.get('prixbh'), 0))
                
                cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s", 
                           (ligne.get('quantite'), ligne.get('numero_item'), user_id))

        # Update client balance if payment mode is 'a_terme'
        if payment_mode == 'a_terme' and numero_table != 0:
            total_sale = sum(float(ligne.get('prixt', 0)) for ligne in lignes)
            solde_change = amount_paid - total_sale

            if is_local:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s", (numero_table,))
            else:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_table, user_id))
            
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client {numero_table} non trouvé")

            current_solde = float(client['solde'] or '0.0')
            new_solde = current_solde + solde_change
            new_solde_str = f"{new_solde:.2f}"

            if is_local:
                cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s", (new_solde_str, numero_table))
            else:
                cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s AND user_id = %s", 
                           (new_solde_str, numero_table, user_id))
            logger.debug(f"Solde client mis à jour: numero_clt={numero_table}, nouveau solde={new_solde_str}")

        conn.commit()
        logger.debug(f"Vente validée: numero_comande={numero_comande}")
        return jsonify({"numero_comande": numero_comande}), 200

    except Exception as e:
        logger.error(f"Erreur dans valider_vente: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/client_solde', methods=['GET'])
def client_solde():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    conn = None
    try:
        logger.debug(f"Récupération des soldes clients pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("SELECT numero_clt, COALESCE(solde, '0.00') as solde FROM client")
        else:
            cur.execute("SELECT numero_clt, COALESCE(solde, '0.00') as solde FROM client WHERE user_id = %s", (user_id,))
        
        soldes = cur.fetchall()
        logger.debug(f"Nombre de soldes clients trouvés: {len(soldes)}")
        
        response = [
            {
                'numero_clt': row['numero_clt'],
                'solde': f"{float(row['solde'] or 0):.2f}"
            }
            for row in soldes
        ]
        
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Erreur dans client_solde: {str(e)}")
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    conn = None
    try:
        logger.debug(f"Récupération des ventes du jour pour user_id: {user_id}, date: {selected_date}, numero_clt: {numero_clt}, numero_util: {numero_util}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Define date range
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

        # Build SQL query
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
            WHERE {user_condition}
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [date_start, date_end]

        if not is_local:
            query = query.format(user_condition="c.user_id = %s AND a.user_id = %s AND i.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
        else:
            query = query.format(user_condition="1=1")

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(numero_clt)

        if numero_util:
            if numero_util == '0':
                pass
            else:
                query += " AND c.numero_util = %s"
                params.append(numero_util)

        query += " ORDER BY c.numero_comande DESC"
        logger.debug(f"Exécution de la requête ventes_jour: {query % tuple(params)}")

        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre de lignes de ventes trouvées: {len(rows)}")

        # Process results
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
                'prixt': f"{float(row['prixt']):.2f}",
                'remarque': row['remarque'] or ''
            })

            total += float(row['prixt'])

        for vente in ventes_map.values():
            if vente['nature'] == 'TICKET':
                tickets.append(vente)
            elif vente['nature'] == 'BON DE L.':
                bons.append(vente)

        response = {
            'tickets': tickets,
            'bons': bons,
            'total': f"{total:.2f}"
        }
        logger.debug(f"Réponse ventes_jour: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans ventes_jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/articles_plus_vendus', methods=['GET'])
def articles_plus_vendus():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    conn = None
    try:
        logger.debug(f"Récupération des articles plus vendus pour user_id: {user_id}, date: {selected_date}, numero_clt: {numero_clt}, numero_util: {numero_util}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Define date range
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

        # Build SQL query
        query = """
            SELECT 
                i.numero_item,
                i.designation,
                SUM(a.quantite) AS quantite,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)) AS total_vente
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE {user_condition}
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [date_start, date_end]

        if not is_local:
            query = query.format(user_condition="c.user_id = %s AND a.user_id = %s AND i.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
        else:
            query = query.format(user_condition="1=1")

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(numero_clt)

        if numero_util and numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(numero_util)

        query += """
            GROUP BY i.numero_item, i.designation
            ORDER BY quantite DESC
            LIMIT 10
        """
        logger.debug(f"Exécution de la requête articles_plus_vendus: {query % tuple(params)}")

        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre d'articles trouvés: {len(rows)}")

        # Format response
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
        logger.error(f"Erreur dans articles_plus_vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util', '0')

    conn = None
    try:
        logger.debug(f"Récupération des profits par date pour user_id: {user_id}, date: {selected_date}, numero_clt: {numero_clt}, numero_util: {numero_util}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Define date range
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

        # Build SQL query
        query = """
            SELECT 
                DATE(c.date_comande) AS date,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0') AS FLOAT))) AS profit
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE {user_condition}
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [date_start, date_end]

        if not is_local:
            query = query.format(user_condition="c.user_id = %s AND a.user_id = %s AND i.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
        else:
            query = query.format(user_condition="1=1")

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(numero_clt)

        if numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(numero_util)

        query += """
            GROUP BY DATE(c.date_comande)
            ORDER BY DATE(c.date_comande) DESC
        """
        logger.debug(f"Exécution de la requête profit_by_date: {query % tuple(params)}")

        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre de profits par date trouvés: {len(rows)}")

        # Format response
        profits = [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'profit': f"{float(row['profit'] or 0):.2f}"
            }
            for row in rows
        ]

        return jsonify(profits), 200

    except Exception as e:
        logger.error(f"Erreur dans profit_by_date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")


@app.route('/dashboard', methods=['GET'])
def dashboard():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    period = request.args.get('period', 'day')
    conn = None
    try:
        logger.debug(f"Début de la requête dashboard pour user_id: {user_id}, période: {period}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

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
            WHERE {user_condition}
            AND c.date_comande >= %s
            AND c.date_comande <= %s
        """
        params = [date_start, date_end]

        if not is_local:
            query_kpi = query_kpi.format(user_condition="c.user_id = %s AND a.user_id = %s AND i.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
        else:
            query_kpi = query_kpi.format(user_condition="1=1")

        logger.debug(f"Exécution de la requête KPI: {query_kpi % tuple(params)}")
        cur.execute(query_kpi, params)
        kpi_data = cur.fetchone()

        # Query for low stock items
        if is_local:
            cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE qte < 10")
        else:
            cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE qte < 10 AND user_id = %s", (user_id,))
        
        low_stock_count = cur.fetchone()['low_stock']
        logger.debug(f"Nombre d'articles en stock faible: {low_stock_count}")

        # Query for top client
        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS client_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            WHERE {user_condition}
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY cl.nom
            ORDER BY client_ca DESC
            LIMIT 1
        """
        params_top = [date_start, date_end]

        if not is_local:
            query_top_client = query_top_client.format(user_condition="c.user_id = %s AND a.user_id = %s")
            params_top.insert(0, user_id)
            params_top.insert(1, user_id)
        else:
            query_top_client = query_top_client.format(user_condition="1=1")

        logger.debug(f"Exécution de la requête top client: {query_top_client % tuple(params_top)}")
        cur.execute(query_top_client, params_top)
        top_client = cur.fetchone()

        # Query for chart data (CA per day)
        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            WHERE {user_condition}
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        params_chart = [date_start, date_end]

        if not is_local:
            query_chart = query_chart.format(user_condition="c.user_id = %s AND a.user_id = %s")
            params_chart.insert(0, user_id)
            params_chart.insert(1, user_id)
        else:
            query_chart = query_chart.format(user_condition="1=1")

        logger.debug(f"Exécution de la requête chart: {query_chart % tuple(params_chart)}")
        cur.execute(query_chart, params_chart)
        chart_data = cur.fetchall()

        # Format chart data
        chart_labels = []
        chart_values = []
        current_date = date_start
        while current_date <= date_end:
            chart_labels.append(current_date.strftime('%Y-%m-%d'))
            daily_ca = next((row['daily_ca'] for row in chart_data 
                           if row['sale_date'].strftime('%Y-%m-%d') == current_date.strftime('%Y-%m-%d')), 0)
            chart_values.append(float(daily_ca))
            current_date += timedelta(days=1)

        response = {
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
        }
        logger.debug(f"Réponse dashboard: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans dashboard: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Tentative de récupération des utilisateurs pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")
        
        if is_local:
            cur.execute("SELECT numero_util, nom, statue FROM utilisateur ORDER BY nom")
        else:
            cur.execute("SELECT numero_util, nom, statue FROM utilisateur WHERE user_id = %s ORDER BY nom", (user_id,))
        
        rows = cur.fetchall()
        logger.debug(f"Nombre d'utilisateurs trouvés: {len(rows)}")
        
        utilisateurs = [
            {
                'numero': row['numero_util'],
                'nom': row['nom'],
                'statut': row['statue']
            }
            for row in rows
        ]
        
        cur.close()
        conn.close()
        logger.debug("Connexion fermée, retour des utilisateurs")
        return jsonify(utilisateurs)
    except Exception as e:
        logger.error(f"Erreur dans liste_utilisateurs: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')
    statue = data.get('statue')

    if not all([nom, password2, statue]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password2, statue)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    try:
        logger.debug(f"Ajout utilisateur pour user_id: {user_id}, nom: {nom}, statue: {statue}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute(
                "INSERT INTO utilisateur (nom, password2, statue) VALUES (%s, %s, %s) RETURNING numero_util",
                (nom, password2, statue)
            )
        else:
            cur.execute(
                "INSERT INTO utilisateur (nom, password2, statue, user_id) VALUES (%s, %s, %s, %s) RETURNING numero_util",
                (nom, password2, statue, user_id)
            )
        
        numero_util = cur.fetchone()['numero_util']
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Utilisateur ajouté: numero_util={numero_util}")
        return jsonify({'statut': 'Utilisateur ajouté', 'id': numero_util}), 201
    except Exception as e:
        logger.error(f"Erreur dans ajouter_utilisateur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_utilisateur/<numero_util>', methods=['PUT'])
def modifier_utilisateur(numero_util):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')  # Optional
    statue = data.get('statue')

    if not all([nom, statue]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, statue)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    try:
        logger.debug(f"Modification utilisateur numero_util: {numero_util}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if password2:
            if is_local:
                cur.execute(
                    "UPDATE utilisateur SET nom = %s, password2 = %s, statue = %s WHERE numero_util = %s RETURNING numero_util",
                    (nom, password2, statue, numero_util)
                )
            else:
                cur.execute(
                    "UPDATE utilisateur SET nom = %s, password2 = %s, statue = %s WHERE numero_util = %s AND user_id = %s RETURNING numero_util",
                    (nom, password2, statue, numero_util, user_id)
                )
        else:
            if is_local:
                cur.execute(
                    "UPDATE utilisateur SET nom = %s, statue = %s WHERE numero_util = %s RETURNING numero_util",
                    (nom, statue, numero_util)
                )
            else:
                cur.execute(
                    "UPDATE utilisateur SET nom = %s, statue = %s WHERE numero_util = %s AND user_id = %s RETURNING numero_util",
                    (nom, statue, numero_util, user_id)
                )
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Utilisateur modifié: numero_util={numero_util}")
        return jsonify({'statut': 'Utilisateur modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur dans modifier_utilisateur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    try:
        logger.debug(f"Suppression utilisateur numero_util: {numero_util}, user_id: {user_id}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("DELETE FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("DELETE FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        
        conn.commit()
        cur.close()
        conn.close()
        logger.debug(f"Utilisateur supprimé: numero_util={numero_util}")
        return jsonify({'statut': 'Utilisateur supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur dans supprimer_utilisateur: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': str(e)}), 500



@app.route('/stock_value', methods=['GET'])
def valeur_stock():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        logger.error(f"Erreur validation utilisateur: {user_id}")
        return user_id

    conn = None
    try:
        logger.debug(f"Récupération de la valeur du stock pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            logger.debug("Exécution de la requête SQL pour la base locale")
            cur.execute("""
                SELECT 
                    SUM(COALESCE(CAST(NULLIF(REPLACE(prixba, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_achat,
                    SUM(COALESCE(CAST(NULLIF(REPLACE(prix, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_vente
                FROM item
            """)
        else:
            logger.debug(f"Exécution de la requête SQL pour Supabase avec user_id: {user_id}")
            cur.execute("""
                SELECT 
                    SUM(COALESCE(CAST(NULLIF(REPLACE(prixba, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_achat,
                    SUM(COALESCE(CAST(NULLIF(REPLACE(prix, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_vente
                FROM item 
                WHERE user_id = %s
            """, (user_id,))
        
        result = cur.fetchone()
        logger.debug(f"Résultat de la requête stock: {result}")

        # Gérer le cas où result est None
        valeur_achat = float(result['valeur_achat'] or 0)
        valeur_vente = float(result['valeur_vente'] or 0)
        zakat = valeur_vente * 0.025  # 2.5% de la valeur de vente

        response = {
            'valeur_achat': f"{valeur_achat:.2f}",
            'valeur_vente': f"{valeur_vente:.2f}",
            'zakat': f"{zakat:.2f}"
        }
        logger.debug(f"Réponse valeur_stock: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans valeur_stock: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/valider_reception', methods=['POST'])
def valider_reception():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        return jsonify({"erreur": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    lignes = data['lignes']
    nature = "Bon de réception"

    conn = None
    try:
        logger.debug(f"Validation réception pour user_id: {user_id}, numero_four: {numero_four}, numero_util: {numero_util}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Verify user and password
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        
        utilisateur = cur.fetchone()
        if not utilisateur:
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Verify supplier
        if is_local:
            cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s", (numero_four,))
        else:
            cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        
        fournisseur = cur.fetchone()
        if not fournisseur:
            return jsonify({"erreur": "Fournisseur non trouvé"}), 400

        # Insert main movement
        if is_local:
            cur.execute("""
                INSERT INTO mouvement (date_m, etat_m, numero_four, refdoc, vers, nature, connection1, numero_util, cheque)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING numero_mouvement
            """, (datetime.utcnow(), "clôture", numero_four, "", "", nature, 0, numero_util, ""))
        else:
            cur.execute("""
                INSERT INTO mouvement (date_m, etat_m, numero_four, refdoc, vers, nature, connection1, numero_util, cheque, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING numero_mouvement
            """, (datetime.utcnow(), "clôture", numero_four, "", "", nature, 0, numero_util, "", user_id))
        
        numero_mouvement = cur.fetchone()['numero_mouvement']
        logger.debug(f"Mouvement inséré: numero_mouvement={numero_mouvement}")

        # Update refdoc
        cur.execute("UPDATE mouvement SET refdoc = %s WHERE numero_mouvement = %s", 
                    (str(numero_mouvement), numero_mouvement))

        # Calculate total cost and process lines
        total_cost = 0.0
        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if qtea <= 0:
                raise Exception("La quantité ajoutée doit être positive")

            # Verify item
            if is_local:
                cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s", (numero_item,))
            else:
                cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            prixba = float(item['prixba'] or 0)

            nqte = current_qte + qtea
            total_cost += qtea * prixbh

            # Insert details into attache2
            if is_local:
                cur.execute("""
                    INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (numero_item, numero_mouvement, qtea, nqte, str(prixbh)[:30], str(prixba)[:30], True))
            else:
                cur.execute("""
                    INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (numero_item, numero_mouvement, qtea, nqte, str(prixbh)[:30], str(prixba)[:30], True, user_id))

            # Update stock and purchase price
            if is_local:
                cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s", 
                            (nqte, str(prixbh), numero_item))
            else:
                cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                            (nqte, str(prixbh), numero_item, user_id))

        # Update supplier balance
        current_solde = float(fournisseur['solde'] or '0.0')
        new_solde = current_solde - total_cost
        new_solde_str = f"{new_solde:.2f}"

        if is_local:
            cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s", 
                        (new_solde_str, numero_four))
        else:
            cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                        (new_solde_str, numero_four, user_id))
        logger.debug(f"Solde fournisseur mis à jour: numero_fou={numero_four}, nouveau solde={new_solde_str}")

        conn.commit()
        logger.debug(f"Réception validée: numero_mouvement={numero_mouvement}")
        return jsonify({"numero_mouvement": numero_mouvement}), 200

    except Exception as e:
        logger.error(f"Erreur dans valider_reception: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/receptions_jour', methods=['GET'])
def receptions_jour():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    selected_date = request.args.get('date')
    numero_util = request.args.get('numero_util')
    numero_four = request.args.get('numero_four', '')

    conn = None
    try:
        logger.debug(f"Récupération des réceptions du jour pour user_id: {user_id}, date: {selected_date}, numero_util: {numero_util}, numero_four: {numero_four}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Define date range
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

        # Build SQL query
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
            WHERE {user_condition}
            AND m.date_m >= %s 
            AND m.date_m <= %s
            AND m.nature = 'Bon de réception'
        """
        params = [date_start, date_end]

        if not is_local:
            query = query.format(user_condition="m.user_id = %s AND a2.user_id = %s AND i.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
        else:
            query = query.format(user_condition="1=1")

        if numero_util and numero_util != '0':
            query += " AND m.numero_util = %s"
            params.append(numero_util)
        if numero_four and numero_four != '':
            query += " AND m.numero_four = %s"
            params.append(numero_four)

        query += " ORDER BY m.numero_mouvement DESC"
        logger.debug(f"Exécution de la requête receptions_jour: {query % tuple(params)}")

        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre de lignes de réceptions trouvées: {len(rows)}")

        # Process results
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
                'nprix': f"{float(row['nprix']):.2f}",
                'total_ligne': f"{float(row['qtea']) * float(row['nprix']):.2f}"
            })

            total += float(row['qtea']) * float(row['nprix'])

        receptions = list(receptions_map.values())

        response = {
            'receptions': receptions,
            'total': f"{total:.2f}"
        }
        logger.debug(f"Réponse receptions_jour: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans receptions_jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")
			

# --- Versements ---

@app.route('/ajouter_versement', methods=['POST'])
def ajouter_versement():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données d'annulation invalides")
        return jsonify({"erreur": "Type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    type_versement = data.get('type')  # 'C' pour client, 'F' pour fournisseur
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            return jsonify({"erreur": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'utilisateur et le mot de passe
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Vérifier client ou fournisseur
        if type_versement == 'C':
            if is_local:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s", (numero_cf,))
            else:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_cf, user_id))
            
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:  # 'F'
            if is_local:
                cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s", (numero_cf,))
            else:
                cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_cf, user_id))
            
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if type_versement == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"erreur": f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Mettre à jour le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde + montant  # Montant peut être positif ou négatif
        new_solde_str = f"{new_solde:.2f}"

        if is_local:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s",
                        (new_solde_str, numero_cf))
        else:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                        (new_solde_str, numero_cf, user_id))

        # Insérer le versement dans MOUVEMENTC
        now = datetime.utcnow()
        if is_local:
            cur.execute(
                """
                INSERT INTO MOUVEMENTC (date_mc, time_mc, montant, justificatif, numero_util, origine, cf, numero_cf)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING numero_mc
                """,
                (now.date(), now, f"{montant:.2f}", justificatif,
                 numero_util, origine, type_versement, numero_cf)
            )
        else:
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
        logger.debug(f"Versement ajouté: numero_mc={numero_mc}, type={type_versement}, montant={montant}")

        conn.commit()
        return jsonify({"numero_mc": numero_mc, "statut": "Versement ajouté"}), 201

    except ValueError:
        logger.error("Le montant doit être un nombre valide")
        return jsonify({"erreur": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        logger.error(f"Erreur dans ajouter_versement: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/historique_versements', methods=['GET'])
def historique_versements():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    selected_date = request.args.get('date')
    type_versement = request.args.get('type')  # 'C', 'F', ou vide pour tous

    conn = None
    try:
        logger.debug(f"Récupération historique versements pour user_id: {user_id}, date: {selected_date}, type: {type_versement}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Définir la plage de dates (30 jours si aucune date spécifique)
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                logger.error("Format de date invalide")
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = date_end - timedelta(days=30)

        # Construire la requête SQL
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
            WHERE {user_condition}
            AND mc.date_mc >= %s
            AND mc.date_mc <= %s
            AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
        """
        params = [date_start, date_end]

        if not is_local:
            query = query.format(user_condition="mc.user_id = %s AND cl.user_id = %s AND f.user_id = %s AND u.user_id = %s")
            params.insert(0, user_id)
            params.insert(1, user_id)
            params.insert(2, user_id)
            params.insert(3, user_id)
        else:
            query = query.format(user_condition="1=1")

        if type_versement in ['C', 'F']:
            query += " AND mc.cf = %s"
            params.append(type_versement)

        query += " ORDER BY mc.date_mc DESC, mc.time_mc DESC"
        logger.debug(f"Exécution de la requête historique_versements: {query % tuple(params)}")

        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre de versements trouvés: {len(rows)}")

        # Formater la réponse
        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': f"{float(row['montant']):.2f}",
                'justificatif': row['justificatif'] or '',
                'type': 'Client' if row['cf'] == 'C' else 'Fournisseur',
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        return jsonify(versements), 200

    except Exception as e:
        logger.error(f"Erreur dans historique_versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/annuler_versement', methods=['DELETE'])
def annuler_versement():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'numero_mc' not in data or 'type' not in data or 'numero_cf' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données d'annulation invalides")
        return jsonify({"erreur": "Numéro de versement, type, numéro client/fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    conn = None
    try:
        logger.debug(f"Annulation versement pour user_id: {user_id}, numero_mc: {numero_mc}, type: {type_versement}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'utilisateur et le mot de passe
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                       (numero_util, user_id))
        
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer le versement
        if is_local:
            cur.execute("""
                SELECT montant, cf, numero_cf 
                FROM MOUVEMENTC 
                WHERE numero_mc = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')
            """, (numero_mc,))
        else:
            cur.execute("""
                SELECT montant, cf, numero_cf 
                FROM MOUVEMENTC 
                WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')
            """, (numero_mc, user_id))
            
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement {numero_mc} non trouvé")
            return jsonify({"erreur": "Versement non trouvé"}), 404

        montant = float(versement['montant'])

        # Déterminer la table et la colonne ID
        if versement['cf'] == 'C':
            table = 'client'
            id_column = 'numero_clt'
        else:  # 'F'
            table = 'fournisseur'
            id_column = 'numero_fou'

        # Vérifier l'entité
        if is_local:
            cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s", (numero_cf,))
        else:
            cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", 
                       (numero_cf, user_id))
            
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"erreur": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Restaurer le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - montant  # Inverser l'effet du versement
        new_solde_str = f"{new_solde:.2f}"

        if is_local:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s",
                        (new_solde_str, numero_cf))
        else:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                        (new_solde_str, numero_cf, user_id))

        # Supprimer le versement
        if is_local:
            cur.execute("DELETE FROM MOUVEMENTC WHERE numero_mc = %s", (numero_mc,))
        else:
            cur.execute("DELETE FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s", 
                       (numero_mc, user_id))

        conn.commit()
        logger.info(f"Versement annulé: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"statut": "Versement annulé"}), 200

    except Exception as e:
        logger.error(f"Erreur dans annuler_versement: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/modifier_versement', methods=['PUT'])
def modifier_versement():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'numero_mc' not in data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de modification invalides")
        return jsonify({"erreur": "Numéro de versement, type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            return jsonify({"erreur": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'utilisateur et le mot de passe
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                       (numero_util, user_id))
            
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer le versement existant
        if is_local:
            cur.execute("""
                SELECT montant, cf, numero_cf 
                FROM MOUVEMENTC 
                WHERE numero_mc = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')
            """, (numero_mc,))
        else:
            cur.execute("""
                SELECT montant, cf, numero_cf 
                FROM MOUVEMENTC 
                WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')
            """, (numero_mc, user_id))
            
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement {numero_mc} non trouvé")
            return jsonify({"erreur": "Versement non trouvé"}), 404

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
        if is_local:
            cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s", (numero_cf,))
        else:
            cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", 
                       (numero_cf, user_id))
            
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"erreur": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Ajuster le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - old_montant + montant  # Annuler l'ancien montant et appliquer le nouveau
        new_solde_str = f"{new_solde:.2f}"

        if is_local:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s",
                        (new_solde_str, numero_cf))
        else:
            cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                        (new_solde_str, numero_cf, user_id))

        # Mettre à jour le versement
        now = datetime.utcnow()
        if is_local:
            cur.execute(
                """
                UPDATE MOUVEMENTC 
                SET montant = %s, justificatif = %s, date_mc = %s, time_mc = %s
                WHERE numero_mc = %s
                """,
                (f"{montant:.2f}", justificatif, now.date(), now, numero_mc)
            )
        else:
            cur.execute(
                """
                UPDATE MOUVEMENTC 
                SET montant = %s, justificatif = %s, date_mc = %s, time_mc = %s
                WHERE numero_mc = %s AND user_id = %s
                """,
                (f"{montant:.2f}", justificatif, now.date(), now, numero_mc, user_id)
            )

        conn.commit()
        logger.info(f"Versement modifié: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"statut": "Versement modifié"}), 200

    except ValueError:
        logger.error("Le montant doit être un nombre valide")
        return jsonify({"erreur": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        logger.error(f"Erreur dans modifier_versement: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/situation_versements', methods=['GET'])
def situation_versements():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    type_versement = request.args.get('type')  # 'C' ou 'F'
    numero_cf = request.args.get('numero_cf')  # ID du client ou fournisseur

    if not type_versement or type_versement not in ['C', 'F']:
        logger.error("Paramètre 'type' invalide")
        return jsonify({'erreur': "Paramètre 'type' requis et doit être 'C' ou 'F'"}), 400
    if not numero_cf:
        logger.error("Paramètre 'numero_cf' manquant")
        return jsonify({'erreur': "Paramètre 'numero_cf' requis"}), 400

    conn = None
    try:
        logger.debug(f"Récupération situation versements pour user_id: {user_id}, type: {type_versement}, numero_cf: {numero_cf}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
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
                WHERE mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
                AND mc.cf = %s
                AND mc.numero_cf = %s
                ORDER BY mc.date_mc DESC, mc.time_mc DESC
            """
            params = [type_versement, numero_cf]
        else:
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
                LEFT JOIN client cl ON mc.cf = 'C' AND mc.numero_cf = cl.numero_clt AND cl.user_id = %s
                LEFT JOIN fournisseur f ON mc.cf = 'F' AND mc.numero_cf = f.numero_fou AND f.user_id = %s
                LEFT JOIN utilisateur u ON mc.numero_util = u.numero_util AND u.user_id = %s
                WHERE mc.user_id = %s
                AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
                AND mc.cf = %s
                AND mc.numero_cf = %s
                ORDER BY mc.date_mc DESC, mc.time_mc DESC
            """
            params = [user_id, user_id, user_id, user_id, type_versement, numero_cf]

        logger.debug(f"Exécution de la requête situation_versements: {query % tuple(params)}")
        cur.execute(query, params)
        rows = cur.fetchall()
        logger.debug(f"Nombre de versements trouvés: {len(rows)}")

        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': f"{float(row['montant']):.2f}",
                'justificatif': row['justificatif'] or '',
                'cf': row['cf'],
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        return jsonify(versements), 200

    except Exception as e:
        logger.error(f"Erreur dans situation_versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Ventes ---


@app.route('/annuler_vente', methods=['POST'])
def annuler_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Erreur validation utilisateur: {user_id}")
        return user_id

    data = request.get_json()
    if not data or 'numero_comande' not in data or 'password2' not in data:
        logger.error("Données d'annulation vente invalides")
        return jsonify({"error": "Numéro de commande ou mot de passe manquant"}), 400

    numero_comande = data.get('numero_comande')
    password2 = data.get('password2')

    conn = None
    try:
        logger.debug(f"Annulation vente pour user_id: {user_id}, numero_comande: {numero_comande}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier si la connexion est locale
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'existence de la commande et récupérer l'utilisateur
        if is_local:
            cur.execute("""
                SELECT c.numero_table, c.nature, c.numero_util, u.password2 
                FROM comande c
                JOIN utilisateur u ON c.numero_util = u.numero_util
                WHERE c.numero_comande = %s
            """, (numero_comande,))
        else:
            cur.execute("""
                SELECT c.numero_table, c.nature, c.numero_util, u.password2 
                FROM comande c
                JOIN utilisateur u ON c.numero_util = u.numero_util AND u.user_id = %s
                WHERE c.numero_comande = %s AND c.user_id = %s
            """, (user_id, numero_comande, user_id))
            
        commande = cur.fetchone()
        if not commande:
            logger.error(f"Commande {numero_comande} non trouvée")
            return jsonify({"error": "Commande non trouvée"}), 404

        # Vérifier le mot de passe
        if commande['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour annuler la commande {numero_comande}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la vente
        if is_local:
            cur.execute("""
                SELECT numero_item, quantite, prixt
                FROM attache 
                WHERE numero_comande = %s
            """, (numero_comande,))
        else:
            cur.execute("""
                SELECT numero_item, quantite, prixt
                FROM attache 
                WHERE numero_comande = %s AND user_id = %s
            """, (numero_comande, user_id))
            
        lignes = cur.fetchall()

        if not lignes:
            logger.error(f"Aucune ligne trouvée pour la commande {numero_comande}")
            return jsonify({"error": "Aucune ligne de vente trouvée"}), 404

        # Restaurer le stock dans item
        for ligne in lignes:
            quantite = float(ligne['quantite'] or 0)
            if is_local:
                cur.execute("""
                    UPDATE item 
                    SET qte = qte + %s 
                    WHERE numero_item = %s
                """, (quantite, ligne['numero_item']))
            else:
                cur.execute("""
                    UPDATE item 
                    SET qte = qte + %s 
                    WHERE numero_item = %s AND user_id = %s
                """, (quantite, ligne['numero_item'], user_id))

        # Si vente à terme (numero_table != '0'), ajuster le solde du client
        if commande['numero_table'] != '0':
            total_sale = sum(float(ligne['prixt'] or 0) for ligne in lignes)
            if is_local:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s", 
                           (commande['numero_table'],))
            else:
                cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                           (commande['numero_table'], user_id))
                
            client = cur.fetchone()
            if not client:
                logger.error(f"Client {commande['numero_table']} non trouvé")
                raise Exception(f"Client {commande['numero_table']} non trouvé")
            
            current_solde = float(client['solde'] or 0)
            new_solde = current_solde - total_sale  # Réduire la dette (inverser la vente)
            new_solde_str = f"{new_solde:.2f}"
            
            if is_local:
                cur.execute("""
                    UPDATE client 
                    SET solde = %s 
                    WHERE numero_clt = %s
                """, (new_solde_str, commande['numero_table']))
            else:
                cur.execute("""
                    UPDATE client 
                    SET solde = %s 
                    WHERE numero_clt = %s AND user_id = %s
                """, (new_solde_str, commande['numero_table'], user_id))
                
            logger.debug(f"Solde client mis à jour: numero_clt={commande['numero_table']}, total_sale={total_sale}, new_solde={new_solde_str}")

        # Supprimer les lignes de attache
        if is_local:
            cur.execute("DELETE FROM attache WHERE numero_comande = %s", (numero_comande,))
        else:
            cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", 
                        (numero_comande, user_id))

        # Supprimer la commande
        if is_local:
            cur.execute("DELETE FROM comande WHERE numero_comande = %s", (numero_comande,))
        else:
            cur.execute("DELETE FROM comande WHERE numero_comande = %s AND user_id = %s", 
                        (numero_comande, user_id))

        conn.commit()
        logger.info(f"Vente annulée: numero_comande={numero_comande}, {len(lignes)} lignes")
        return jsonify({"statut": "Vente annulée"}), 200

    except Exception as e:
        logger.error(f"Erreur dans annuler_vente: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")
```



		
# --- Réceptions ---

@app.route('/annuler_reception', methods=['POST'])
def annuler_reception():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'numero_mouvement' not in data or 'password2' not in data:
        logger.error("Données d'annulation réception invalides")
        return jsonify({"erreur": "Numéro de mouvement ou mot de passe manquant"}), 400

    numero_mouvement = data.get('numero_mouvement')
    password2 = data.get('password2')

    conn = None
    try:
        logger.debug(f"Annulation réception pour user_id: {user_id}, numero_mouvement: {numero_mouvement}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'existence du mouvement et récupérer l'utilisateur
        if is_local:
            cur.execute("""
                SELECT m.numero_four, m.numero_util, u.password2 
                FROM mouvement m
                JOIN utilisateur u ON m.numero_util = u.numero_util
                WHERE m.numero_mouvement = %s AND m.nature = 'Bon de réception'
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT m.numero_four, m.numero_util, u.password2 
                FROM mouvement m
                JOIN utilisateur u ON m.numero_util = u.numero_util AND u.user_id = %s
                WHERE m.numero_mouvement = %s AND m.user_id = %s AND m.nature = 'Bon de réception'
            """, (user_id, numero_mouvement, user_id))
            
        mouvement = cur.fetchone()
        if not mouvement:
            logger.error(f"Mouvement {numero_mouvement} non trouvé")
            return jsonify({"erreur": "Mouvement non trouvé"}), 404

        # Vérifier le mot de passe
        if mouvement['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour annuler le mouvement {numero_mouvement}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la réception
        if is_local:
            cur.execute("""
                SELECT numero_item, qtea, nprix 
                FROM attache2 
                WHERE numero_mouvement = %s
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT numero_item, qtea, nprix 
                FROM attache2 
                WHERE numero_mouvement = %s AND user_id = %s
            """, (numero_mouvement, user_id))
            
        lignes = cur.fetchall()

        if not lignes:
            logger.error(f"Aucune ligne trouvée pour le mouvement {numero_mouvement}")
            return jsonify({"erreur": "Aucune ligne de réception trouvée"}), 404

        # Calculer le coût total de la réception
        total_cost = sum(float(ligne['qtea']) * float(ligne['nprix'] or 0) for ligne in lignes)

        # Restaurer le stock dans item
        for ligne in lignes:
            if is_local:
                cur.execute("""
                    UPDATE item 
                    SET qte = qte - %s 
                    WHERE numero_item = %s
                """, (ligne['qtea'], ligne['numero_item']))
            else:
                cur.execute("""
                    UPDATE item 
                    SET qte = qte - %s 
                    WHERE numero_item = %s AND user_id = %s
                """, (ligne['qtea'], ligne['numero_item'], user_id))

        # Mettre à jour le solde du fournisseur
        if is_local:
            cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s", 
                       (mouvement['numero_four'],))
        else:
            cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", 
                       (mouvement['numero_four'], user_id))
            
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur {mouvement['numero_four']} non trouvé")
            raise Exception(f"Fournisseur {mouvement['numero_four']} non trouvé")

        current_solde = float(fournisseur['solde'] or '0.0')
        new_solde = current_solde + total_cost  # Inverser l'effet de la réception
        new_solde_str = f"{new_solde:.2f}"

        if is_local:
            cur.execute("""
                UPDATE fournisseur 
                SET solde = %s 
                WHERE numero_fou = %s
            """, (new_solde_str, mouvement['numero_four']))
        else:
            cur.execute("""
                UPDATE fournisseur 
                SET solde = %s 
                WHERE numero_fou = %s AND user_id = %s
            """, (new_solde_str, mouvement['numero_four'], user_id))
                
        logger.debug(f"Solde fournisseur mis à jour: numero_fou={mouvement['numero_four']}, total_cost={total_cost}, new_solde={new_solde_str}")

        # Supprimer les lignes de attache2
        if is_local:
            cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s", (numero_mouvement,))
        else:
            cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", 
                        (numero_mouvement, user_id))

        # Supprimer le mouvement
        if is_local:
            cur.execute("DELETE FROM mouvement WHERE numero_mouvement = %s", (numero_mouvement,))
        else:
            cur.execute("DELETE FROM mouvement WHERE numero_mouvement = %s AND user_id = %s", 
                        (numero_mouvement, user_id))

        conn.commit()
        logger.info(f"Réception annulée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"statut": "Réception annulée"}), 200

    except Exception as e:
        logger.error(f"Erreur dans annuler_reception: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Modification de vente ---

@app.route('/modifier_vente/<numero_comande>', methods=['PUT'])
def modifier_vente(numero_comande):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de vente invalides")
        return jsonify({"erreur": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', '0')
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    lignes = data['lignes']
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    nature = "TICKET" if numero_table == '0' else "BON DE L."

    conn = None
    try:
        logger.debug(f"Modification vente pour user_id: {user_id}, numero_comande: {numero_comande}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'utilisateur et le mot de passe
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                       (numero_util, user_id))
            
        utilisateur = cur.fetchone()
        if not utilisateur or utilisateur['password2'] != password2:
            logger.error("Utilisateur ou mot de passe incorrect")
            return jsonify({"erreur": "Utilisateur ou mot de passe incorrect"}), 401

        # Vérifier l'existence de la commande
        if is_local:
            cur.execute("SELECT * FROM comande WHERE numero_comande = %s", (numero_comande,))
        else:
            cur.execute("SELECT * FROM comande WHERE numero_comande = %s AND user_id = %s", 
                       (numero_comande, user_id))
            
        if not cur.fetchone():
            logger.error(f"Commande {numero_comande} non trouvée")
            return jsonify({"erreur": "Commande non trouvée"}), 404

        # Restaurer le stock des anciens articles
        if is_local:
            cur.execute("SELECT numero_item, quantite FROM attache WHERE numero_comande = %s", (numero_comande,))
        else:
            cur.execute("SELECT numero_item, quantite FROM attache WHERE numero_comande = %s AND user_id = %s", 
                       (numero_comande, user_id))
            
        old_lignes = cur.fetchall()
        for ligne in old_lignes:
            if is_local:
                cur.execute("UPDATE item SET qte = qte + %s WHERE numero_item = %s",
                           (ligne['quantite'], ligne['numero_item']))
            else:
                cur.execute("UPDATE item SET qte = qte + %s WHERE numero_item = %s AND user_id = %s",
                           (ligne['quantite'], ligne['numero_item'], user_id))

        # Supprimer les anciennes lignes
        if is_local:
            cur.execute("DELETE FROM attache WHERE numero_comande = %s", (numero_comande,))
        else:
            cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", 
                       (numero_comande, user_id))

        # Mettre à jour la commande (sans toucher au solde)
        if is_local:
            cur.execute("""
                UPDATE comande 
                SET numero_table = %s, date_comande = %s, nature = %s, numero_util = %s
                WHERE numero_comande = %s
            """, (numero_table, date_comande, nature, numero_util, numero_comande))
        else:
            cur.execute("""
                UPDATE comande 
                SET numero_table = %s, date_comande = %s, nature = %s, numero_util = %s
                WHERE numero_comande = %s AND user_id = %s
            """, (numero_table, date_comande, nature, numero_util, numero_comande, user_id))

        # Insérer les nouvelles lignes et ajuster le stock
        for ligne in lignes:
            if is_local:
                cur.execute("""
                    INSERT INTO attache (numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (numero_comande, ligne.get('numero_item'), ligne.get('quantite'), ligne.get('prixt'),
                     ligne.get('remarque', ''), ligne.get('prixbh', '0.00'), 0))
                
                cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s",
                           (ligne.get('quantite'), ligne.get('numero_item')))
            else:
                cur.execute("""
                    INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, numero_comande, ligne.get('numero_item'), ligne.get('quantite'), ligne.get('prixt'),
                     ligne.get('remarque', ''), ligne.get('prixbh', '0.00'), 0))
                
                cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s",
                           (ligne.get('quantite'), ligne.get('numero_item'), user_id))

        conn.commit()
        logger.info(f"Vente modifiée: numero_comande={numero_comande}, {len(lignes)} lignes")
        return jsonify({"numero_comande": numero_comande, "statut": "Vente modifiée"}), 200

    except Exception as e:
        logger.error(f"Erreur dans modifier_vente: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Consultation de vente ---

@app.route('/vente/<numero_comande>', methods=['GET'])
def get_vente(numero_comande):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    conn = None
    try:
        logger.debug(f"Récupération vente pour user_id: {user_id}, numero_comande: {numero_comande}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Récupérer les détails de la commande
        if is_local:
            cur.execute("""
                SELECT c.numero_comande, c.numero_table, c.date_comande, c.nature, c.numero_util,
                       cl.nom AS client_nom, u.nom AS utilisateur_nom
                FROM comande c
                LEFT JOIN client cl ON c.numero_table = cl.numero_clt
                LEFT JOIN utilisateur u ON c.numero_util = u.numero_util
                WHERE c.numero_comande = %s
            """, (numero_comande,))
        else:
            cur.execute("""
                SELECT c.numero_comande, c.numero_table, c.date_comande, c.nature, c.numero_util,
                       cl.nom AS client_nom, u.nom AS utilisateur_nom
                FROM comande c
                LEFT JOIN client cl ON c.numero_table = cl.numero_clt AND cl.user_id = %s
                LEFT JOIN utilisateur u ON c.numero_util = u.numero_util AND u.user_id = %s
                WHERE c.numero_comande = %s AND c.user_id = %s
            """, (user_id, user_id, numero_comande, user_id))
            
        commande = cur.fetchone()

        if not commande:
            logger.error(f"Commande {numero_comande} non trouvée")
            return jsonify({"erreur": "Commande non trouvée"}), 404

        # Récupérer les lignes de la commande
        if is_local:
            cur.execute("""
                SELECT a.numero_item, a.quantite, a.prixt, a.remarque, a.prixbh, i.designation
                FROM attache a
                JOIN item i ON a.numero_item = i.numero_item
                WHERE a.numero_comande = %s
            """, (numero_comande,))
        else:
            cur.execute("""
                SELECT a.numero_item, a.quantite, a.prixt, a.remarque, a.prixbh, i.designation
                FROM attache a
                JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
                WHERE a.numero_comande = %s AND a.user_id = %s
            """, (user_id, numero_comande, user_id))
            
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
                    'prixt': f"{float(ligne['prixt'] or 0):.2f}",
                    'remarque': ligne['remarque'] or '',
                    'prixbh': f"{float(ligne['prixbh'] or 0):.2f}"
                }
                for ligne in lignes
            ]
        }

        logger.debug(f"Vente récupérée: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans get_vente: {str(e)}")
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Consultation de réception ---

@app.route('/reception/<numero_mouvement>', methods=['GET'])
def get_reception(numero_mouvement):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    conn = None
    try:
        logger.debug(f"Récupération réception pour user_id: {user_id}, numero_mouvement: {numero_mouvement}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Récupérer les détails du mouvement
        if is_local:
            cur.execute("""
                SELECT m.numero_mouvement, m.numero_four, m.date_m, m.nature, m.numero_util,
                       f.nom AS fournisseur_nom, u.nom AS utilisateur_nom
                FROM mouvement m
                LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou
                LEFT JOIN utilisateur u ON m.numero_util = u.numero_util
                WHERE m.numero_mouvement = %s AND m.nature = 'Bon de réception'
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT m.numero_mouvement, m.numero_four, m.date_m, m.nature, m.numero_util,
                       f.nom AS fournisseur_nom, u.nom AS utilisateur_nom
                FROM mouvement m
                LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou AND f.user_id = %s
                LEFT JOIN utilisateur u ON m.numero_util = u.numero_util AND u.user_id = %s
                WHERE m.numero_mouvement = %s AND m.user_id = %s AND m.nature = 'Bon de réception'
            """, (user_id, user_id, numero_mouvement, user_id))
            
        mouvement = cur.fetchone()

        if not mouvement:
            logger.error(f"Réception {numero_mouvement} non trouvée")
            return jsonify({"erreur": "Réception non trouvée"}), 404

        # Récupérer les lignes du mouvement
        if is_local:
            cur.execute("""
                SELECT a2.numero_item, a2.qtea, a2.nprix, a2.nqte, a2.pump, i.designation
                FROM attache2 a2
                JOIN item i ON a2.numero_item = i.numero_item
                WHERE a2.numero_mouvement = %s
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT a2.numero_item, a2.qtea, a2.nprix, a2.nqte, a2.pump, i.designation
                FROM attache2 a2
                JOIN item i ON a2.numero_item = i.numero_item AND i.user_id = %s
                WHERE a2.numero_mouvement = %s AND a2.user_id = %s
            """, (user_id, numero_mouvement, user_id))
            
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
                    'nprix': f"{float(ligne['nprix'] or 0):.2f}",
                    'nqte': ligne['nqte'],
                    'pump': f"{float(ligne['pump'] or 0):.2f}"
                }
                for ligne in lignes
            ]
        }

        logger.debug(f"Réception récupérée: {response}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur dans get_reception: {str(e)}")
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Modifier Réception ---

@app.route('/modifier_reception/<numero_mouvement>', methods=['PUT'])
def modifier_reception(numero_mouvement):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de réception invalides")
        return jsonify({"erreur": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    lignes = data['lignes']

    conn = None
    try:
        logger.debug(f"Modification réception pour user_id: {user_id}, numero_mouvement: {numero_mouvement}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'utilisateur et le mot de passe
        if is_local:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        else:
            cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                       (numero_util, user_id))
            
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        if is_local:
            cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s", (numero_four,))
        else:
            cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", 
                       (numero_four, user_id))
            
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur {numero_four} non trouvé")
            return jsonify({"erreur": "Fournisseur non trouvé"}), 400

        # Vérifier que la réception existe
        if is_local:
            cur.execute("""
                SELECT numero_mouvement, numero_four 
                FROM mouvement 
                WHERE numero_mouvement = %s AND nature = 'Bon de réception'
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT numero_mouvement, numero_four 
                FROM mouvement 
                WHERE numero_mouvement = %s AND user_id = %s AND nature = 'Bon de réception'
            """, (numero_mouvement, user_id))
            
        mouvement = cur.fetchone()
        if not mouvement:
            logger.error(f"Réception {numero_mouvement} non trouvée")
            return jsonify({"erreur": "Réception non trouvée"}), 404

        # Récupérer les lignes précédentes de la réception
        if is_local:
            cur.execute("""
                SELECT numero_item, qtea, nprix
                FROM attache2
                WHERE numero_mouvement = %s
            """, (numero_mouvement,))
        else:
            cur.execute("""
                SELECT numero_item, qtea, nprix
                FROM attache2
                WHERE numero_mouvement = %s AND user_id = %s
            """, (numero_mouvement, user_id))
            
        old_lines = cur.fetchall()
        old_lines_dict = {line['numero_item']: line for line in old_lines}
        old_total_cost = sum(float(line['qtea']) * float(line['nprix']) for line in old_lines)
        logger.debug(f"Coût total réception précédente: {old_total_cost}")

        # Restaurer le solde initial
        current_solde = float(fournisseur['solde'] or '0.0')
        restored_solde = current_solde + old_total_cost
        logger.debug(f"Solde restauré: {restored_solde}")

        # Calculer le nouveau coût total et préparer les mises à jour du stock
        new_total_cost = 0.0
        stock_updates = {}

        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            new_qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if new_qtea < 0:
                raise Exception("La quantité ajoutée ne peut pas être négative")
            if prixbh < 0:
                raise Exception("Le prix d'achat ne peut pas être négatif")

            # Vérifier l'article
            if is_local:
                cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s", (numero_item,))
            else:
                cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", 
                           (numero_item, user_id))
                
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            current_prixba = float(item['prixba'] or 0)
            old_qtea = float(old_lines_dict.get(numero_item, {}).get('qtea', 0))

            new_total_cost += new_qtea * prixbh
            stock_updates[numero_item] = {
                'old_qtea': old_qtea,
                'new_qtea': new_qtea,
                'prixbh': prixbh,
                'current_qte': current_qte,
                'current_prixba': current_prixba
            }

        # Traiter les articles supprimés
        for numero_item, old_line in old_lines_dict.items():
            if numero_item not in stock_updates:
                if is_local:
                    cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s", (numero_item,))
                else:
                    cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", 
                               (numero_item, user_id))
                    
                item = cur.fetchone()
                stock_updates[numero_item] = {
                    'old_qtea': float(old_line['qtea']),
                    'new_qtea': 0,
                    'prixbh': 0,
                    'current_qte': float(item['qte'] or 0),
                    'current_prixba': float(item['prixba'] or 0)
                }

        # Mettre à jour le solde du fournisseur
        new_solde = restored_solde - new_total_cost
        new_solde_str = f"{new_solde:.2f}"
        
        if is_local:
            cur.execute("""
                UPDATE fournisseur 
                SET solde = %s 
                WHERE numero_fou = %s
            """, (new_solde_str, numero_four))
        else:
            cur.execute("""
                UPDATE fournisseur 
                SET solde = %s 
                WHERE numero_fou = %s AND user_id = %s
            """, (new_solde_str, numero_four, user_id))
                
        logger.debug(f"Solde fournisseur mis à jour: numero_fou={numero_four}, new_total_cost={new_total_cost}, new_solde={new_solde_str}")

        # Supprimer les anciennes lignes
        if is_local:
            cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s", (numero_mouvement,))
        else:
            cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", 
                        (numero_mouvement, user_id))

        # Insérer les nouvelles lignes et mettre à jour le stock
        for numero_item, update_info in stock_updates.items():
            old_qtea = update_info['old_qtea']
            new_qtea = update_info['new_qtea']
            prixbh = update_info['prixbh']
            current_qte = update_info['current_qte']
            current_prixba = update_info['current_prixba']

            restored_qte = current_qte - old_qtea
            new_qte = restored_qte + new_qtea

            if new_qte < 0:
                raise Exception(f"Stock négatif pour l'article {numero_item}: {new_qte}")

            if new_qtea > 0:
                if is_local:
                    cur.execute("""
                        INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (numero_item, numero_mouvement, new_qtea, new_qte, f"{prixbh:.2f}", f"{prixbh:.2f}", True))
                else:
                    cur.execute("""
                        INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (numero_item, numero_mouvement, new_qtea, new_qte, f"{prixbh:.2f}", f"{prixbh:.2f}", True, user_id))

            # Mettre à jour le stock
            new_prixba = prixbh if new_qtea > 0 else current_prixba
            if is_local:
                cur.execute("""
                    UPDATE item 
                    SET qte = %s, prixba = %s 
                    WHERE numero_item = %s
                """, (new_qte, f"{new_prixba:.2f}", numero_item))
            else:
                cur.execute("""
                    UPDATE item 
                    SET qte = %s, prixba = %s 
                    WHERE numero_item = %s AND user_id = %s
                """, (new_qte, f"{new_prixba:.2f}", numero_item, user_id))
                
            logger.debug(f"Stock mis à jour: numero_item={numero_item}, old_qtea={old_qtea}, new_qtea={new_qtea}, new_qte={new_qte}")

        # Mettre à jour le mouvement
        if is_local:
            cur.execute("""
                UPDATE mouvement 
                SET numero_four = %s, numero_util = %s, date_m = %s
                WHERE numero_mouvement = %s
            """, (numero_four, numero_util, datetime.utcnow(), numero_mouvement))
        else:
            cur.execute("""
                UPDATE mouvement 
                SET numero_four = %s, numero_util = %s, date_m = %s
                WHERE numero_mouvement = %s AND user_id = %s
            """, (numero_four, numero_util, datetime.utcnow(), numero_mouvement, user_id))

        conn.commit()
        logger.info(f"Réception modifiée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"numero_mouvement": numero_mouvement, "statut": "Réception modifiée"}), 200

    except Exception as e:
        logger.error(f"Erreur dans modifier_reception: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

# --- Catégories ---

@app.route('/liste_categories', methods=['GET'])
def liste_categories():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    conn = None
    try:
        logger.debug(f"Récupération liste catégories pour user_id: {user_id}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute("SELECT numer_categorie, description_c FROM categorie ORDER BY description_c")
        else:
            cur.execute("SELECT numer_categorie, description_c FROM categorie WHERE user_id = %s ORDER BY description_c", 
                        (user_id,))
            
        categories = cur.fetchall()
        logger.debug(f"Nombre de catégories trouvées: {len(categories)}")
        return jsonify(categories), 200

    except Exception as e:
        logger.error(f"Erreur dans liste_categories: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/ajouter_categorie', methods=['POST'])
def ajouter_categorie():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    description_c = data.get('description_c')
    if not description_c:
        logger.error("Description de catégorie manquante")
        return jsonify({'erreur': 'Description requise'}), 400

    conn = None
    try:
        logger.debug(f"Ajout catégorie pour user_id: {user_id}, description: {description_c}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor()

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute(
                "INSERT INTO categorie (description_c) VALUES (%s) RETURNING numer_categorie",
                (description_c,)
            )
        else:
            cur.execute(
                "INSERT INTO categorie (description_c, user_id) VALUES (%s, %s) RETURNING numer_categorie",
                (description_c, user_id)
            )
            
        category_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Catégorie ajoutée: numer_categorie={category_id}")
        return jsonify({'statut': 'Catégorie ajoutée', 'id': category_id}), 201

    except Exception as e:
        logger.error(f"Erreur dans ajouter_categorie: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/modifier_categorie/<numer_categorie>', methods=['PUT'])
def modifier_categorie(numer_categorie):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    description_c = data.get('description_c')
    if not description_c:
        logger.error("Description de catégorie manquante")
        return jsonify({'erreur': 'Description requise'}), 400

    conn = None
    try:
        logger.debug(f"Modification catégorie pour user_id: {user_id}, numer_categorie: {numer_categorie}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor()

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if is_local:
            cur.execute(
                "UPDATE categorie SET description_c = %s WHERE numer_categorie = %s RETURNING numer_categorie",
                (description_c, numer_categorie)
            )
        else:
            cur.execute(
                "UPDATE categorie SET description_c = %s WHERE numer_categorie = %s AND user_id = %s RETURNING numer_categorie",
                (description_c, numer_categorie, user_id)
            )
            
        if cur.rowcount == 0:
            logger.error(f"Catégorie {numer_categorie} non trouvée")
            return jsonify({'erreur': 'Catégorie non trouvée'}), 404
            
        conn.commit()
        logger.info(f"Catégorie modifiée: numer_categorie={numer_categorie}")
        return jsonify({'statut': 'Catégorie modifiée'}), 200

    except Exception as e:
        logger.error(f"Erreur dans modifier_categorie: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/supprimer_categorie/<numer_categorie>', methods=['DELETE'])
def supprimer_categorie(numer_categorie):
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    conn = None
    try:
        logger.debug(f"Suppression catégorie pour user_id: {user_id}, numer_categorie: {numer_categorie}")
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor()

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier si la catégorie est utilisée
        if is_local:
            cur.execute("SELECT 1 FROM item WHERE numero_categorie = %s", (numer_categorie,))
        else:
            cur.execute("SELECT 1 FROM item WHERE numero_categorie = %s AND user_id = %s", 
                        (numer_categorie, user_id))
            
        if cur.fetchone():
            logger.error(f"Catégorie {numer_categorie} utilisée par des produits")
            return jsonify({'erreur': 'Catégorie utilisée par des produits'}), 400

        if is_local:
            cur.execute("DELETE FROM categorie WHERE numer_categorie = %s", (numer_categorie,))
        else:
            cur.execute("DELETE FROM categorie WHERE numer_categorie = %s AND user_id = %s", 
                       (numer_categorie, user_id))
            
        if cur.rowcount == 0:
            logger.error(f"Catégorie {numer_categorie} non trouvée")
            return jsonify({'erreur': 'Catégorie non trouvée'}), 404
            
        conn.commit()
        logger.info(f"Catégorie supprimée: numer_categorie={numer_categorie}")
        return jsonify({'statut': 'Catégorie supprimée'}), 200

    except Exception as e:
        logger.error(f"Erreur dans supprimer_categorie: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/assigner_categorie', methods=['POST'])
def assigner_categorie():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    if not data:
        logger.error("Données JSON manquantes")
        return jsonify({'erreur': 'Données JSON requises'}), 400

    numero_item = data.get('numero_item')
    numero_categorie = data.get('numer_categorie')

    logger.debug(f"Assignation catégorie pour user_id: {user_id}, numero_item: {numero_item}, numer_categorie: {numero_categorie}")

    if numero_item is None:
        logger.error("numero_item manquant")
        return jsonify({'erreur': 'Numéro d\'article requis'}), 400

    try:
        numero_item = int(numero_item)
    except (ValueError, TypeError) as e:
        logger.error(f"numero_item invalide: {str(e)}")
        return jsonify({'erreur': 'Numéro d\'article doit être un entier'}), 400

    if numero_categorie is not None:
        try:
            numero_categorie = int(numero_categorie)
        except (ValueError, TypeError) as e:
            logger.error(f"numero_categorie invalide: {str(e)}")
            return jsonify({'erreur': 'Numéro de catégorie doit être un entier'}), 400

    conn = None
    try:
        conn = get_conn(user_id)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        # Vérifier l'article
        if is_local:
            cur.execute("SELECT numero_item, designation FROM item WHERE numero_item = %s", (numero_item,))
        else:
            cur.execute("SELECT numero_item, designation FROM item WHERE numero_item = %s AND user_id = %s", 
                       (numero_item, user_id))
            
        item = cur.fetchone()
        if not item:
            logger.error(f"Article {numero_item} non trouvé")
            return jsonify({'erreur': f'Article {numero_item} non trouvé'}), 404

        # Vérifier la catégorie si spécifiée
        if numero_categorie is not None:
            if is_local:
                cur.execute("SELECT numer_categorie, description_c FROM categorie WHERE numer_categorie = %s", 
                           (numero_categorie,))
            else:
                cur.execute("SELECT numer_categorie, description_c FROM categorie WHERE numer_categorie = %s AND user_id = %s", 
                           (numero_categorie, user_id))
                
            category = cur.fetchone()
            if not category:
                logger.error(f"Catégorie {numero_categorie} non trouvée")
                return jsonify({'erreur': f'Catégorie {numero_categorie} non trouvée'}), 404

        # Mettre à jour la catégorie de l'article
        if is_local:
            cur.execute("""
                UPDATE item 
                SET numero_categorie = %s 
                WHERE numero_item = %s 
                RETURNING numero_categorie
            """, (numero_categorie, numero_item))
        else:
            cur.execute("""
                UPDATE item 
                SET numero_categorie = %s 
                WHERE numero_item = %s AND user_id = %s 
                RETURNING numero_categorie
            """, (numero_categorie, numero_item, user_id))
            
        if cur.rowcount == 0:
            logger.error("Aucun article mis à jour")
            return jsonify({'erreur': 'Aucun article mis à jour'}), 404

        conn.commit()
        logger.info(f"Catégorie assignée: numero_item={numero_item}, numer_categorie={numero_categorie}")
        return jsonify({
            'statut': 'Catégorie assignée',
            'numero_item': numero_item,
            'numer_categorie': numero_categorie
        }), 200

    except Exception as e:
        logger.error(f"Erreur dans assigner_categorie: {str(e)}")
        if conn:
            conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")

@app.route('/liste_produits_par_categorie', methods=['GET'])
def liste_produits_par_categorie():
    user_id = validate_user()
    if isinstance(user_id, tuple):
        return user_id

    numero_categorie = request.args.get('numero_categorie', type=int)
    conn = None
    try:
        logger.debug(f"Récupération produits par catégorie pour user_id: {user_id}, numero_categorie: {numero_categorie}")
        conn = get_conn(user_id)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check if connection is local
        config = get_local_db_config(user_id)
        is_local = config and config['local_db_host']
        logger.debug(f"Connexion {'locale' if is_local else 'Supabase'} pour user_id: {user_id}")

        if numero_categorie is None and 'numero_categorie' in request.args:
            # Produits sans catégorie
            if is_local:
                cur.execute("SELECT numero_item, designation FROM item WHERE numero_categorie IS NULL")
            else:
                cur.execute("SELECT numero_item, designation FROM item WHERE numero_categorie IS NULL AND user_id = %s", 
                           (user_id,))
                
            produits = cur.fetchall()
            logger.debug(f"Nombre de produits sans catégorie trouvés: {len(produits)}")
            return jsonify({'produits': produits}), 200
        else:
            # Produits par catégorie
            if is_local:
                cur.execute("""
                    SELECT c.numer_categorie, c.description_c, i.numero_item, i.designation
                    FROM categorie c
                    LEFT JOIN item i ON c.numer_categorie = i.numero_categorie
                    WHERE c.numer_categorie = %s OR %s IS NULL
                """, (numero_categorie, numero_categorie))
            else:
                cur.execute("""
                    SELECT c.numer_categorie, c.description_c, i.numero_item, i.designation
                    FROM categorie c
                    LEFT JOIN item i ON c.numer_categorie = i.numero_categorie AND i.user_id = %s
                    WHERE c.user_id = %s AND (c.numer_categorie = %s OR %s IS NULL)
                """, (user_id, user_id, numero_categorie, numero_categorie))
                
            rows = cur.fetchall()
            logger.debug(f"Nombre de lignes trouvées: {len(rows)}")
            categories = {}
            for row in rows:
                cat_id = row['numer_categorie']
                if cat_id not in categories:
                    categories[cat_id] = {
                        'numero_categorie': cat_id,
                        'description_c': row['description_c'],
                        'produits': []
                    }
                if row['numero_item']:
                    categories[cat_id]['produits'].append({
                        'numero_item': row['numero_item'],
                        'designation': row['designation']
                    })
            
            return jsonify({'categories': list(categories.values())}), 200

    except Exception as e:
        logger.error(f"Erreur dans liste_produits_par_categorie: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
            logger.debug("Connexion fermée")
# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))					
			
