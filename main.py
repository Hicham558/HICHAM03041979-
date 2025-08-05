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

# Fonctions utilitaires
def to_comma_decimal(value):
    """Convertit un float ou string en string avec virgule comme séparateur décimal."""
    try:
        return f"{float(value):.2f}".replace('.', ',')
    except (ValueError, TypeError):
        return '0,00'

def to_dot_decimal(value):
    """Convertit un string avec virgule en float avec point."""
    try:
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return 0.0

# Connexion à la base de données
def get_conn():
    url = os.environ['DATABASE_URL']
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode='require')

# Vérification de l'utilisateur (X-User-ID)
def validate_user_id():
    user_id = request.headers.get('X-User-ID')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("Identifiant utilisateur manquant ou invalide")
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
        logger.info("Connexion PostgreSQL réussie")
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        logger.error(f"Erreur connexion DB: {str(e)}")
        return f'Erreur connexion DB: {str(e)}', 500

# Endpoint: Rechercher produit par code-barres
@app.route('/rechercher_produit_codebar', methods=['GET'])
def rechercher_produit_codebar():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    codebar = request.args.get('codebar')
    if not codebar:
        logger.error("Code-barres requis")
        return jsonify({'erreur': 'Code-barres requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Rechercher par code-barres principal dans item
        cur.execute("""
            SELECT numero_item, bar, designation, prix, prixba, qte
            FROM item
            WHERE bar = %s AND user_id = %s
        """, (codebar, user_id))
        produit = cur.fetchone()

        if produit:
            produit['prix'] = to_comma_decimal(produit['prix'])
            produit['prixba'] = to_comma_decimal(produit['prixba'])
            cur.close()
            conn.close()
            logger.info(f"Produit trouvé (principal): codebar={codebar}, user_id={user_id}")
            return jsonify({
                'statut': 'trouvé',
                'type': 'principal',
                'produit': produit
            }), 200

        # Rechercher dans codebar pour un code-barres lié
        cur.execute("""
            SELECT i.numero_item, i.bar, i.designation, i.prix, i.prixba, i.qte
            FROM codebar c
            JOIN item i ON c.bar = i.numero_item::varchar
            WHERE c.bar2 = %s AND i.user_id = %s
        """, (codebar, user_id))
        produit = cur.fetchone()

        if produit:
            produit['prix'] = to_comma_decimal(produit['prix'])
            produit['prixba'] = to_comma_decimal(produit['prixba'])
            cur.close()
            conn.close()
            logger.info(f"Produit trouvé (lié): codebar={codebar}, user_id={user_id}")
            return jsonify({
                'statut': 'trouvé',
                'type': 'lié',
                'produit': produit
            }), 200

        cur.close()
        conn.close()
        logger.info(f"Produit non trouvé: codebar={codebar}, user_id={user_id}")
        return jsonify({'erreur': 'Produit non trouvé'}), 404

    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Erreur recherche produit: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Ajouter code-barres lié
@app.route('/ajouter_codebar_lie', methods=['POST'])
def ajouter_codebar_lie():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('barcode')

    if not numero_item:
        logger.error("numero_item requis")
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
            logger.error(f"Produit non trouvé: numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Vérifier que bar2 n'existe pas déjà
        if bar2:
            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar2, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                logger.error(f"Code-barres lié existe déjà: bar2={bar2}, user_id={user_id}")
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
                logger.error(f"Code EAN-13 généré existe déjà: bar2={bar2}, user_id={user_id}")
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
        logger.info(f"Code-barres lié ajouté: id={codebar_id}, bar2={bar2}, user_id={user_id}")
        return jsonify({'statut': 'Code-barres lié ajouté', 'id': codebar_id, 'bar2': bar2}), 201
    except ValueError:
        conn.rollback()
        logger.error(f"numero_item invalide: {numero_item}")
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        logger.error(f"Erreur ajout code-barres lié: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Liste des codes-barres liés
@app.route('/liste_codebar_lies', methods=['GET'])
def liste_codebar_lies():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    numero_item = request.args.get('numero_item')
    if not numero_item:
        logger.error("numero_item requis")
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
            logger.error(f"Produit non trouvé: numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Récupérer les codes-barres liés
        cur.execute("SELECT bar2 FROM codebar WHERE bar::INTEGER = %s AND user_id = %s ORDER BY n", (numero_item, user_id))
        linked_barcodes = [row['bar2'] for row in cur.fetchall()]

        cur.close()
        conn.close()
        logger.info(f"Codes-barres liés récupérés: numero_item={numero_item}, user_id={user_id}, {len(linked_barcodes)} codes")
        return jsonify({'linked_barcodes': linked_barcodes}), 200
    except ValueError:
        if conn:
            conn.close()
        logger.error(f"numero_item invalide: {numero_item}")
        return jsonify({'erreur': 'numero_item doit être un nombre valide'}), 400
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Erreur récupération codes-barres liés: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer code-barres lié
@app.route('/supprimer_codebar_lie', methods=['POST'])
def supprimer_codebar_lie():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('bar2')

    if not numero_item or not bar2:
        logger.error("numero_item et bar2 requis")
        return jsonify({'erreur': 'numero_item et bar2 sont requis'}), 400

    try:
        numero_item_str = str(numero_item)
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier que l'item existe
        cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (int(numero_item), user_id))
        item = cur.fetchone()
        if not item:
            cur.close()
            conn.close()
            logger.error(f"Produit non trouvé: numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        # Vérifier que le code-barres lié existe
        cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            logger.error(f"Code-barres lié non trouvé: bar2={bar2}, numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Code-barres lié non trouvé pour ce produit'}), 404

        # Supprimer le code-barres lié
        cur.execute("DELETE FROM codebar WHERE bar2 = %s AND bar = %s AND user_id = %s", (bar2, numero_item_str, user_id))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Code-barres lié supprimé: bar2={bar2}, numero_item={numero_item}, user_id={user_id}")
        return jsonify({'statut': 'Code-barres lié supprimé'}), 200
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        logger.error(f"Erreur suppression code-barres lié: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Liste des clients
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        clients = [
            {
                'numero_clt': row['numero_clt'],
                'nom': row['nom'],
                'solde': to_comma_decimal(row['solde']),
                'reference': row['reference'],
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]
        logger.info(f"Récupération de {len(clients)} clients pour user_id={user_id}")
        return jsonify(clients), 200
    except Exception as e:
        logger.error(f"Erreur récupération clients: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Ajouter client
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
        logger.error("Le champ nom est obligatoire")
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) FROM client WHERE user_id = %s", (user_id,))
        count = cur.fetchone()['count']
        reference = f"C{count + 1}"

        cur.execute(
            "INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_clt",
            (nom, '0,00', reference, contact, adresse, user_id)
        )
        client_id = cur.fetchone()['numero_clt']
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Client ajouté: id={client_id}, reference={reference}, user_id={user_id}")
        return jsonify({'statut': 'Client ajouté', 'id': client_id, 'reference': reference}), 201
    except Exception as e:
        logger.error(f"Erreur ajout client: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Modifier client
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
        logger.error("Le champ nom est obligatoire")
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "UPDATE client SET nom = %s, contact = %s, adresse = %s WHERE numero_clt = %s AND user_id = %s RETURNING numero_clt",
            (nom, contact, adresse, numero_clt, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Client non trouvé: numero_clt={numero_clt}, user_id={user_id}")
            return jsonify({'erreur': 'Client non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Client modifié: numero_clt={numero_clt}, user_id={user_id}")
        return jsonify({'statut': 'Client modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur modification client: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer client
@app.route('/supprimer_client/<numero_clt>', methods=['DELETE'])
def supprimer_client(numero_clt):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM client WHERE numero_clt = %s AND user_id = %s", (numero_clt, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Client non trouvé: numero_clt={numero_clt}, user_id={user_id}")
            return jsonify({'erreur': 'Client non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Client supprimé: numero_clt={numero_clt}, user_id={user_id}")
        return jsonify({'statut': 'Client supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur suppression client: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Liste des fournisseurs
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        fournisseurs = [
            {
                'numero_fou': row['numero_fou'],
                'nom': row['nom'],
                'solde': to_comma_decimal(row['solde']),
                'reference': row['reference'],
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]
        logger.info(f"Récupération de {len(fournisseurs)} fournisseurs pour user_id={user_id}")
        return jsonify(fournisseurs), 200
    except Exception as e:
        logger.error(f"Erreur récupération fournisseurs: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Modifier fournisseur
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
        logger.error("Le champ nom est obligatoire")
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "UPDATE fournisseur SET nom = %s, contact = %s, adresse = %s WHERE numero_fou = %s AND user_id = %s RETURNING numero_fou",
            (nom, contact, adresse, numero_fou, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Fournisseur non trouvé: numero_fou={numero_fou}, user_id={user_id}")
            return jsonify({'erreur': 'Fournisseur non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Fournisseur modifié: numero_fou={numero_fou}, user_id={user_id}")
        return jsonify({'statut': 'Fournisseur modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur modification fournisseur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Ajouter fournisseur
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
        logger.error("Le champ nom est obligatoire")
        return jsonify({'erreur': 'Le champ nom est obligatoire'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) FROM fournisseur WHERE user_id = %s", (user_id,))
        count = cur.fetchone()['count']
        reference = f"F{count + 1}"

        cur.execute(
            "INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_fou",
            (nom, '0,00', reference, contact, adresse, user_id)
        )
        fournisseur_id = cur.fetchone()['numero_fou']
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Fournisseur ajouté: id={fournisseur_id}, reference={reference}, user_id={user_id}")
        return jsonify({'statut': 'Fournisseur ajouté', 'id': fournisseur_id, 'reference': reference}), 201
    except Exception as e:
        logger.error(f"Erreur ajout fournisseur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer fournisseur
@app.route('/supprimer_fournisseur/<numero_fou>', methods=['DELETE'])
def supprimer_fournisseur(numero_fou):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_fou, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Fournisseur non trouvé: numero_fou={numero_fou}, user_id={user_id}")
            return jsonify({'erreur': 'Fournisseur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Fournisseur supprimé: numero_fou={numero_fou}, user_id={user_id}")
        return jsonify({'statut': 'Fournisseur supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur suppression fournisseur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Liste des produits
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_item, bar, designation, qte, prix, prixba, ref FROM item WHERE user_id = %s ORDER BY designation", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        produits = [
            {
                'NUMERO_ITEM': row['numero_item'],
                'BAR': row['bar'],
                'DESIGNATION': row['designation'],
                'QTE': row['qte'],
                'PRIX': to_comma_decimal(row['prix']),
                'PRIXBA': to_comma_decimal(row['prixba']),
                'REF': row['ref'] or ''
            }
            for row in rows
        ]
        logger.info(f"Récupération de {len(produits)} produits pour user_id={user_id}")
        return jsonify(produits), 200
    except Exception as e:
        logger.error(f"Erreur récupération produits: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Modifier produit
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
        logger.error("Champs obligatoires manquants (designation, bar, prix, qte)")
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, bar, prix, qte)'}), 400

    try:
        prix = to_dot_decimal(prix)
        qte = int(qte)
        prixba = to_dot_decimal(prixba) if prixba is not None else 0.0
        if prix < 0 or qte < 0:
            logger.error("Le prix et la quantité doivent être positifs")
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s AND numero_item != %s", (bar, user_id, numero_item))
        if cur.fetchone():
            cur.close()
            conn.close()
            logger.error(f"Code-barres déjà utilisé: bar={bar}, user_id={user_id}")
            return jsonify({'erreur': 'Ce code-barres est déjà utilisé'}), 409

        cur.execute(
            "UPDATE item SET designation = %s, bar = %s, prix = %s, qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s RETURNING numero_item",
            (designation, bar, to_comma_decimal(prix), qte, to_comma_decimal(prixba), numero_item, user_id)
        )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Produit non trouvé: numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Produit modifié: numero_item={numero_item}, user_id={user_id}")
        return jsonify({'statut': 'Produit modifié'}), 200
    except ValueError:
        logger.error("Le prix et la quantité doivent être des nombres valides")
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        logger.error(f"Erreur modification produit: {str(e)}")
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

# Endpoint: Ajouter produit
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
        logger.error("Champs obligatoires manquants (designation, prix, qte)")
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, prix, qte)'}), 400

    try:
        prix = to_dot_decimal(prix)
        qte = int(qte)
        prixba = to_dot_decimal(prixba) if prixba is not None else 0.0
        if prix < 0 or qte < 0:
            logger.error("Le prix et la quantité doivent être positifs")
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("LOCK TABLE item IN EXCLUSIVE MODE")

        if bar:
            cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                logger.error(f"Code-barres existe déjà: bar={bar}, user_id={user_id}")
                return jsonify({'erreur': 'Ce code-barres existe déjà pour cet utilisateur'}), 409

            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                logger.error(f"Code-barres existe comme lié: bar={bar}, user_id={user_id}")
                return jsonify({'erreur': 'Ce code-barres existe déjà comme code-barres lié pour cet utilisateur'}), 409

        cur.execute("SELECT ref, bar FROM item WHERE user_id = %s ORDER BY ref", (user_id,))
        existing_items = cur.fetchall()
        used_numbers = []
        for item in existing_items:
            ref_num = int(item['ref'][1:]) if item['ref'].startswith('P') and item['ref'][1:].isdigit() else 0
            bar_num = int(item['bar'][1:12]) if item['bar'].startswith('1') and len(item['bar']) == 13 and item['bar'][1:12].isdigit() else 0
            used_numbers.append(max(ref_num, bar_num))

        next_number = 1
        used_numbers = sorted(set(used_numbers))
        for num in used_numbers:
            if num == next_number:
                next_number += 1
            elif num > next_number:
                break

        ref = f"P{next_number}"

        temp_bar = bar if bar else 'TEMP_BAR'
        cur.execute(
            "INSERT INTO item (designation, bar, prix, qte, prixba, ref, user_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING numero_item",
            (designation, temp_bar, to_comma_decimal(prix), qte, to_comma_decimal(prixba), ref, user_id)
        )
        item_id = cur.fetchone()['numero_item']

        if not bar:
            code12 = f"1{next_number:011d}"
            check_digit = calculate_ean13_check_digit(code12)
            bar = f"{code12}{check_digit}"

            cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s AND numero_item != %s", 
                       (bar, user_id, item_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                logger.error(f"Code EAN-13 généré existe déjà: bar={bar}, user_id={user_id}")
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà pour cet utilisateur'}), 409

            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                logger.error(f"Code EAN-13 généré existe comme lié: bar={bar}, user_id={user_id}")
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà comme code-barres lié pour cet utilisateur'}), 409

            cur.execute(
                "UPDATE item SET bar = %s WHERE numero_item = %s AND user_id = %s",
                (bar, item_id, user_id)
            )

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Produit ajouté: id={item_id}, ref={ref}, bar={bar}, user_id={user_id}")
        return jsonify({'statut': 'Item ajouté', 'id': item_id, 'ref': ref, 'bar': bar}), 201
    except ValueError:
        conn.rollback()
        logger.error("Le prix et la quantité doivent être des nombres valides")
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        logger.error(f"Erreur ajout produit: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer produit
@app.route('/supprimer_item/<numero_item>', methods=['DELETE'])
def supprimer_item(numero_item):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Produit non trouvé: numero_item={numero_item}, user_id={user_id}")
            return jsonify({'erreur': 'Produit non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Produit supprimé: numero_item={numero_item}, user_id={user_id}")
        return jsonify({'statut': 'Produit supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur suppression produit: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Valider vente
@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de vente invalides, utilisateur ou mot de passe manquant")
        return jsonify({"error": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')
    amount_paid = to_dot_decimal(data.get('amount_paid', '0,00'))
    lignes = data['lignes']
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    if payment_mode == 'a_terme' and numero_table == 0:
        logger.error("Vente à terme sans client sélectionné")
        return jsonify({"error": "Veuillez sélectionner un client pour une vente à terme"}), 400

    if payment_mode == 'a_terme' and amount_paid < 0:
        logger.error("Montant versé négatif")
        return jsonify({"error": "Le montant versé ne peut pas être négatif"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}, user_id={user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect: numero_util={numero_util}, user_id={user_id}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        cur.execute("SELECT COALESCE(MAX(compteur), 0) as max_compteur FROM comande WHERE nature = %s AND user_id = %s", (nature, user_id))
        compteur = cur.fetchone()['max_compteur'] + 1

        cur.execute("""
            INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id, numero_util)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_comande
        """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id, numero_util))
        numero_comande = cur.fetchone()['numero_comande']

        total_sale = 0.0
        for ligne in lignes:
            quantite = to_dot_decimal(ligne.get('quantite', '1'))
            prixt = to_dot_decimal(ligne.get('prixt', '0,00'))
            prixba = to_dot_decimal(ligne.get('prixbh', '0,00'))
            remarque = ligne.get('remarque', '')
            if isinstance(remarque, (int, float)):
                remarque = to_comma_decimal(remarque)

            cur.execute("""
                INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, numero_comande, ligne.get('numero_item'), quantite, to_comma_decimal(prixt), remarque, to_comma_decimal(prixba), 0))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s", (quantite, ligne.get('numero_item'), user_id))
            total_sale += quantite * prixt

        if payment_mode == 'a_terme' and numero_table != 0:
            solde_change = amount_paid - total_sale
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_table, user_id))
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client non trouvé: numero_clt={numero_table}, user_id={user_id}")

            current_solde = to_dot_decimal(client['solde'])
            new_solde = current_solde + solde_change
            new_solde_str = to_comma_decimal(new_solde)

            cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s AND user_id = %s", (new_solde_str, numero_table, user_id))
            logger.info(f"Solde client mis à jour: numero_clt={numero_table}, user_id={user_id}, solde_change={solde_change}, new_solde={new_solde_str}")

        conn.commit()
        logger.info(f"Vente validée: numero_comande={numero_comande}, user_id={user_id}, {len(lignes)} lignes")
        return jsonify({"numero_comande": numero_comande}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur validation vente: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Endpoint: Solde client
@app.route('/client_solde', methods=['GET'])
def client_solde():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_clt, COALESCE(solde, '0,00') as solde FROM client WHERE user_id = %s", (user_id,))
        soldes = [
            {
                'numero_clt': row['numero_clt'],
                'solde': to_comma_decimal(row['solde'])
            }
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
        logger.info(f"Soldes récupérés: {len(soldes)} clients, user_id={user_id}")
        return jsonify(soldes), 200
    except Exception as e:
        logger.error(f"Erreur récupération soldes: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Endpoint: Ventes du jour
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
                logger.error("Format de date invalide")
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
                a.prixt,
                a.remarque,
                i.designation
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt AND cl.user_id = %s
            LEFT JOIN utilisateur u ON c.numero_util = u.numero_util AND u.user_id = %s
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, user_id, user_id, user_id, user_id, date_start, date_end]

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
                'prixt': to_comma_decimal(row['prixt']),
                'remarque': to_comma_decimal(row['remarque']) if isinstance(row['remarque'], (int, float)) else row['remarque'] or ''
            })

            total += to_dot_decimal(row['prixt'])

        for vente in ventes_map.values():
            if vente['nature'] == 'TICKET':
                tickets.append(vente)
            elif vente['nature'] == 'BON DE L.':
                bons.append(vente)

        cur.close()
        conn.close()
        logger.info(f"Ventes récupérées: {len(ventes_map)} ventes, user_id={user_id}, total={total}")
        return jsonify({
            'tickets': tickets,
            'bons': bons,
            'total': to_comma_decimal(total)
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        logger.error(f"Erreur récupération ventes du jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Articles les plus vendus
@app.route('/articles_plus_vendus', methods=['GET'])
def articles_plus_vendus():
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
                logger.error("Format de date invalide")
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                i.numero_item,
                i.designation,
                SUM(a.quantite) AS quantite,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT)) AS total_vente
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, user_id, user_id, date_start, date_end]

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

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

        articles = [
            {
                'numero_item': row['numero_item'],
                'designation': row['designation'] or 'N/A',
                'quantite': int(row['quantite'] or 0),
                'total_vente': to_comma_decimal(row['total_vente'])
            }
            for row in rows
        ]

        cur.close()
        conn.close()
        logger.info(f"Articles les plus vendus récupérés: {len(articles)} articles, user_id={user_id}")
        return jsonify(articles), 200
    except Exception as e:
        logger.error(f"Erreur récupération articles plus vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Endpoint: Profit par date
@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util', '0')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

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

        query = """
            SELECT 
                DATE(c.date_comande) AS date,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0,00') AS FLOAT))) AS profit
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, user_id, user_id, date_start, date_end]

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        if numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(int(numero_util))

        query += """
            GROUP BY DATE(c.date_comande)
            ORDER BY DATE(c.date_comande) DESC
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        profits = [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'profit': to_comma_decimal(row['profit'])
            }
            for row in rows
        ]

        cur.close()
        conn.close()
        logger.info(f"Profits par date récupérés: {len(profits)} jours, user_id={user_id}")
        return jsonify(profits), 200
    except Exception as e:
        logger.error(f"Erreur récupération profit par date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Endpoint: Tableau de bord
@app.route('/dashboard', methods=['GET'])
def dashboard():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    period = request.args.get('period', 'day')
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if period == 'week':
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query_kpi = """
            SELECT 
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT)), 0) AS total_ca,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0,00') AS FLOAT))), 0) AS total_profit,
                COUNT(DISTINCT c.numero_comande) AS sales_count
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
        """
        cur.execute(query_kpi, (user_id, user_id, user_id, date_start, date_end))
        kpi_data = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE user_id = %s AND qte < 10", (user_id,))
        low_stock_count = cur.fetchone()['low_stock']

        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT)), 0) AS client_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt AND cl.user_id = %s
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY cl.nom
            ORDER BY client_ca DESC
            LIMIT 1
        """
        cur.execute(query_top_client, (user_id, user_id, user_id, date_start, date_end))
        top_client = cur.fetchone()

        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0,00') AS FLOAT)), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        cur.execute(query_chart, (user_id, user_id, date_start, date_end))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

        chart_labels = []
        chart_values = []
        current_date = date_start
        while current_date <= date_end:
            chart_labels.append(current_date.strftime('%Y-%m-%d'))
            daily_ca = next((row['daily_ca'] for row in chart_data if row['sale_date'].strftime('%Y-%m-%d') == current_date.strftime('%Y-%m-%d')), 0)
            chart_values.append(to_comma_decimal(daily_ca))
            current_date += timedelta(days=1)

        logger.info(f"Tableau de bord récupéré: user_id={user_id}, period={period}")
        return jsonify({
            'total_ca': to_comma_decimal(kpi_data['total_ca']),
            'total_profit': to_comma_decimal(kpi_data['total_profit']),
            'sales_count': int(kpi_data['sales_count'] or 0),
            'low_stock_items': int(low_stock_count or 0),
            'top_client': {
                'name': top_client['nom'] if top_client else 'N/A',
                'ca': to_comma_decimal(top_client['client_ca']) if top_client else '0,00'
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
        logger.error(f"Erreur récupération KPI: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Liste des utilisateurs
@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_util, nom, statue FROM utilisateur WHERE user_id = %s ORDER BY nom", (user_id,))
        utilisateurs = [
            {
                'numero': row['numero_util'],
                'nom': row['nom'],
                'statut': row['statue']
            }
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
        logger.info(f"Récupération de {len(utilisateurs)} utilisateurs pour user_id={user_id}")
        return jsonify(utilisateurs), 200
    except Exception as e:
        logger.error(f"Erreur récupération utilisateurs: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Modifier utilisateur
@app.route('/modifier_utilisateur/<int:numero_util>', methods=['PUT'])
def modifier_utilisateur(numero_util):
    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')
    statue = data.get('statue')
    user_id = data.get('user_id')

    if not all([nom, statue, user_id]):
        logger.error("Champs obligatoires manquants (nom, statue, user_id)")
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        logger.error("Statue invalide")
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    x_user_id = request.headers.get('X-User-ID')
    if x_user_id and x_user_id != user_id:
        logger.error(f"user_id non autorisé: header={x_user_id}, payload={user_id}")
        return jsonify({'erreur': 'user_id non autorisé'}), 403

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if password2:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, password2 = %s, statue = %s, user_id = %s WHERE numero_util = %s AND user_id = %s",
                (nom, password2, statue, user_id, numero_util, user_id)
            )
        else:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, statue = %s, user_id = %s WHERE numero_util = %s AND user_id = %s",
                (nom, statue, user_id, numero_util, user_id)
            )
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}, user_id={user_id}")
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Utilisateur modifié: numero_util={numero_util}, user_id={user_id}")
        return jsonify({'statut': 'Utilisateur modifié'}), 200
    except Exception as e:
        logger.error(f"Erreur modification utilisateur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Ajouter utilisateur
@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')
    statue = data.get('statue')
    user_id = data.get('user_id')

    if not all([nom, password2, statue, user_id]):
        logger.error("Champs obligatoires manquants (nom, password2, statue, user_id)")
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password2, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        logger.error("Statue invalide")
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    x_user_id = request.headers.get('X-User-ID')
    if x_user_id and x_user_id != user_id:
        logger.error(f"user_id non autorisé: header={x_user_id}, payload={user_id}")
        return jsonify({'erreur': 'user_id non autorisé'}), 403

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "INSERT INTO utilisateur (nom, password2, statue, user_id) VALUES (%s, %s, %s, %s) RETURNING numero_util",
            (nom, password2, statue, user_id)
        )
        numero_util = cur.fetchone()['numero_util']
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Utilisateur ajouté: id={numero_util}, user_id={user_id}")
        return jsonify({'statut': 'Utilisateur ajouté', 'id': numero_util}), 201
    except Exception as e:
        logger.error(f"Erreur ajout utilisateur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer utilisateur
@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}, user_id={user_id}")
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Utilisateur supprimé: numero_util={numero_util}, user_id={user_id}")
        return jsonify({'statut': 'Utilisateur supprimé'}), 200
    except Exception as e:
        logger.error(f"Erreur suppression utilisateur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

@app.route('/stock_value', methods=['GET'])
def valeur_stock():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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

        valeur_achat = float(result['valeur_achat'] or 0) if result else 0.0
        valeur_vente = float(result['valeur_vente'] or 0) if result else 0.0
        zakat = valeur_vente * 0.025  # 2.5% de la valeur de vente

        logger.debug(f"Valeur stock calculée: user_id={user_id}, valeur_achat={valeur_achat}, valeur_vente={valeur_vente}, zakat={zakat}")
        return jsonify({
            'statut': 'Succès',
            'valeur_achat': f"{valeur_achat:.2f}",
            'valeur_vente': f"{valeur_vente:.2f}",
            'zakat': f"{zakat:.2f}"
        }), 200
    except Exception as e:
        logger.error(f"Erreur récupération valeur stock: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/valider_reception', methods=['POST'])
def valider_reception():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de réception invalides")
        return jsonify({"erreur": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

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
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur non trouvé: numero_four={numero_four}")
            return jsonify({"erreur": "Fournisseur non trouvé"}), 400

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
                logger.error(f"Quantité non positive: qtea={qtea}, numero_item={numero_item}")
                raise ValueError("La quantité ajoutée doit être positive")
            if prixbh < 0:
                logger.error(f"Prix d'achat négatif: prixbh={prixbh}, numero_item={numero_item}")
                raise ValueError("Le prix d'achat ne peut pas être négatif")

            # Vérifier l'article
            cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                logger.error(f"Article non trouvé: numero_item={numero_item}")
                raise ValueError(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            prixba = float(item['prixba'] or 0)
            nqte = current_qte + qtea
            total_cost += qtea * prixbh

            # Insérer les détails dans ATTACHE2
            cur.execute("""
                INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (numero_item, numero_mouvement, qtea, nqte, f"{prixbh:.2f}", f"{prixba:.2f}", True, user_id))

            # Mettre à jour le stock et le prix d'achat
            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (nqte, f"{prixbh:.2f}", numero_item, user_id))

        # Mettre à jour le solde du fournisseur
        current_solde = float(fournisseur['solde'] or 0.0)
        new_solde = current_solde - total_cost
        new_solde_str = f"{new_solde:.2f}"
        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={numero_four}, total_cost={total_cost}, new_solde={new_solde_str}")

        conn.commit()
        logger.info(f"Réception validée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"statut": "Réception validée", "numero_mouvement": numero_mouvement}), 200
    except ValueError as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur validation réception: {str(e)}")
        return jsonify({"erreur": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur validation réception: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/receptions_jour', methods=['GET'])
def receptions_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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
                logger.error(f"Format de date invalide: {selected_date}")
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
            try:
                params.append(int(numero_util))
                query += " AND m.numero_util = %s"
            except ValueError:
                logger.error(f"Numero_util invalide: {numero_util}")
                return jsonify({'erreur': 'Numéro utilisateur invalide'}), 400
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
                'nprix': f"{row['nprix']:.2f}",
                'total_ligne': f"{float(row['qtea']) * float(row['nprix']):.2f}"
            })

            total += float(row['qtea']) * float(row['nprix'])

        receptions = list(receptions_map.values())
        logger.info(f"Récupération de {len(receptions)} réceptions pour user_id={user_id}, date={selected_date or 'aujourd\'hui'}")
        return jsonify({
            'statut': 'Succès',
            'receptions': receptions,
            'total': f"{total:.2f}"
        }), 200
    except Exception as e:
        logger.error(f"Erreur récupération réceptions: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ajouter_versement', methods=['POST'])
def ajouter_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    data = request.get_json()
    if not data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de versement invalides")
        return jsonify({"erreur": "Type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        logger.error(f"Type invalide: {type_versement}")
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            logger.error("Montant zéro fourni")
            return jsonify({"erreur": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Vérifier client ou fournisseur
        if type_versement == 'C':
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:
            cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        if not entity:
            logger.error(f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé: numero_cf={numero_cf}")
            return jsonify({"erreur": f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Mettre à jour le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde + montant
        new_solde_str = f"{new_solde:.2f}"
        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Insérer le versement dans MOUVEMENTC
        now = datetime.utcnow()
        cur.execute(
            """
            INSERT INTO MOUVEMENTC (date_mc, time_mc, montant, justificatif, numero_util, origine, cf, numero_cf, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mc
            """,
            (now.date(), now, f"{montant:.2f}", justificatif, numero_util, origine, type_versement, numero_cf, user_id)
        )
        numero_mc = cur.fetchone()['numero_mc']

        conn.commit()
        logger.info(f"Versement ajouté: numero_mc={numero_mc}, type={type_versement}, montant={montant}, user_id={user_id}")
        return jsonify({"statut": "Versement ajouté", "numero_mc": numero_mc}), 201
    except ValueError:
        logger.error(f"Montant invalide: {montant}")
        return jsonify({"erreur": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur ajout versement: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/historique_versements', methods=['GET'])
def historique_versements():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    selected_date = request.args.get('date')
    type_versement = request.args.get('type')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                logger.error(f"Format de date invalide: {selected_date}")
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
                'montant': f"{float(row['montant']):.2f}",
                'justificatif': row['justificatif'] or '',
                'type': 'Client' if row['cf'] == 'C' else 'Fournisseur',
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        logger.info(f"Récupération de {len(versements)} versements pour user_id={user_id}, date={selected_date or 'aujourd\'hui'}")
        return jsonify({'statut': 'Succès', 'versements': versements}), 200
    except Exception as e:
        logger.error(f"Erreur récupération historique versements: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/annuler_versement', methods=['DELETE'])
def annuler_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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
        logger.error(f"Type invalide: {type_versement}")
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer le versement
        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')", 
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement non trouvé: numero_mc={numero_mc}")
            return jsonify({"erreur": "Versement non trouvé"}), 404

        montant = float(versement['montant'])

        # Déterminer la table et la colonne ID
        table = 'client' if versement['cf'] == 'C' else 'fournisseur'
        id_column = 'numero_clt' if versement['cf'] == 'C' else 'numero_fou'

        # Vérifier l'entité
        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé: numero_cf={numero_cf}")
            return jsonify({"erreur": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Restaurer le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - montant
        new_solde_str = f"{new_solde:.2f}"
        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Supprimer le versement
        cur.execute("DELETE FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s", (numero_mc, user_id))

        conn.commit()
        logger.info(f"Versement annulé: numero_mc={numero_mc}, type={type_versement}, montant={montant}, user_id={user_id}")
        return jsonify({"statut": "Versement annulé", "numero_mc": numero_mc}), 200
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation versement: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_versement', methods=['PUT'])
def modifier_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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
        logger.error(f"Type invalide: {type_versement}")
        return jsonify({"erreur": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            logger.error("Montant zéro fourni")
            return jsonify({"erreur": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer le versement existant
        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F')", 
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement non trouvé: numero_mc={numero_mc}")
            return jsonify({"erreur": "Versement non trouvé"}), 404

        old_montant = float(versement['montant'])
        table = 'client' if versement['cf'] == 'C' else 'fournisseur'
        id_column = 'numero_clt' if versement['cf'] == 'C' else 'numero_fou'
        origine = 'VERSEMENT C' if versement['cf'] == 'C' else 'VERSEMENT F'

        # Vérifier l'entité
        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé: numero_cf={numero_cf}")
            return jsonify({"erreur": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Ajuster le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde - old_montant + montant
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
        logger.info(f"Versement modifié: numero_mc={numero_mc}, type={type_versement}, montant={montant}, user_id={user_id}")
        return jsonify({"statut": "Versement modifié", "numero_mc": numero_mc}), 200
    except ValueError:
        logger.error(f"Montant invalide: {montant}")
        return jsonify({"erreur": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification versement: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/situation_versements', methods=['GET'])
def situation_versements():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    type_versement = request.args.get('type')
    numero_cf = request.args.get('numero_cf')

    if not type_versement or type_versement not in ['C', 'F']:
        logger.error(f"Type manquant ou invalide: {type_versement}")
        return jsonify({'erreur': "Paramètre 'type' requis et doit être 'C' ou 'F'"}), 400
    if not numero_cf:
        logger.error("numero_cf manquant")
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
        cur.execute(query, (user_id, type_versement, numero_cf))
        rows = cur.fetchall()

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

        logger.info(f"Situation versements: type={type_versement}, numero_cf={numero_cf}, {len(versements)} versements, user_id={user_id}")
        return jsonify({'statut': 'Succès', 'versements': versements}), 200
    except Exception as e:
        logger.error(f"Erreur récupération situation versements: {str(e)}", exc_info=True)
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/annuler_vente', methods=['POST'])
def annuler_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    data = request.get_json()
    if not data or 'numero_comande' not in data or 'password2' not in data:
        logger.error("Données d'annulation vente invalides")
        return jsonify({"erreur": "Numéro de commande ou mot de passe manquant"}), 400

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
            logger.error(f"Commande non trouvée: numero_comande={numero_comande}")
            return jsonify({"erreur": "Commande non trouvée"}), 404

        # Vérifier le mot de passe
        if commande['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour annuler la commande {numero_comande}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la vente
        cur.execute("""
            SELECT numero_item, quantite, prixt
            FROM attache 
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_comande, user_id))
        lignes = cur.fetchall()

        if not lignes:
            logger.error(f"Aucune ligne trouvée pour la commande {numero_comande}")
            return jsonify({"erreur": "Aucune ligne de vente trouvée"}), 404

        # Restaurer le stock dans item
        for ligne in lignes:
            quantite = float(ligne['quantite'])
            if quantite <= 0:
                logger.error(f"Quantité non positive dans attache: numero_item={ligne['numero_item']}, quantite={quantite}")
                raise ValueError("Quantité non positive dans les lignes de vente")
            cur.execute("""
                UPDATE item 
                SET qte = qte + %s 
                WHERE numero_item = %s AND user_id = %s
            """, (quantite, ligne['numero_item'], user_id))

        # Si vente à terme (numero_table != 0), ajuster le solde du client
        if commande['numero_table'] != 0:
            total_sale = sum(float(ligne['prixt']) for ligne in lignes)
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                        (commande['numero_table'], user_id))
            client = cur.fetchone()
            if not client:
                logger.error(f"Client non trouvé: numero_clt={commande['numero_table']}")
                raise ValueError(f"Client {commande['numero_table']} non trouvé")

            current_solde = float(client['solde'] or '0.0')
            new_solde = current_solde - total_sale
            new_solde_str = f"{new_solde:.2f}"
            cur.execute("""
                UPDATE client 
                SET solde = %s 
                WHERE numero_clt = %s AND user_id = %s
            """, (new_solde_str, commande['numero_table'], user_id))
            logger.info(f"Solde client mis à jour: numero_clt={commande['numero_table']}, total_sale={total_sale}, new_solde={new_solde_str}")

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
        logger.info(f"Vente annulée: numero_comande={numero_comande}, {len(lignes)} lignes, user_id={user_id}")
        return jsonify({"statut": "Vente annulée", "numero_comande": numero_comande}), 200
    except ValueError as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation vente: {str(e)}")
        return jsonify({"erreur": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation vente: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/annuler_reception', methods=['POST'])
def annuler_reception():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    data = request.get_json()
    if not data or 'numero_mouvement' not in data or 'password2' not in data:
        logger.error("Données d'annulation réception invalides")
        return jsonify({"erreur": "Numéro de mouvement ou mot de passe manquant"}), 400

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
            logger.error(f"Mouvement non trouvé: numero_mouvement={numero_mouvement}")
            return jsonify({"erreur": "Mouvement non trouvé"}), 404

        # Vérifier le mot de passe
        if mouvement['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour annuler le mouvement {numero_mouvement}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la réception
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
        total_cost = sum(float(ligne['qtea']) * float(ligne['nprix']) for ligne in lignes)

        # Restaurer le stock dans item
        for ligne in lignes:
            qtea = float(ligne['qtea'])
            if qtea <= 0:
                logger.error(f"Quantité non positive dans attache2: numero_item={ligne['numero_item']}, qtea={qtea}")
                raise ValueError("Quantité non positive dans les lignes de réception")
            cur.execute("""
                UPDATE item 
                SET qte = qte - %s 
                WHERE numero_item = %s AND user_id = %s
            """, (qtea, ligne['numero_item'], user_id))

        # Mettre à jour le solde du fournisseur
        cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", 
                    (mouvement['numero_four'], user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur non trouvé: numero_fou={mouvement['numero_four']}")
            raise ValueError(f"Fournisseur {mouvement['numero_four']} non trouvé")

        current_solde = float(fournisseur['solde'] or '0.0')
        new_solde = current_solde + total_cost
        new_solde_str = f"{new_solde:.2f}"
        cur.execute("""
            UPDATE fournisseur 
            SET solde = %s 
            WHERE numero_fou = %s AND user_id = %s
        """, (new_solde_str, mouvement['numero_four'], user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={mouvement['numero_four']}, total_cost={total_cost}, new_solde={new_solde_str}")

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
        logger.info(f"Réception annulée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes, user_id={user_id}")
        return jsonify({"statut": "Réception annulée", "numero_mouvement": numero_mouvement}), 200
    except ValueError as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation réception: {str(e)}")
        return jsonify({"erreur": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation réception: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_vente/<int:numero_comande>', methods=['PUT'])
def modifier_vente(numero_comande):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        logger.error("Données de vente invalides")
        return jsonify({"erreur": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    lignes = data['lignes']
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
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
            logger.error(f"Utilisateur ou mot de passe incorrect: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur ou mot de passe incorrect"}), 401

        # Vérifier l'existence de la commande
        cur.execute("SELECT numero_table FROM comande WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))
        commande = cur.fetchone()
        if not commande:
            logger.error(f"Commande non trouvée: numero_comande={numero_comande}")
            return jsonify({"erreur": "Commande non trouvée"}), 404

        # Restaurer le stock des anciens articles
        cur.execute("SELECT numero_item, quantite, prixt FROM attache WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))
        old_lignes = cur.fetchall()
        old_total_sale = sum(float(ligne['prixt']) for ligne in old_lignes)
        for ligne in old_lignes:
            quantite = float(ligne['quantite'])
            if quantite <= 0:
                logger.error(f"Quantité non positive dans attache: numero_item={ligne['numero_item']}, quantite={quantite}")
                raise ValueError("Quantité non positive dans les lignes de vente")
            cur.execute("UPDATE item SET qte = qte + %s WHERE numero_item = %s AND user_id = %s",
                        (quantite, ligne['numero_item'], user_id))

        # Ajuster le solde du client si vente à terme
        if commande['numero_table'] != 0:
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                        (commande['numero_table'], user_id))
            client = cur.fetchone()
            if not client:
                logger.error(f"Client non trouvé: numero_clt={commande['numero_table']}")
                raise ValueError(f"Client {commande['numero_table']} non trouvé")
            current_solde = float(client['solde'] or '0.0')
            new_solde = current_solde - old_total_sale
            new_solde_str = f"{new_solde:.2f}"
            cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s AND user_id = %s",
                        (new_solde_str, commande['numero_table'], user_id))
            logger.info(f"Solde client restauré: numero_clt={commande['numero_table']}, old_total_sale={old_total_sale}, new_solde={new_solde_str}")

        # Supprimer les anciennes lignes
        cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", (numero_comande, user_id))

        # Insérer les nouvelles lignes et ajuster le stock
        new_total_sale = 0.0
        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            quantite = float(ligne.get('quantite', 0))
            prixt = float(ligne.get('prixt', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if quantite <= 0:
                logger.error(f"Quantité non positive: quantite={quantite}, numero_item={numero_item}")
                raise ValueError("La quantité doit être positive")
            if prixt < 0:
                logger.error(f"Prix de vente négatif: prixt={prixt}, numero_item={numero_item}")
                raise ValueError("Le prix de vente ne peut pas être négatif")

            cur.execute("SELECT qte FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                logger.error(f"Article non trouvé: numero_item={numero_item}")
                raise ValueError(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            if current_qte < quantite:
                logger.error(f"Stock insuffisant: numero_item={numero_item}, current_qte={current_qte}, quantite={quantite}")
                raise ValueError(f"Stock insuffisant pour l'article {numero_item}")

            new_total_sale += quantite * prixt
            cur.execute("""
                INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, numero_comande, numero_item, quantite, f"{prixt:.2f}", ligne.get('remarque', ''), f"{prixbh:.2f}", 0))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s",
                        (quantite, numero_item, user_id))

        # Mettre à jour le solde du client si nouvelle vente à terme
        if numero_table != 0:
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_table, user_id))
            client = cur.fetchone()
            if not client:
                logger.error(f"Nouveau client non trouvé: numero_clt={numero_table}")
                raise ValueError(f"Client {numero_table} non trouvé")
            current_solde = float(client['solde'] or '0.0')
            new_solde = current_solde + new_total_sale
            new_solde_str = f"{new_solde:.2f}"
            cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s AND user_id = %s",
                        (new_solde_str, numero_table, user_id))
            logger.info(f"Solde client mis à jour: numero_clt={numero_table}, new_total_sale={new_total_sale}, new_solde={new_solde_str}")

        # Mettre à jour la commande
        cur.execute("""
            UPDATE comande 
            SET numero_table = %s, date_comande = %s, nature = %s, numero_util = %s
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_table, date_comande, nature, numero_util, numero_comande, user_id))

        conn.commit()
        logger.info(f"Vente modifiée: numero_comande={numero_comande}, {len(lignes)} lignes, user_id={user_id}")
        return jsonify({"statut": "Vente modifiée", "numero_comande": numero_comande}), 200
    except ValueError as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification vente: {str(e)}")
        return jsonify({"erreur": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification vente: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/vente/<int:numero_comande>', methods=['GET'])
def get_vente(numero_comande):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
        return user_id

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
            logger.error(f"Commande non trouvée: numero_comande={numero_comande}")
            return jsonify({"erreur": "Commande non trouvée"}), 404

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
                    'prixt': f"{float(ligne['prixt']):.2f}",
                    'remarque': ligne['remarque'] or '',
                    'prixbh': f"{float(ligne['prixbh']):.2f}"
                }
                for ligne in lignes
            ]
        }

        logger.info(f"Vente récupérée: numero_comande={numero_comande}, user_id={user_id}")
        return jsonify({'statut': 'Succès', 'vente': response}), 200
    except Exception as e:
        logger.error(f"Erreur récupération vente: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/reception/<int:numero_mouvement>', methods=['GET'])
def get_reception(numero_mouvement):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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
            logger.error(f"Mouvement non trouvé: numero_mouvement={numero_mouvement}")
            return jsonify({"erreur": "Mouvement non trouvé"}), 404

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
                    'nprix': f"{float(ligne['nprix']):.2f}",
                    'nqte': ligne['nqte'],
                    'pump': f"{float(ligne['pump']):.2f}"
                }
                for ligne in lignes
            ]
        }

        logger.info(f"Réception récupérée: numero_mouvement={numero_mouvement}, user_id={user_id}")
        return jsonify({'statut': 'Succès', 'reception': response}), 200
    except Exception as e:
        logger.error(f"Erreur récupération réception: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_reception/<int:numero_mouvement>', methods=['PUT'])
def modifier_reception(numero_mouvement):
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        logger.error(f"Échec validation user_id: {user_id[0].get('erreur')}")
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
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur non trouvé: numero_util={numero_util}")
            return jsonify({"erreur": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"erreur": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("SELECT numero_fou, solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur non trouvé: numero_four={numero_four}")
            return jsonify({"erreur": "Fournisseur non trouvé"}), 400

        # Vérifier que la réception existe
        cur.execute("SELECT numero_mouvement, numero_four FROM mouvement WHERE numero_mouvement = %s AND user_id = %s AND nature = 'Bon de réception'", 
                    (numero_mouvement, user_id))
        mouvement = cur.fetchone()
        if not mouvement:
            logger.error(f"Réception non trouvée: numero_mouvement={numero_mouvement}")
            return jsonify({"erreur": "Réception non trouvée"}), 404

        # Récupérer les lignes précédentes de la réception
        cur.execute("""
            SELECT numero_item, qtea, nprix
            FROM attache2
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))
        old_lines = cur.fetchall()
        old_lines_dict = {line['numero_item']: line for line in old_lines}
        old_total_cost = sum(float(line['qtea']) * float(line['nprix']) for line in old_lines)
        logger.debug(f"Coût total réception précédente: {old_total_cost}, numero_mouvement={numero_mouvement}")

        # Restaurer le solde initial
        current_solde = float(fournisseur['solde'] or 0.0)
        restored_solde = current_solde + old_total_cost
        logger.debug(f"Solde restauré: {restored_solde}, numero_fou={numero_four}")

        # Calculer le nouveau coût total et préparer les mises à jour du stock
        new_total_cost = 0.0
        stock_updates = {}

        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            new_qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if new_qtea < 0:
                logger.error(f"Quantité négative: qtea={new_qtea}, numero_item={numero_item}")
                raise ValueError("La quantité ajoutée ne peut pas être négative")
            if prixbh < 0:
                logger.error(f"Prix d'achat négatif: prixbh={prixbh}, numero_item={numero_item}")
                raise ValueError("Le prix d'achat ne peut pas être négatif")

            # Vérifier l'article
            cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                logger.error(f"Article non trouvé: numero_item={numero_item}")
                raise ValueError(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            old_qtea = float(old_lines_dict.get(numero_item, {}).get('qtea', 0))
            new_total_cost += new_qtea * prixbh

            stock_updates[numero_item] = {
                'old_qtea': old_qtea,
                'new_qtea': new_qtea,
                'prixbh': prixbh,
                'current_qte': current_qte
            }

        # Traiter les articles supprimés
        for numero_item, old_line in old_lines_dict.items():
            if numero_item not in stock_updates:
                cur.execute("SELECT qte FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
                item = cur.fetchone()
                stock_updates[numero_item] = {
                    'old_qtea': float(old_line['qtea']),
                    'new_qtea': 0,
                    'prixbh': 0,
                    'current_qte': float(item['qte'] or 0)
                }

        # Mettre à jour le solde du fournisseur
        new_solde = restored_solde - new_total_cost
        new_solde_str = f"{new_solde:.2f}"
        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={numero_four}, new_total_cost={new_total_cost}, new_solde={new_solde_str}")

        # Supprimer les anciennes lignes
        cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", (numero_mouvement, user_id))

        # Insérer les nouvelles lignes et mettre à jour le stock
        for numero_item, update_info in stock_updates.items():
            old_qtea = update_info['old_qtea']
            new_qtea = update_info['new_qtea']
            prixbh = update_info['prixbh']
            current_qte = update_info['current_qte']

            restored_qte = current_qte - old_qtea
            new_qte = restored_qte + new_qtea

            if new_qte < 0:
                logger.error(f"Stock négatif: numero_item={numero_item}, new_qte={new_qte}")
                raise ValueError(f"Stock négatif pour l'article {numero_item}: {new_qte}")

            if new_qtea > 0:
                cur.execute("""
                    INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (numero_item, numero_mouvement, new_qtea, new_qte, f"{prixbh:.2f}", f"{prixbh:.2f}", True, user_id))

            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (new_qte, f"{prixbh:.2f}" if new_qtea > 0 else str(update_info.get('current_prixba', 0)), 
                         numero_item, user_id))
            logger.debug(f"Stock mis à jour: numero_item={numero_item}, old_qtea={old_qtea}, new_qtea={new_qtea}, new_qte={new_qte}")

        # Mettre à jour le mouvement
        cur.execute("""
            UPDATE mouvement 
            SET numero_four = %s, numero_util = %s, date_m = %s
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_four, numero_util, datetime.utcnow(), numero_mouvement, user_id))

        conn.commit()
        logger.info(f"Réception modifiée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes, user_id={user_id}")
        return jsonify({"statut": "Réception modifiée", "numero_mouvement": numero_mouvement}), 200
    except ValueError as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification réception: {str(e)}")
        return jsonify({"erreur": str(e)}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification réception: {str(e)}", exc_info=True)
        return jsonify({"erreur": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
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
