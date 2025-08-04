import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import logging

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://hicham558.github.io", "https://firepoz-s7tl.vercel.app"]}})
app.debug = True  # Activer le mode debug pour voir les erreurs

# Configurez le logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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


@app.route('/', methods=['GET'])
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}', 500

@app.route('/rechercher_produit_codebar', methods=['GET'])
def rechercher_produit_codebar():
    codebar = request.args.get('codebar')
    user_id = request.args.get('user_id')
    if not codebar or not user_id:
        return jsonify({'erreur': 'Code-barres et user_id requis'}), 400

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
            cur.close()
            conn.close()
            return jsonify({
                'statut': 'trouvé',
                'type': 'principal',
                'produit': produit
            }), 200

        # Rechercher dans codebar pour un code-barres lié
        cur.execute("""
            SELECT i.numero_item, i.bar, i.designation, i.prix, i.prixba, i.qte
            FROM codebar c
            JOIN item i ON c.bar = i.numero_item::varchar AND i.user_id = %s
            WHERE c.bar2 = %s AND c.user_id = %s
        """, (user_id, codebar, user_id))
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
    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('barcode')
    user_id = data.get('user_id')

    if not numero_item or not user_id:
        return jsonify({'erreur': 'numero_item et user_id sont requis'}), 400

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
                return jsonify({'erreur': 'Ce code-barres lié existe déjà'}), 409

        # Générer un bar2 si non fourni
        cur.execute("SELECT bar2 FROM codebar WHERE user_id = %s", (user_id,))
        existing_barcodes = cur.fetchall()
        used_numbers = []
        for code in existing_barcodes:
            bar_num = int(code['bar2'][1:12]) if code['bar2'].startswith('1') and len(code['bar2']) == 13 and code['bar2'][1:12].isdigit() else 0
            used_numbers.append(bar_num)

        next_number = 1000000000016  # Point de départ pour user_id
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
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà'}), 409

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
    numero_item = request.args.get('numero_item')
    user_id = request.args.get('user_id')
    if not numero_item or not user_id:
        return jsonify({'erreur': 'numero_item et user_id sont requis'}), 400

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

        # Récupérer les codes-barres liés
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
    data = request.get_json()
    numero_item = data.get('numero_item')
    bar2 = data.get('bar2')
    user_id = data.get('user_id')

    if not numero_item or not bar2 or not user_id:
        return jsonify({'erreur': 'numero_item, bar2 et user_id sont requis'}), 400

    try:
        numero_item_str = str(numero_item)
        conn = get_conn()
        conn.autocommit = False
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Vérifier que l'item existe
            cur.execute("SELECT 1 FROM item WHERE numero_item = %s AND user_id = %s", (int(numero_item), user_id))
            item = cur.fetchone()
            if not item:
                raise Exception('Produit non trouvé')

            # Vérifier que le code-barres lié existe
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

@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()

        clients = [
            {
                'numero_clt': row['numero_clt'],
                'nom': row['nom'] or '',
                'solde': to_comma_decimal(to_dot_decimal(row['solde'] or '0,00')),
                'reference': row['reference'] or '',
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]

        logger.info(f"Récupération de {len(clients)} clients pour user_id {user_id}")
        return jsonify(clients), 200

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des clients: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = data.get('user_id')

    if not nom or not user_id:
        return jsonify({'erreur': 'Le champ nom et user_id sont obligatoires'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Compter les clients pour ce user_id
        cur.execute("SELECT COUNT(*) FROM client WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        reference = f"C{count + 1}"

        cur.execute(
            "INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_clt",
            (nom, '0,00', reference, contact, adresse, user_id)
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
    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = data.get('user_id')

    if not nom or not user_id:
        return jsonify({'erreur': 'Le champ nom et user_id sont obligatoires'}), 400

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

@app.route('/supprimer_client/<numero_clt>', methods=['DELETE'])
def supprimer_client(numero_clt):
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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

@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()

        fournisseurs = [
            {
                'numero_fou': row['numero_fou'],
                'nom': row['nom'] or '',
                'solde': to_comma_decimal(to_dot_decimal(row['solde'] or '0,00')),
                'reference': row['reference'] or '',
                'contact': row['contact'] or '',
                'adresse': row['adresse'] or ''
            }
            for row in rows
        ]

        logger.info(f"Récupération de {len(fournisseurs)} fournisseurs pour user_id {user_id}")
        return jsonify(fournisseurs), 200

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des fournisseurs: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ajouter_fournisseur', methods=['POST'])
def ajouter_fournisseur():
    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = data.get('user_id')

    if not nom or not user_id:
        return jsonify({'erreur': 'Le champ nom et user_id sont obligatoires'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fournisseur WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        reference = f"F{count + 1}"

        cur.execute(
            "INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_fou",
            (nom, '0,00', reference, contact, adresse, user_id)
        )
        fournisseur_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur ajouté', 'id': fournisseur_id, 'reference': reference}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/modifier_fournisseur/<numero_fou>', methods=['PUT'])
def modifier_fournisseur(numero_fou):
    data = request.get_json()
    nom = data.get('nom')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = data.get('user_id')

    if not nom or not user_id:
        return jsonify({'erreur': 'Le champ nom et user_id sont obligatoires'}), 400

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

@app.route('/supprimer_fournisseur/<numero_fou>', methods=['DELETE'])
def supprimer_fournisseur(numero_fou):
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_fou, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Fournisseur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')
    user_id = data.get('user_id')

    if not all([designation, bar, prix is not None, qte is not None, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, bar, prix, qte, user_id)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM item WHERE bar = %s AND numero_item != %s AND user_id = %s", (bar, numero_item, user_id))
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

def calculate_ean13_check_digit(code12):
    digits = [int(d) for d in code12]
    odd_sum = sum(digits[0::2])
    even_sum = sum(digits[1::2])
    total = odd_sum * 3 + even_sum
    next_multiple_of_10 = (total + 9) // 10 * 10
    check_digit = next_multiple_of_10 - total
    return check_digit

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')
    user_id = data.get('user_id')

    if not all([designation, prix is not None, qte is not None, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, prix, qte, user_id)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
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
                return jsonify({'erreur': 'Ce code-barres existe déjà'}), 409

            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Ce code-barres existe déjà comme code-barres lié'}), 409

        cur.execute("SELECT ref, bar FROM item WHERE user_id = %s ORDER BY ref", (user_id,))
        existing_items = cur.fetchall()
        used_numbers = []
        for item in existing_items:
            ref_num = int(item['ref'][1:]) if item['ref'].startswith('P') and item['ref'][1:].isdigit() else 0
            bar_num = int(item['bar'][1:12]) if item['bar'].startswith('1') and len(item['bar']) == 13 and item['bar'][1:12].isdigit() else 0
            used_numbers.append(max(ref_num, bar_num))

        next_number = 1000000000016
        used_numbers = sorted(set(used_numbers))
        for num in used_numbers:
            if num == next_number:
                next_number += 1
            elif num > next_number:
                break

        ref = f"P{next_number - 1000000000000}"

        temp_bar = bar if bar else 'TEMP_BAR'
        cur.execute(
            "INSERT INTO item (designation, bar, prix, qte, prixba, ref, user_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING numero_item",
            (designation, temp_bar, prix, qte, prixba or '0.00', ref, user_id)
        )
        item_id = cur.fetchone()['numero_item']

        if not bar:
            code12 = f"1{next_number:011d}"
            check_digit = calculate_ean13_check_digit(code12)
            bar = f"{code12}{check_digit}"

            cur.execute("SELECT 1 FROM item WHERE bar = %s AND numero_item != %s AND user_id = %s", (bar, item_id, user_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà'}), 409

            cur.execute("SELECT 1 FROM codebar WHERE bar2 = %s AND user_id = %s", (bar, user_id))
            if cur.fetchone():
                conn.rollback()
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Le code EAN-13 généré existe déjà comme code-barres lié'}), 409

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

@app.route('/supprimer_item/<numero_item>', methods=['DELETE'])
def supprimer_item(numero_item):
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
    conn = None
    cur = None
    
    try:
        data = request.get_json()
        if not data or not all(k in data for k in ['lignes', 'numero_util', 'password2', 'user_id']):
            logger.error("Données manquantes")
            return jsonify({"error": "Données manquantes (lignes, numero_util, password2, user_id)"}), 400

        numero_table = int(data.get('numero_table', 0))
        payment_mode = data.get('payment_mode', 'espece')
        amount_paid = to_dot_decimal(data.get('amount_paid', '0,00'))
        amount_paid_str = to_comma_decimal(amount_paid)
        numero_util = data['numero_util']
        password2 = data['password2']
        user_id = data['user_id']
        nature = "TICKET" if numero_table == 0 else "BON DE L."

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        conn.autocommit = False

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        user = cur.fetchone()
        if not user or user['password2'] != password2:
            logger.error("Authentification invalide")
            return jsonify({"error": "Authentification invalide"}), 401

        cur.execute("""
            INSERT INTO comande (
                numero_table, date_comande, etat_c, nature, 
                connection1, compteur, numero_util, user_id
            ) VALUES (
                %s, NOW(), 'Cloture', %s, 
                -1, 
                (SELECT COALESCE(MAX(compteur),0)+1 FROM comande WHERE nature = %s AND user_id = %s), 
                %s, %s
            ) RETURNING numero_comande
        """, (numero_table, nature, nature, user_id, numero_util, user_id))
        numero_comande = cur.fetchone()['numero_comande']

        total_vente = 0.0
        for ligne in data['lignes']:
            quantite = to_dot_decimal(ligne.get('quantite', '1'))
            remarque = to_dot_decimal(ligne.get('remarque', '0,00'))
            prixt = to_dot_decimal(ligne.get('prixt', '0,00'))
            total_vente += quantite * remarque

            prixt_str = to_comma_decimal(prixt)
            prixbh_str = to_comma_decimal(to_dot_decimal(ligne.get('prixbh', '0,00')))

            remarque_str = ligne.get('remarque', '')
            if isinstance(remarque_str, (int, float)):
                remarque_str = to_comma_decimal(remarque_str)
            elif isinstance(remarque_str, str) and any(c.isdigit() for c in remarque_str):
                try:
                    if '.' in remarque_str or ',' in remarque_str:
                        remarque_str = to_comma_decimal(to_dot_decimal(remarque_str))
                except ValueError:
                    pass

            cur.execute("""
                INSERT INTO attache (
                    numero_comande, numero_item, quantite, prixt,
                    remarque, prixbh, achatfx, send, user_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                numero_comande,
                ligne['numero_item'],
                quantite,
                prixt_str,
                remarque_str,
                prixbh_str,
                0,
                True,
                user_id
            ))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s", 
                        (quantite, ligne['numero_item'], user_id))

        total_vente_str = to_comma_decimal(total_vente)
        montant_reglement = total_vente if payment_mode == 'espece' else amount_paid
        montant_reglement_str = to_comma_decimal(montant_reglement)
        solde_restant = total_vente - amount_paid if payment_mode == 'a_terme' else 0.0
        solde_restant_str = to_comma_decimal(solde_restant)

        now = datetime.now()
        cur.execute("""
            INSERT INTO encaisse (
                apaye, reglement, tva, ht, 
                numero_comande, origine, time_enc, soldeR, user_id
            ) VALUES (
                %s, %s, %s, %s, 
                %s, %s, %s, %s, %s
            )
        """, (
            total_vente_str,
            montant_reglement_str,
            '0,00',
            total_vente_str,
            numero_comande,
            nature,
            now,
            solde_restant_str,
            user_id
        ))

        if payment_mode == 'a_terme' and numero_table != 0:
            cur.execute("""
                UPDATE client 
                SET solde = to_char((CAST(REPLACE(solde, ',', '.') AS NUMERIC) + %s), 'FM999999999.99')
                WHERE numero_clt = %s AND user_id = %s
            """, (solde_restant, numero_table, user_id))

        conn.commit()
        return jsonify({
            "success": True,
            "numero_comande": numero_comande,
            "total_vente": total_vente_str,
            "montant_verse": amount_paid_str,
            "reglement": montant_reglement_str,
            "solde_restant": solde_restant_str if payment_mode == 'a_terme' else "0,00"
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur: {str(e)}")
        return jsonify({
            "error": "Erreur de traitement",
            "details": str(e)
        }), 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.route('/client_solde', methods=['GET'])
def client_solde():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_clt, COALESCE(solde, '0.00') as solde FROM client WHERE user_id = %s", (user_id,))
        soldes = cur.fetchall()
        logger.info(f"Soldes récupérés: {len(soldes)} clients pour user_id {user_id}")
        return jsonify(soldes), 200
    except Exception as e:
        logger.error(f"Erreur récupération soldes: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')
    user_id = request.args.get('user_id')
    
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
                CAST(COALESCE(NULLIF(REPLACE(a.prixt, ',', '.'), ''), '0') AS FLOAT) AS prixt,
                a.remarque,
                i.designation
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt AND cl.user_id = %s
            LEFT JOIN utilisateur u ON c.numero_util = u.numero_util AND u.user_id = %s
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
        """
        params = [user_id, user_id, user_id, user_id, date_start, date_end, user_id]
        
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
                'prixt': str(row['prixt']),
                'remarque': row['remarque'] or ''
            })
            
            total += float(row['prixt'])
        
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
        logger.error(f"Erreur récupération ventes du jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
        
       
@app.route('/articles_plus_vendus', methods=['GET'])
def articles_plus_vendus():
    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400
    
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
                i.numero_item,
                i.designation,
                SUM(a.quantite) AS quantite,
                SUM(CAST(COALESCE(NULLIF(REPLACE(a.prixt, ',', '.'), ''), '0') AS FLOAT)) AS total_vente
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
        """
        params = [user_id, user_id, date_start, date_end, user_id]
        
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
                'total_vente': to_comma_decimal(float(row['total_vente'] or 0))
            }
            for row in rows
        ]
        
        return jsonify(articles), 200
    
    except Exception as e:
        logger.error(f"Erreur récupération articles plus vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util', '0')
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400
    
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
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = date_end - timedelta(days=30)
        
        query = """
            SELECT
                DATE(c.date_comande) AS date,
                SUM(CAST(COALESCE(NULLIF(REPLACE(a.prixt, ',', '.'), ''), '0') AS FLOAT) -
                    (a.quantite * CAST(COALESCE(NULLIF(REPLACE(i.prixba, ',', '.'), ''), '0') AS FLOAT))) AS profit
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
        """
        params = [user_id, user_id, date_start, date_end, user_id]
        
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
                'profit': to_comma_decimal(float(row['profit'] or 0))
            }
            for row in rows
        ]
        
        return jsonify(profits), 200
    
    except Exception as e:
        logger.error(f"Erreur récupération profit par date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/dashboard', methods=['GET'])
def dashboard():
    period = request.args.get('period', 'day')
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
                COALESCE(SUM(CAST(REPLACE(COALESCE(NULLIF(a.prixt, ''), '0'), ',', '.') AS NUMERIC)), 0) AS total_ca,
                COALESCE(SUM(
                    CAST(REPLACE(COALESCE(NULLIF(a.prixt, ''), '0'), ',', '.') AS NUMERIC) - 
                    (a.quantite * CAST(REPLACE(COALESCE(NULLIF(i.prixba, ''), '0'), ',', '.') AS NUMERIC))
                ), 0) AS total_profit,
                COUNT(DISTINCT c.numero_comande) AS sales_count
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            JOIN item i ON a.numero_item = i.numero_item AND i.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
        """
        cur.execute(query_kpi, (user_id, user_id, date_start, date_end, user_id))
        kpi_data = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE qte < 10 AND user_id = %s", (user_id,))
        low_stock_count = cur.fetchone()['low_stock']

        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(CAST(REPLACE(COALESCE(NULLIF(a.prixt, ''), '0'), ',', '.') AS NUMERIC)), 0) AS client_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt AND cl.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
            GROUP BY cl.nom
            ORDER BY client_ca DESC
            LIMIT 1
        """
        cur.execute(query_top_client, (user_id, user_id, date_start, date_end, user_id))
        top_client = cur.fetchone()

        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(CAST(REPLACE(COALESCE(NULLIF(a.prixt, ''), '0'), ',', '.') AS NUMERIC)), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande AND a.user_id = %s
            WHERE c.date_comande >= %s
            AND c.date_comande <= %s
            AND c.user_id = %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        cur.execute(query_chart, (user_id, date_start, date_end, user_id))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

        chart_labels = []
        chart_values = []
        current_date = date_start
        while current_date <= date_end:
            chart_labels.append(current_date.strftime('%Y-%m-%d'))
            daily_ca = next((to_dot_decimal(row['daily_ca']) for row in chart_data 
                           if row['sale_date'].strftime('%Y-%m-%d') == current_date.strftime('%Y-%m-%d')), 0)
            chart_values.append(daily_ca)
            current_date += timedelta(days=1)

        return jsonify({
            'total_ca': to_comma_decimal(float(kpi_data['total_ca'] or 0)),
            'total_profit': to_comma_decimal(float(kpi_data['total_profit'] or 0)),
            'sales_count': int(kpi_data['sales_count'] or 0),
            'low_stock_items': int(low_stock_count or 0),
            'top_client': {
                'name': top_client['nom'] if top_client else 'N/A',
                'ca': to_comma_decimal(float(top_client['client_ca'] or 0)) if top_client else '0,00'
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

@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_util, nom, statue FROM utilisateur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        utilisateurs = [
            {
                'numero': row[0],
                'nom': row[1],
                'statut': row[2]
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
    password2 = data.get('password2')
    statue = data.get('statue')
    user_id = data.get('user_id')

    if not all([nom, statue, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        if password2:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, password2 = %s, statue = %s WHERE numero_util = %s AND user_id = %s",
                (nom, password2, statue, numero_util, user_id)
            )
        else:
            cur.execute(
                "UPDATE utilisateur SET nom = %s, statue = %s WHERE numero_util = %s AND user_id = %s",
                (nom, statue, numero_util, user_id)
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
    user_id = data.get('user_id')

    if not all([nom, password2, statue, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password2, statue, user_id)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

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

@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
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
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                SUM(COALESCE(CAST(NULLIF(REPLACE(prixba, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_achat,
                SUM(COALESCE(CAST(NULLIF(REPLACE(prix, ',', '.'), '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_vente
            FROM item
            WHERE qte >= 0 AND GERE=TRUE AND user_id = %s
        """, (user_id,))
        result = cur.fetchone()

        valeur_achat = to_dot_decimal(result['valeur_achat'] or '0,00')
        valeur_vente = to_dot_decimal(result['valeur_vente'] or '0,00')
        zakat = valeur_vente * 0.025

        response = {
            'valeur_achat': to_comma_decimal(valeur_achat),
            'valeur_vente': to_comma_decimal(valeur_vente),
            'zakat': to_comma_decimal(zakat)
        }

        logger.info(f"Valeur stock calculée pour user_id {user_id}: valeur_achat={to_comma_decimal(valeur_achat)}, valeur_vente={to_comma_decimal(valeur_vente)}, zakat={to_comma_decimal(zakat)}")
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur récupération valeur stock: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/valider_reception', methods=['POST'])
def valider_reception():
    data = request.get_json()
    if not data or not all(k in data for k in ['lignes', 'numero_four', 'numero_util', 'password2', 'user_id']):
        logger.error("Données de réception invalides")
        return jsonify({"error": "Données de réception invalides, fournisseur, utilisateur, mot de passe ou user_id manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')
    lignes = data['lignes']
    nature = "Bon de réception"

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        cur.execute("SELECT numero_fou FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        if not cur.fetchone():
            logger.error(f"Fournisseur {numero_four} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Fournisseur non trouvé"}), 400

        cur.execute("""
            INSERT INTO mouvement (date_m, etat_m, numero_four, refdoc, vers, nature, connection1, numero_util, cheque, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mouvement
        """, (datetime.utcnow(), "cloture", numero_four, "", "", nature, 0, numero_util, "", user_id))
        numero_mouvement = cur.fetchone()['numero_mouvement']

        cur.execute("UPDATE mouvement SET refdoc = %s WHERE numero_mouvement = %s AND user_id = %s", 
                    (str(numero_mouvement), numero_mouvement, user_id))

        total_cost = 0.0
        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            qtea = to_dot_decimal(ligne.get('qtea', '0'))
            prixbh = to_dot_decimal(ligne.get('prixbh', '0'))

            if qtea <= 0:
                raise Exception("La quantité ajoutée doit être positive")

            cur.execute("""
                SELECT qte, CAST(COALESCE(NULLIF(REPLACE(prixba, ',', '.'), ''), '0') AS FLOAT) AS prixba 
                FROM item WHERE numero_item = %s AND user_id = %s
            """, (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            prixba = float(item['prixba'] or 0)

            nqte = current_qte + qtea
            total_cost += qtea * prixbh

            prixbh_str = to_comma_decimal(prixbh)
            prixba_str = to_comma_decimal(prixba)

            cur.execute("""
                INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (numero_item, numero_mouvement, qtea, nqte, prixbh_str, prixba_str, True, user_id))

            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (nqte, prixbh_str, numero_item, user_id))

        cur.execute("""
            SELECT CAST(COALESCE(NULLIF(REPLACE(solde, ',', '.'), ''), '0') AS FLOAT) AS solde 
            FROM fournisseur WHERE numero_fou = %s AND user_id = %s
        """, (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            raise Exception(f"Fournisseur {numero_four} non trouvé")

        current_solde = float(fournisseur['solde'] or 0)
        new_solde = current_solde - total_cost
        new_solde_str = to_comma_decimal(new_solde)

        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={numero_four}, total_cost={to_comma_decimal(total_cost)}, new_solde={new_solde_str}")

        conn.commit()
        logger.info(f"Réception validée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"numero_mouvement": numero_mouvement}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur validation réception: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/receptions_jour', methods=['GET'])
def receptions_jour():
    selected_date = request.args.get('date')
    numero_util = request.args.get('numero_util')
    numero_four = request.args.get('numero_four', '')
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
                CAST(COALESCE(NULLIF(REPLACE(a2.nprix, ',', '.'), ''), '0') AS FLOAT) AS nprix,
                i.designation
            FROM mouvement m
            LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou AND f.user_id = %s
            LEFT JOIN utilisateur u ON m.numero_util = u.numero_util AND u.user_id = %s
            JOIN attache2 a2 ON m.numero_mouvement = a2.numero_mouvement AND a2.user_id = %s
            JOIN item i ON a2.numero_item = i.numero_item AND i.user_id = %s
            WHERE m.date_m >= %s 
            AND m.date_m <= %s
            AND m.nature = 'Bon de réception'
            AND m.user_id = %s
        """
        params = [user_id, user_id, user_id, user_id, date_start, date_end, user_id]

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
            nprix = float(row['nprix'])
            total_ligne = float(row['qtea']) * nprix

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
                'nprix': to_comma_decimal(nprix),
                'total_ligne': to_comma_decimal(total_ligne)
            })

            total += total_ligne

        receptions = list(receptions_map.values())

        cur.close()
        conn.close()

        return jsonify({
            'receptions': receptions,
            'total': to_comma_decimal(total)
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        logger.error(f"Erreur récupération réceptions: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_versement', methods=['POST'])
def ajouter_versement():
    data = request.get_json()
    required_fields = ['type', 'numero_cf', 'montant', 'numero_util', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error("Données de versement invalides")
        return jsonify({"error": "Type, numéro client/fournisseur, montant, utilisateur, mot de passe ou user_id manquant"}), 400

    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = to_dot_decimal(montant)
        if montant == 0:
            return jsonify({"error": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

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
            logger.error(f"{'Client' if type_versement == 'C' else 'Fournisseur'} {numero_cf} non trouvé pour user_id {user_id}")
            return jsonify({"error": f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé"}), 400

        current_solde = to_dot_decimal(entity['solde'] or '0,00')
        new_solde = current_solde + montant
        new_solde_str = to_comma_decimal(new_solde)

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        now = datetime.utcnow()
        cur.execute(
            """
            INSERT INTO MOUVEMENTC (date_mc, time_mc, montant, justificatif, numero_util, origine, cf, numero_cf, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mc
            """,
            (now.date(), now, to_comma_decimal(montant), justificatif,
             numero_util, origine, type_versement, numero_cf, user_id)
        )
        numero_mc = cur.fetchone()['numero_mc']

        conn.commit()
        logger.info(f"Versement ajouté: numero_mc={numero_mc}, type={type_versement}, montant={to_comma_decimal(montant)}")
        return jsonify({"numero_mc": numero_mc, "statut": "Versement ajouté"}), 201

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur ajout versement: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/historique_versements', methods=['GET'])
def historique_versements():
    selected_date = request.args.get('date')
    type_versement = request.args.get('type')
    user_id = request.args.get('user_id')
    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400

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
            WHERE mc.date_mc >= %s
            AND mc.date_mc <= %s
            AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
            AND mc.user_id = %s
        """
        params = [user_id, user_id, user_id, date_start, date_end, user_id]

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
                'montant': to_comma_decimal(to_dot_decimal(row['montant'])),
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
        logger.error(f"Erreur récupération historique versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

@app.route('/annuler_versement', methods=['DELETE'])
def annuler_versement():
    data = request.get_json()
    required_fields = ['numero_mc', 'type', 'numero_cf', 'numero_util', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error("Données d'annulation invalides")
        return jsonify({"error": "Numéro de versement, type, numéro client/fournisseur, utilisateur, mot de passe ou user_id manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F') AND user_id = %s",
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement {numero_mc} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Versement non trouvé"}), 404

        if type_versement != versement['cf']:
            logger.error(f"Type {type_versement} ne correspond pas au versement {numero_mc}")
            return jsonify({"error": "Type ne correspond pas au versement"}), 400

        if versement['cf'] == 'C':
            table = 'client'
            id_column = 'numero_clt'
        else:
            table = 'fournisseur'
            id_column = 'numero_fou'

        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé pour user_id {user_id}")
            return jsonify({"error": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        montant = to_dot_decimal(versement['montant'])
        current_solde = to_dot_decimal(entity['solde'] or '0,00')
        new_solde = current_solde - montant
        new_solde_str = to_comma_decimal(new_solde)

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        cur.execute("DELETE FROM MOUVEMENTC WHERE numero_mc = %s AND user_id = %s", (numero_mc, user_id))
        if cur.rowcount == 0:
            conn.rollback()
            logger.error(f"Versement {numero_mc} non supprimé")
            return jsonify({"error": "Versement non supprimé"}), 500

        conn.commit()
        logger.info(f"Versement annulé: numero_mc={numero_mc}, type={type_versement}, montant={to_comma_decimal(montant)}")
        return jsonify({"statut": "Versement annulé", "numero_mc": numero_mc}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation versement: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/modifier_versement', methods=['PUT'])
def modifier_versement():
    data = request.get_json()
    required_fields = ['numero_mc', 'type', 'numero_cf', 'montant', 'numero_util', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error("Données de modification invalides")
        return jsonify({"error": "Numéro de versement, type, numéro client/fournisseur, montant, utilisateur, mot de passe ou user_id manquant"}), 400

    numero_mc = data.get('numero_mc')
    type_versement = data.get('type')
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = to_dot_decimal(montant)
        if montant == 0:
            return jsonify({"error": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        cur.execute("SELECT montant, cf, numero_cf FROM MOUVEMENTC WHERE numero_mc = %s AND origine IN ('VERSEMENT C', 'VERSEMENT F') AND user_id = %s",
                    (numero_mc, user_id))
        versement = cur.fetchone()
        if not versement:
            logger.error(f"Versement {numero_mc} non trouvé pour user_id {user_id}")
            return jsonify({"error": "Versement non trouvé"}), 404

        if type_versement != versement['cf']:
            logger.error(f"Type {type_versement} ne correspond pas au versement {numero_mc}")
            return jsonify({"error": "Type ne correspond pas au versement"}), 400

        if versement['cf'] == 'C':
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        cur.execute(f"SELECT solde FROM {table} WHERE {id_column} = %s AND user_id = %s", (numero_cf, user_id))
        entity = cur.fetchone()
        if not entity:
            logger.error(f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} {numero_cf} non trouvé pour user_id {user_id}")
            return jsonify({"error": f"{'Client' if versement['cf'] == 'C' else 'Fournisseur'} non trouvé"}), 400

        old_montant = to_dot_decimal(versement['montant'])
        current_solde = to_dot_decimal(entity['solde'] or '0,00')
        solde_change = -old_montant + montant
        new_solde = current_solde + solde_change
        new_solde_str = to_comma_decimal(new_solde)

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        now = datetime.utcnow()
        cur.execute("""
            UPDATE MOUVEMENTC 
            SET montant = %s, justificatif = %s, date_mc = %s, time_mc = %s
            WHERE numero_mc = %s AND origine = %s AND user_id = %s
        """, (to_comma_decimal(montant), justificatif, now.date(), now, numero_mc, origine, user_id))

        if cur.rowcount == 0:
            conn.rollback()
            logger.error(f"Versement {numero_mc} non modifié")
            return jsonify({"error": "Versement non modifié"}), 500

        conn.commit()
        logger.info(f"Versement modifié: numero_mc={numero_mc}, type={type_versement}, montant={to_comma_decimal(montant)}, justificatif={justificatif}")
        return jsonify({"statut": "Versement modifié", "numero_mc": numero_mc}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification versement: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/situation_versements', methods=['GET'])
def situation_versements():
    type_versement = request.args.get('type')  # 'C' ou 'F'
    numero_cf = request.args.get('numero_cf')  # ID du client ou fournisseur
    user_id = request.args.get('user_id')     # ID de l'utilisateur

    if not user_id:
        return jsonify({'erreur': 'user_id est requis'}), 400
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
            LEFT JOIN client cl ON mc.cf = 'C' AND mc.numero_cf = cl.numero_clt AND cl.user_id = %s
            LEFT JOIN fournisseur f ON mc.cf = 'F' AND mc.numero_cf = f.numero_fou AND f.user_id = %s
            LEFT JOIN utilisateur u ON mc.numero_util = u.numero_util AND u.user_id = %s
            WHERE mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
            AND mc.cf = %s
            AND mc.numero_cf = %s
            AND mc.user_id = %s
            ORDER BY mc.date_mc DESC, mc.time_mc DESC
        """
        params = [user_id, user_id, user_id, type_versement, numero_cf, user_id]

        cur.execute(query, params)
        rows = cur.fetchall()

        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': to_comma_decimal(to_dot_decimal(row['montant'])),
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
        logger.info(f"Situation versements: type={type_versement}, numero_cf={numero_cf}, user_id={user_id}, {len(versements)} versements")
        return jsonify(versements), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        logger.error(f"Erreur récupération situation versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
@app.route('/annuler_vente', methods=['POST'])
def annuler_vente():
    # Récupération et validation des données JSON
    data = request.get_json()
    required_fields = ['numero_comande', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error(f"Données d'annulation invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({"error": "Numéro de commande, mot de passe ou user_id manquant"}), 400

    numero_comande = data.get('numero_comande')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    # Validation des types
    try:
        numero_comande = int(numero_comande)  # Vérifier que c'est un entier
        if not isinstance(password2, str) or not password2.strip():
            logger.error("Mot de passe invalide (vide ou non-chaîne)")
            return jsonify({"error": "Mot de passe invalide"}), 400
        if not isinstance(user_id, str) or not user_id.strip():
            logger.error("user_id invalide (vide ou non-chaîne)")
            return jsonify({"error": "user_id invalide"}), 400
    except (ValueError, TypeError):
        logger.error("Format invalide pour numero_comande")
        return jsonify({"error": "Format invalide pour numero_comande"}), 400

    conn = None
    cur = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'existence de la commande et récupérer le numero_util
        cur.execute("""
            SELECT c.numero_table, c.nature, c.numero_util 
            FROM comande c
            WHERE c.numero_comande = %s AND c.user_id = %s
        """, (numero_comande, user_id))
        commande = cur.fetchone()
        if not commande:
            logger.error(f"Commande {numero_comande} non trouvée pour user_id={user_id}")
            return jsonify({"error": "Commande non trouvée ou non associée à cet utilisateur"}), 404

        # Vérifier le mot de passe de l'utilisateur associé
        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                    (commande['numero_util'], user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {commande['numero_util']} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Utilisateur associé à la commande non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour la commande {numero_comande} et user_id={user_id}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la vente
        cur.execute("""
            SELECT numero_item, quantite, prixt
            FROM attache 
            WHERE numero_comande = %s
        """, (numero_comande,))
        lignes = cur.fetchall()
        if not lignes:
            logger.error(f"Aucune ligne trouvée pour la commande {numero_comande}")
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
            total_sale = sum(to_dot_decimal(ligne['prixt'] or '0,00') for ligne in lignes)
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                        (commande['numero_table'], user_id))
            client = cur.fetchone()
            if not client:
                logger.error(f"Client {commande['numero_table']} non trouvé pour user_id={user_id}")
                raise Exception(f"Client {commande['numero_table']} non trouvé")
            
            current_solde = to_dot_decimal(client['solde'] or '0,00')
            new_solde = current_solde - total_sale  # Réduire la dette (inverser la vente)
            new_solde_str = to_comma_decimal(new_solde)
            
            cur.execute("""
                UPDATE client 
                SET solde = %s 
                WHERE numero_clt = %s AND user_id = %s
            """, (new_solde_str, commande['numero_table'], user_id))
            logger.info(f"Solde client mis à jour: numero_clt={commande['numero_table']}, user_id={user_id}, total_sale={total_sale}, new_solde={new_solde_str}")

        # Supprimer les enregistrements associés dans encaisse
        cur.execute("DELETE FROM encaisse WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))
        logger.info(f"Enregistrements supprimés de la table encaisse pour numero_comande={numero_comande}, user_id={user_id}")

        # Supprimer les lignes de attache
        cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))

        # Supprimer la commande
        cur.execute("DELETE FROM comande WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))

        conn.commit()
        logger.info(f"Vente annulée: numero_comande={numero_comande}, user_id={user_id}, {len(lignes)} lignes")
        return jsonify({"statut": "Vente annulée"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation vente: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
          
# Endpoint: Annuler une réception
@app.route('/annuler_reception', methods=['POST'])
def annuler_reception():
    # Récupération et validation des données JSON
    data = request.get_json()
    required_fields = ['numero_mouvement', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error(f"Données d'annulation invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({"error": "Numéro de mouvement, mot de passe ou user_id manquant"}), 400

    numero_mouvement = data.get('numero_mouvement')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    # Validation des types
    try:
        numero_mouvement = int(numero_mouvement)  # Vérifier que c'est un entier
        if not isinstance(password2, str) or not password2.strip():
            logger.error("Mot de passe invalide (vide ou non-chaîne)")
            return jsonify({"error": "Mot de passe invalide"}), 400
        if not isinstance(user_id, str) or not user_id.strip():
            logger.error("user_id invalide (vide ou non-chaîne)")
            return jsonify({"error": "user_id invalide"}), 400
    except (ValueError, TypeError):
        logger.error("Format invalide pour numero_mouvement")
        return jsonify({"error": "Format invalide pour numero_mouvement"}), 400

    conn = None
    cur = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Vérifier l'existence du mouvement et récupérer le numero_util
        cur.execute("""
            SELECT m.numero_four, m.numero_util 
            FROM mouvement m
            WHERE m.numero_mouvement = %s AND m.nature = 'Bon de réception' AND m.user_id = %s
        """, (numero_mouvement, user_id))
        mouvement = cur.fetchone()
        if not mouvement:
            logger.error(f"Mouvement {numero_mouvement} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Mouvement non trouvé ou non associé à cet utilisateur"}), 404

        # Vérifier le mot de passe de l'utilisateur associé
        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                    (mouvement['numero_util'], user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {mouvement['numero_util']} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Utilisateur associé au mouvement non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour le mouvement {numero_mouvement} et user_id={user_id}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer les lignes de la réception
        cur.execute("""
            SELECT numero_item, qtea, nprix 
            FROM attache2 
            WHERE numero_mouvement = %s
        """, (numero_mouvement,))
        lignes = cur.fetchall()

        if not lignes:
            logger.error(f"Aucune ligne trouvée pour le mouvement {numero_mouvement}")
            return jsonify({"error": "Aucune ligne de réception trouvée"}), 404

        # Calculer le coût total de la réception
        total_cost = sum(to_dot_decimal(ligne['qtea']) * to_dot_decimal(ligne['nprix']) for ligne in lignes)

        # Restaurer le stock dans item
        for ligne in lignes:
            cur.execute("""
                UPDATE item 
                SET qte = qte - %s 
                WHERE numero_item = %s AND user_id = %s
            """, (to_dot_decimal(ligne['qtea']), ligne['numero_item'], user_id))

        # Mettre à jour le solde du fournisseur
        cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", 
                    (mouvement['numero_four'], user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            raise Exception(f"Fournisseur {mouvement['numero_four']} non trouvé pour user_id={user_id}")

        current_solde = to_dot_decimal(fournisseur['solde'] or '0,00')
        new_solde = current_solde + total_cost  # Inverser l'effet de la réception
        new_solde_str = to_comma_decimal(new_solde)

        cur.execute("""
            UPDATE fournisseur 
            SET solde = %s 
            WHERE numero_fou = %s AND user_id = %s
        """, (new_solde_str, mouvement['numero_four'], user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={mouvement['numero_four']}, user_id={user_id}, total_cost={total_cost}, new_solde={new_solde_str}")

        # Supprimer les lignes de attache2
        cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", 
                    (numero_mouvement, user_id))

        # Supprimer le mouvement
        cur.execute("DELETE FROM mouvement WHERE numero_mouvement = %s AND user_id = %s", 
                    (numero_mouvement, user_id))

        conn.commit()
        logger.info(f"Réception annulée: numero_mouvement={numero_mouvement}, user_id={user_id}, {len(lignes)} lignes")
        return jsonify({"statut": "Réception annulée"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur annulation réception: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Endpoint: Modifier une vente
@app.route('/modifier_vente/<int:numero_comande>', methods=['PUT'])
def modifier_vente(numero_comande):
    data = request.get_json()
    required_fields = ['lignes', 'numero_util', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields) or not data['lignes']:
        logger.error(f"Données de vente invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({"error": "Données de vente invalides, utilisateur, mot de passe ou user_id manquant"}), 400

    numero_table = int(data.get('numero_table', 0))
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')
    amount_paid = to_dot_decimal(data.get('amount_paid', '0,00'))  # Convertit en float avec point
    amount_paid_str = to_comma_decimal(amount_paid)  # Convertit en string avec virgule
    lignes = data['lignes']
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')

    if not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id invalide (vide ou non-chaîne)")
        return jsonify({"error": "user_id invalide"}), 400

    conn = None
    cur = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                    (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur or utilisateur['password2'] != password2:
            logger.error(f"Utilisateur {numero_util} ou mot de passe incorrect pour user_id={user_id}")
            return jsonify({"error": "Utilisateur ou mot de passe incorrect"}), 401

        # Vérifier l'existence de la commande
        cur.execute("SELECT numero_table FROM comande WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))
        commande = cur.fetchone()
        if not commande:
            logger.error(f"Commande {numero_comande} non trouvée pour user_id={user_id}")
            return jsonify({"error": "Commande non trouvée ou non associée à cet utilisateur"}), 404

        # Restaurer le solde client si paiement à terme et numero_table != 0
        if commande['numero_table'] != 0:
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", 
                        (commande['numero_table'], user_id))
            old_client = cur.fetchone()
            if old_client and old_client['solde']:
                old_solde = to_dot_decimal(old_client['solde'])
                cur.execute("UPDATE client SET solde = %s WHERE numero_clt = %s AND user_id = %s",
                            (to_comma_decimal(0), commande['numero_table'], user_id))

        # Restaurer le stock des anciens articles
        cur.execute("SELECT numero_item, quantite FROM attache WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))
        old_lignes = cur.fetchall()
        for ligne in old_lignes:
            cur.execute("UPDATE item SET qte = qte + %s WHERE numero_item = %s AND user_id = %s",
                        (ligne['quantite'], ligne['numero_item'], user_id))

        # Supprimer les anciennes lignes et l'entrée encaisse
        cur.execute("DELETE FROM attache WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))
        cur.execute("DELETE FROM encaisse WHERE numero_comande = %s AND user_id = %s", 
                    (numero_comande, user_id))

        # Mettre à jour la commande
        nature = "TICKET" if numero_table == 0 else "BON DE L."
        cur.execute("""
            UPDATE comande 
            SET numero_table = %s, date_comande = %s, nature = %s, numero_util = %s, user_id = %s
            WHERE numero_comande = %s AND user_id = %s
        """, (numero_table, date_comande, nature, numero_util, user_id, numero_comande, user_id))

        # Insérer les nouvelles lignes et ajuster le stock
        total_vente = 0.0
        for ligne in lignes:
            quantite = to_dot_decimal(ligne.get('quantite', '1'))
            remarque = to_dot_decimal(ligne.get('remarque', '0,00'))  # Prix unitaire
            prixt = to_dot_decimal(ligne.get('prixt', '0,00'))  # Total de la ligne
            total_vente += quantite * remarque  # Calcul avec prix unitaire

            # Conversion pour stockage avec virgule
            prixt_str = to_comma_decimal(prixt)
            prixbh_str = to_comma_decimal(to_dot_decimal(ligne.get('prixbh', '0,00')))

            # Gestion de la remarque
            remarque_str = ligne.get('remarque', '')
            if isinstance(remarque_str, (int, float)):
                remarque_str = to_comma_decimal(remarque_str)
            elif isinstance(remarque_str, str) and any(c.isdigit() for c in remarque_str):
                try:
                    if '.' in remarque_str or ',' in remarque_str:
                        remarque_str = to_comma_decimal(to_dot_decimal(remarque_str))
                except ValueError:
                    pass  # Garder la valeur originale si la conversion échoue

            cur.execute("""
                INSERT INTO attache (numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx, send, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (numero_comande, ligne.get('numero_item'), quantite, prixt_str,
                  remarque_str, prixbh_str, 0, True, user_id))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s AND user_id = %s",
                        (quantite, ligne.get('numero_item'), user_id))

        # Insertion dans encaisse
        total_vente_str = to_comma_decimal(total_vente)
        montant_reglement = total_vente if payment_mode == 'espece' else amount_paid
        montant_reglement_str = to_comma_decimal(montant_reglement)
        solde_restant = total_vente - amount_paid if payment_mode == 'a_terme' else 0.0
        solde_restant_str = to_comma_decimal(solde_restant)

        cur.execute("""
            INSERT INTO encaisse (
                apaye, reglement, tva, ht, 
                numero_comande, origine, time_enc, soldeR, user_id
            ) VALUES (
                %s, %s, %s, %s, 
                %s, %s, %s, %s, %s
            )
        """, (
            total_vente_str,        # apaye stocké avec virgule
            montant_reglement_str,  # reglement stocké avec virgule
            '0,00',                 # TVA (ajuste si nécessaire)
            total_vente_str,        # HT = total_vente
            numero_comande,
            nature,
            datetime.now(),
            solde_restant_str,      # Solde restant avec virgule
            user_id
        ))

        # Mise à jour du solde client si à terme
        if payment_mode == 'a_terme' and numero_table != 0:
            cur.execute("""
                UPDATE client SET solde = solde + %s 
                WHERE numero_clt = %s AND user_id = %s
            """, (solde_restant_str, numero_table, user_id))

        conn.commit()
        logger.info(f"Vente modifiée: numero_comande={numero_comande}, user_id={user_id}, {len(lignes)} lignes")
        return jsonify({
            "numero_comande": numero_comande,
            "statut": "Vente modifiée",
            "total_vente": total_vente_str,  # Renvoyé avec virgule
            "montant_verse": amount_paid_str,  # Renvoyé avec virgule
            "reglement": montant_reglement_str,  # Renvoyé avec virgule
            "solde_restant": solde_restant_str if payment_mode == 'a_terme' else "0,00"
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification vente: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Endpoint: Récupérer une vente
@app.route('/vente/<int:numero_comande>', methods=['GET'])
def get_vente(numero_comande):
    user_id = request.args.get('user_id')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id manquant ou invalide dans la requête")
        return jsonify({"error": "user_id requis et doit être une chaîne non vide"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Récupérer les détails de la commande
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
            logger.error(f"Commande {numero_comande} non trouvée pour user_id={user_id}")
            return jsonify({"error": "Commande non trouvée ou non associée à cet utilisateur"}), 404

        # Récupérer les lignes de la commande
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
                    'prixt': str(ligne['prixt']),
                    'remarque': ligne['remarque'] or '',
                    'prixbh': str(ligne['prixbh'])
                }
                for ligne in lignes
            ]
        }

        cur.close()
        conn.close()
        logger.info(f"Vente récupérée: numero_comande={numero_comande}, user_id={user_id}")
        return jsonify(response), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        logger.error(f"Erreur récupération vente: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Endpoint: Récupérer une réception
@app.route('/reception/<int:numero_mouvement>', methods=['GET'])
def get_reception(numero_mouvement):
    user_id = request.args.get('user_id')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id manquant ou invalide dans la requête")
        return jsonify({"error": "user_id requis et doit être une chaîne non vide"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Récupérer les détails du mouvement
        cur.execute("""
            SELECT m.numero_mouvement, m.numero_four, m.date_m, m.nature, m.numero_util,
                   f.nom AS fournisseur_nom, u.nom AS utilisateur_nom
            FROM mouvement m
            LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou AND f.user_id = %s
            LEFT JOIN utilisateur u ON m.numero_util = u.numero_util AND u.user_id = %s
            WHERE m.numero_mouvement = %s AND m.nature = 'Bon de réception' AND m.user_id = %s
        """, (user_id, user_id, numero_mouvement, user_id))
        mouvement = cur.fetchone()

        if not mouvement:
            logger.error(f"Mouvement {numero_mouvement} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Mouvement non trouvé ou non associé à cet utilisateur"}), 404

        # Récupérer les lignes du mouvement
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
                    'nprix': str(ligne['nprix']),
                    'nqte': ligne['nqte'],
                    'pump': str(ligne['pump'])
                }
                for ligne in lignes
            ]
        }

        cur.close()
        conn.close()
        logger.info(f"Réception récupérée: numero_mouvement={numero_mouvement}, user_id={user_id}")
        return jsonify(response), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        logger.error(f"Erreur récupération réception: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Endpoint: Modifier une réception
@app.route('/modifier_reception/<int:numero_mouvement>', methods=['PUT'])
def modifier_reception(numero_mouvement):
    data = request.get_json()
    required_fields = ['lignes', 'numero_four', 'numero_util', 'password2', 'user_id']
    if not data or any(field not in data for field in required_fields) or not data['lignes']:
        logger.error(f"Données de réception invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({"error": "Données de réception invalides, fournisseur, utilisateur, mot de passe ou user_id manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    user_id = data.get('user_id')
    lignes = data['lignes']

    if not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id invalide (vide ou non-chaîne)")
        return jsonify({"error": "user_id invalide"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT password2 FROM utilisateur WHERE numero_util = %s AND user_id = %s", 
                    (numero_util, user_id))
        utilisateur = cur.fetchone()
        if not utilisateur:
            logger.error(f"Utilisateur {numero_util} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            logger.error(f"Mot de passe incorrect pour l'utilisateur {numero_util} et user_id={user_id}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("""
            SELECT numero_fou, CAST(COALESCE(NULLIF(REPLACE(solde, ',', '.'), ''), '0') AS FLOAT) AS solde 
            FROM fournisseur WHERE numero_fou = %s AND user_id = %s
        """, (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            logger.error(f"Fournisseur {numero_four} non trouvé pour user_id={user_id}")
            return jsonify({"error": "Fournisseur non trouvé"}), 400

        # Vérifier que la réception existe
        cur.execute("SELECT numero_mouvement, numero_four FROM mouvement WHERE numero_mouvement = %s AND user_id = %s", 
                    (numero_mouvement, user_id))
        mouvement = cur.fetchone()
        if not mouvement:
            logger.error(f"Réception {numero_mouvement} non trouvée pour user_id={user_id}")
            return jsonify({"error": "Réception non trouvée ou non associée à cet utilisateur"}), 404

        # Récupérer les lignes précédentes de la réception (quantités et prix)
        cur.execute("""
            SELECT numero_item, qtea, CAST(COALESCE(NULLIF(REPLACE(nprix, ',', '.'), ''), '0') AS FLOAT) AS nprix
            FROM attache2
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_mouvement, user_id))
        old_lines = cur.fetchall()
        old_lines_dict = {line['numero_item']: line for line in old_lines}
        old_total_cost = sum(float(line['qtea']) * float(line['nprix']) for line in old_lines)
        logger.info(f"Coût total réception précédente: {old_total_cost}, user_id={user_id}")

        # Restaurer le solde initial (annuler l'effet de la réception précédente)
        current_solde = float(fournisseur['solde'] or 0)
        restored_solde = current_solde + old_total_cost
        logger.info(f"Solde restauré: {restored_solde}, user_id={user_id}")

        # Récupérer les quantités actuelles des articles
        item_ids = list(set([ligne.get('numero_item') for ligne in lignes] + list(old_lines_dict.keys())))
        cur.execute("""
            SELECT numero_item, qte, CAST(COALESCE(NULLIF(REPLACE(prixba, ',', '.'), ''), '0') AS FLOAT) AS prixba 
            FROM item WHERE numero_item IN %s AND user_id = %s
        """, (tuple(item_ids), user_id))
        items = {item['numero_item']: item for item in cur.fetchall()}

        # Calculer le nouveau coût total et préparer les mises à jour du stock
        new_total_cost = 0.0
        stock_updates = {}  # {numero_item: {old_qtea, new_qtea, prixbh}}

        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            new_qtea = to_dot_decimal(ligne.get('qtea', '0'))
            prixbh = to_dot_decimal(ligne.get('prixbh', '0'))

            if new_qtea < 0:
                raise Exception("La quantité ajoutée ne peut pas être négative")
            if prixbh < 0:
                raise Exception("Le prix d'achat ne peut pas être négatif")

            # Vérifier l'article
            item = items.get(numero_item)
            if not item:
                raise Exception(f"Article {numero_item} non trouvé pour user_id={user_id}")

            current_qte = float(item['qte'] or 0)
            old_qtea = float(old_lines_dict.get(numero_item, {}).get('qtea', 0))

            # Calculer le coût de la ligne
            new_total_cost += new_qtea * prixbh

            # Stocker les informations pour la mise à jour du stock
            stock_updates[numero_item] = {
                'old_qtea': old_qtea,
                'new_qtea': new_qtea,
                'prixbh': prixbh,
                'current_qte': current_qte,
                'current_prixba': float(item['prixba'] or 0)
            }

        # Traiter les articles supprimés (présents dans old_lines mais absents dans lignes)
        for numero_item, old_line in old_lines_dict.items():
            if numero_item not in stock_updates:
                item = items.get(numero_item)
                current_qte = float(item['qte'] or 0) if item else 0
                current_prixba = float(item['prixba'] or 0) if item else 0
                stock_updates[numero_item] = {
                    'old_qtea': float(old_line['qtea']),
                    'new_qtea': 0,
                    'prixbh': 0,
                    'current_qte': current_qte,
                    'current_prixba': current_prixba
                }

        # Mettre à jour le solde du fournisseur
        new_solde = restored_solde - new_total_cost
        new_solde_str = to_comma_decimal(new_solde)
        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        logger.info(f"Solde fournisseur mis à jour: numero_fou={numero_four}, user_id={user_id}, new_total_cost={new_total_cost}, new_solde={new_solde_str}")

        # Supprimer les anciennes lignes de la réception
        cur.execute("DELETE FROM attache2 WHERE numero_mouvement = %s AND user_id = %s", 
                    (numero_mouvement, user_id))

        # Insérer les nouvelles lignes et mettre à jour le stock
        for numero_item, update_info in stock_updates.items():
            old_qtea = update_info['old_qtea']
            new_qtea = update_info['new_qtea']
            prixbh = update_info['prixbh']
            current_qte = update_info['current_qte']
            current_prixba = update_info['current_prixba']

            # Restaurer le stock initial (annuler l'ancienne quantité)
            restored_qte = current_qte - old_qtea
            # Appliquer la nouvelle quantité
            new_qte = restored_qte + new_qtea

            if new_qte < 0:
                raise Exception(f"Stock négatif pour l'article {numero_item}: {new_qte}")

            # Si l'article est dans les nouvelles lignes, insérer dans attache2
            if new_qtea > 0:
                prixbh_str = to_comma_decimal(prixbh)[:30]
                cur.execute("""
                    INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (numero_item, numero_mouvement, new_qtea, new_qte, prixbh_str, prixbh_str, True, user_id))

            # Mettre à jour le stock et le prix d'achat
            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (new_qte, to_comma_decimal(prixbh)[:30] if new_qtea > 0 else to_comma_decimal(current_prixba)[:30], 
                         numero_item, user_id))
            logger.info(f"Stock mis à jour: numero_item={numero_item}, user_id={user_id}, old_qtea={old_qtea}, new_qtea={new_qtea}, new_qte={new_qte}")

        # Mettre à jour le mouvement
        cur.execute("""
            UPDATE mouvement 
            SET numero_four = %s, numero_util = %s, date_m = %s, user_id = %s
            WHERE numero_mouvement = %s AND user_id = %s
        """, (numero_four, numero_util, datetime.utcnow(), user_id, numero_mouvement, user_id))

        conn.commit()
        logger.info(f"Réception modifiée: numero_mouvement={numero_mouvement}, user_id={user_id}, {len(lignes)} lignes")
        return jsonify({
            "numero_mouvement": numero_mouvement,
            "total_cost": to_comma_decimal(new_total_cost),
            "new_solde": new_solde_str
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erreur modification réception: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Endpoint: Liste des catégories
@app.route('/liste_categories', methods=['GET'])
def liste_categories():
    user_id = request.args.get('user_id')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id manquant ou invalide dans la requête")
        return jsonify({"error": "user_id requis et doit être une chaîne non vide"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT numer_categorie, description_c FROM categorie WHERE user_id = %s ORDER BY description_c", 
                    (user_id,))
        categories = cur.fetchall()
        cur.close()
        conn.close()
        logger.info(f"Récupération de {len(categories)} catégories pour user_id={user_id}")
        return jsonify(categories), 200
    except Exception as e:
        logger.error(f"Erreur récupération catégories: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Ajouter une catégorie
@app.route('/ajouter_categorie', methods=['POST'])
def ajouter_categorie():
    data = request.get_json()
    required_fields = ['description_c', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error(f"Données d'ajout catégorie invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({'erreur': 'Description ou user_id requis'}), 400

    description_c = data.get('description_c')
    user_id = data.get('user_id')

    if not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id invalide (vide ou non-chaîne)")
        return jsonify({"error": "user_id invalide"}), 400

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
        logger.info(f"Catégorie ajoutée: id={category_id}, description={description_c}, user_id={user_id}")
        return jsonify({'statut': 'Catégorie ajoutée', 'id': category_id}), 201
    except Exception as e:
        logger.error(f"Erreur ajout catégorie: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Modifier une catégorie
@app.route('/modifier_categorie/<int:numer_categorie>', methods=['PUT'])
def modifier_categorie(numer_categorie):
    data = request.get_json()
    required_fields = ['description_c', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error(f"Données de modification catégorie invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({'erreur': 'Description ou user_id requis'}), 400

    description_c = data.get('description_c')
    user_id = data.get('user_id')

    if not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id invalide (vide ou non-chaîne)")
        return jsonify({"error": "user_id invalide"}), 400

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
            logger.error(f"Catégorie {numer_categorie} non trouvée pour user_id={user_id}")
            return jsonify({'erreur': 'Catégorie non trouvée ou non associée à cet utilisateur'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Catégorie modifiée: numer_categorie={numer_categorie}, user_id={user_id}")
        return jsonify({'statut': 'Catégorie modifiée'}), 200
    except Exception as e:
        logger.error(f"Erreur modification catégorie: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Supprimer une catégorie
@app.route('/supprimer_categorie/<int:numer_categorie>', methods=['DELETE'])
def supprimer_categorie(numer_categorie):
    user_id = request.args.get('user_id')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id manquant ou invalide dans la requête")
        return jsonify({"error": "user_id requis et doit être une chaîne non vide"}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Vérifier si la catégorie est utilisée par des produits
        cur.execute("SELECT 1 FROM item WHERE numero_categorie = %s AND user_id = %s", 
                    (numer_categorie, user_id))
        if cur.fetchone():
            cur.close()
            conn.close()
            logger.error(f"Catégorie {numer_categorie} utilisée par des produits pour user_id={user_id}")
            return jsonify({'erreur': 'Catégorie utilisée par des produits'}), 400
        cur.execute("DELETE FROM categorie WHERE numer_categorie = %s AND user_id = %s", 
                    (numer_categorie, user_id))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            logger.error(f"Catégorie {numer_categorie} non trouvée pour user_id={user_id}")
            return jsonify({'erreur': 'Catégorie non trouvée ou non associée à cet utilisateur'}), 404
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Catégorie supprimée: numer_categorie={numer_categorie}, user_id={user_id}")
        return jsonify({'statut': 'Catégorie supprimée'}), 200
    except Exception as e:
        logger.error(f"Erreur suppression catégorie: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# Endpoint: Assigner une catégorie à un produit
@app.route('/assigner_categorie', methods=['POST'])
def assigner_categorie():
    data = request.get_json()
    required_fields = ['numero_item', 'user_id']
    if not data or any(field not in data for field in required_fields):
        logger.error(f"Données d'assignation catégorie invalides: champs manquants {', '.join(field for field in required_fields if field not in data)}")
        return jsonify({'erreur': 'Numéro d\'article ou user_id requis'}), 400

    numero_item = data.get('numero_item')
    numero_categorie = data.get('numer_categorie')
    user_id = data.get('user_id')

    try:
        numero_item = int(numero_item)
        if numero_categorie is not None:
            numero_categorie = int(numero_categorie)
        if not isinstance(user_id, str) or not user_id.strip():
            logger.error("user_id invalide (vide ou non-chaîne)")
            return jsonify({"error": "user_id invalide"}), 400
    except (ValueError, TypeError) as e:
        logger.error(f"numero_item ou numero_categorie invalide: {str(e)}")
        return jsonify({'erreur': 'Numéro d\'article ou de catégorie doit être un entier'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            "SELECT numero_item, designation FROM item WHERE numero_item = %s AND user_id = %s",
            (numero_item, user_id)
        )
        item = cur.fetchone()
        if not item:
            logger.error(f"Article non trouvé: numero_item={numero_item}, user_id={user_id}")
            cur.close()
            conn.close()
            return jsonify({'erreur': f'Article {numero_item} non trouvé ou non associé à cet utilisateur'}), 404

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
                return jsonify({'erreur': f'Catégorie {numero_categorie} non trouvée ou non associée à cet utilisateur'}), 404

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
        logger.info(f"Catégorie assignée: numero_item={numero_item}, numer_categorie={numero_categorie}, user_id={user_id}")
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

# Endpoint: Liste des produits par catégorie
@app.route('/liste_produits_par_categorie', methods=['GET'])
def liste_produits_par_categorie():
    user_id = request.args.get('user_id')
    if not user_id or not isinstance(user_id, str) or not user_id.strip():
        logger.error("user_id manquant ou invalide dans la requête")
        return jsonify({"error": "user_id requis et doit être une chaîne non vide"}), 400

    numero_categorie = request.args.get('numero_categorie', type=int)

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if numero_categorie is None and 'numero_categorie' in request.args:
            cur.execute(
                "SELECT numero_item, designation FROM item WHERE numero_categorie IS NULL AND user_id = %s",
                (user_id,)
            )
            produits = cur.fetchall()
            cur.close()
            conn.close()
            logger.info(f"Récupération de {len(produits)} produits sans catégorie pour user_id={user_id}")
            return jsonify({'produits': produits}), 200
        else:
            cur.execute("""
                SELECT c.numer_categorie, c.description_c, i.numero_item, i.designation
                FROM categorie c
                LEFT JOIN item i ON c.numer_categorie = i.numero_categorie AND i.user_id = %s
                WHERE (c.numer_categorie = %s OR %s IS NULL) AND c.user_id = %s
            """, (user_id, numero_categorie, numero_categorie, user_id))
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
            logger.info(f"Récupération de {len(categories)} catégories avec produits pour user_id={user_id}")
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
