from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2 import Error as PsycopgError
import os

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])  # Autoriser les requêtes depuis ton domaine GitHub Pages

# Fonction pour établir une connexion à la base de données
def get_conn():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PG_DBNAME', 'your_db_name'),
            user=os.getenv('PG_USER', 'your_db_user'),
            password=os.getenv('PG_PASSWORD', 'your_db_password'),
            host=os.getenv('PG_HOST', 'your_db_host'),
            port=os.getenv('PG_PORT', '5432')
        )
        return conn
    except PsycopgError as e:
        print(f"Erreur de connexion à la base de données: {e}")
        raise

# Route de test
@app.route('/', methods=['GET'])
def home():
    return "API FirePoz est en ligne !"

# --- Gestion des clients ---
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT numero_clt, nom, solde, reference, contact, adresse
            FROM client
            WHERE user_id = %s
            ORDER BY nom
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        clients = [
            {
                'numero_clt': row[0],
                'nom': row[1],
                'solde': str(row[2]),
                'reference': row[3],
                'contact': row[4],
                'adresse': row[5]
            } for row in rows
        ]
        return jsonify(clients)
    except PsycopgError as e:
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = request.headers.get('X-User-ID')

    if not all([nom, solde, reference, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO client (nom, solde, reference, contact, adresse, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING numero_clt
            """,
            (nom, float(solde), reference, contact, adresse, user_id)
        )
        numero_clt = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté', 'numero_clt': numero_clt})
    except PsycopgError as e:
        conn.rollback()
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Gestion des fournisseurs ---
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    tryiativa: none

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT numero_fou, nom, solde, reference, contact, adresse
            FROM fournisseur
            WHERE user_id = %s
            ORDER BY nom
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        fournisseurs = [
            {
                'numero_fou': row[0],
                'nom': row[1],
                'solde': str(row[2]),
                'reference': row[3],
                'contact': row[4],
                'adresse': row[5]
            } for row in rows
        ]
        return jsonify(fournisseurs)
    except PsycopgError as e:
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_fournisseur', methods=['POST'])
def ajouter_fournisseur():
    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = request.headers.get('X-User-ID')

    if not all([nom, solde, reference, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING numero_fou
            """,
            (nom, float(solde), reference, contact, adresse, user_id)
        )
        numero_fou = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur ajouté', 'numero_fou': numero_fou})
    except PsycopgError as e:
        conn.rollback()
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Gestion des produits ---
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT bar, designation, prix, qte, prixba
            FROM item
            WHERE user_id = %s
            ORDER BY designation
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        produits = [
            {
                'BAR': row[0],
                'DESIGNATION': row[1],
                'PRIX': str(row[2]),
                'QTE': row[3],
                'PRIXBA': str(row[4]) if row[4] is not None else '0.00'
            } for row in rows
        ]
        return jsonify(produits)
    except PsycopgError as e:
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')
    user_id = request.headers.get('X-User-ID')

    if not all([designation, bar, prix, qte, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        # Vérifier si le code-barres existe déjà
        cur.execute("SELECT bar FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
        if cur.fetchone():
            return jsonify({'erreur': 'Ce code-barres existe déjà'}), 400

        cur.execute(
            """
            INSERT INTO item (bar, designation, prix, qte, prixba, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (bar, designation, float(prix), int(qte), prixba or '0.00', user_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Produit ajouté'})
    except PsycopgError as e:
        conn.rollback()
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        conn.rollback()
        return jsonify({'erreur': str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Gestion des ventes ---
@app.route('/liste_ventes', methods=['GET'])
def liste_ventes():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT a.numero_attache, a.numero_comande, i.designation, a.numero_item, 
                   a.quantite, a.prixt, a.remarque, a.prixbh, a.send
            FROM attache a
            JOIN item i ON a.numero_item = i.bar AND i.user_id = %s
            WHERE a.user_id = %s
            ORDER BY a.numero_comande DESC
        """, (user_id, user_id))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        ventes = [
            {
                'numero_attache': row[0],
                'numero_comande': row[1],
                'designation': row[2],
                'numero_item': row[3],
                'quantite': row[4],
                'prixt': str(row[5]),
                'remarque': row[6],
                'prixbh': str(row[7]) if row[7] is not None else '0.00',
                'send': row[8]
            } for row in rows
        ]
        return jsonify(ventes)
    except PsycopgError as e:
        return jsonify({'erreur': f"Erreur PostgreSQL: {e.pgerror}"}), 500
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_vente', methods=['POST'])
def ajouter_vente():
    data = request.get_json()
    client_id = data.get('client_id')
    produit_bar = data.get('produit_bar')
    quantite = data.get('quantite')
    prixt = data.get('prixt')
    remarque = data.get('remarque')
    numero_util = data.get('numero_util')
    etat_c = data.get('etat_c')
    nature = data.get('nature')
    user_id = request.headers.get('X-User-ID')

    if not all([produit_bar, quantite, prixt, remarque, user_id]):
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Vérifier si le produit existe et récupérer prixba
        cur.execute("SELECT qte, prix, prixba FROM item WHERE bar = %s AND user_id = %s", (produit_bar, user_id))
        produit = cur.fetchone()
        if not produit:
            return jsonify({'erreur': 'Produit introuvable'}), 404
        if produit[0] < quantite:
            return jsonify({'erreur': 'Quantité insuffisante en stock'}), 400
        prixbh = produit[2] or '0.00'  # Utiliser prixba comme prixbh

        # Vérifier si le client existe (si spécifié)
        if client_id:
            cur.execute("SELECT numero_clt FROM client WHERE numero_clt = %s AND user_id = %s", (client_id, user_id))
            if not cur.fetchone():
                return jsonify({'erreur': 'Client introuvable'}), 404

        # Créer une nouvelle commande avec CURRENT_TIMESTAMP
        cur.execute(
            """
            INSERT INTO comande (numero_table, date_comande, etat_c, numero_util, nature, user_id, compteur)
            VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s)
            RETURNING numero_comande
            """,
            (client_id, etat_c, numero_util, nature, user_id, 0)  # Compteur initialisé à 0
        )
        numero_comande = cur.fetchone()[0]

        # Ajouter la ligne de vente (attache)
        cur.execute(
            """
            INSERT INTO attache (numero_item, quantite, prixt, remarque, send, numero_comande, prixbh, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_attache
            """,
            (produit_bar, quantite, float(prixt), remarque, False, numero_comande, prixbh, user_id)
        )
        numero_attache = cur.fetchone()[0]

        # Mettre à jour le stock
        cur.execute("UPDATE item SET qte = qte - %s WHERE bar = %s AND user_id = %s", 
                   (quantite, produit_bar, user_id))

        # Mettre à jour le solde du client (si spécifié)
        if client_id:
            cur.execute("UPDATE client SET solde = solde + %s WHERE numero_clt = %s AND user_id = %s",
                       (float(prixt), client_id, user_id))

        conn.commit()
        return jsonify({'statut': 'Vente enregistrée', 'numero_comande': numero_comande, 'numero_attache': numero_attache})
    except PsycopgError as e:
        if conn:
            conn.rollback()
        error_message = f"Erreur PostgreSQL: {e.pgerror}\nCode: {e.pgcode}"
        print(error_message)  # Pour le débogage
        return jsonify({'erreur': error_message}), 500
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur générale: {str(e)}")  # Pour le débogage
        return jsonify({'erreur': str(e)}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)