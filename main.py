from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os
import logging

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])  # Autoriser les requêtes depuis GitHub Pages

# Configurer les logs
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Connexion à la base de données
def get_conn():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PG_DBNAME', 'your_db_name'),
            user=os.getenv('PG_USER', 'your_db_user'),
            password=os.getenv('PG_PASSWORD', 'your_db_password'),
            host=os.getenv('PG_HOST', 'your_db_host'),
            port=os.getenv('PG_PORT', '5432')
        )
        logging.debug("Connexion à la base de données établie")
        return conn
    except Exception as e:
        logging.error(f"Erreur de connexion à la base de données: {str(e)}")
        raise

# Route de test
@app.route('/', methods=['GET'])
def home():
    logging.debug("Requête reçue sur /")
    return "API FirePoz est en ligne !"

# --- Gestion des clients ---
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        logging.error("Identifiant utilisateur manquant")
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Récupération des clients pour user_id: {user_id}")
        cur.execute("""
            SELECT numero_clt, nom, solde, reference, contact, adresse
            FROM client
            WHERE user_id = %s
            ORDER BY nom
        """, (user_id,))
        rows = cur.fetchall()
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
        logging.debug(f"{len(clients)} clients récupérés")
        return jsonify(clients)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des clients: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

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
        logging.error("Champs obligatoires manquants pour ajouter un client")
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Ajout d'un client: {nom}")
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
        logging.debug(f"Client ajouté avec numero_clt: {numero_clt}")
        return jsonify({'statut': 'Client ajouté', 'numero_clt': numero_clt})
    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur lors de l'ajout du client: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --- Gestion des fournisseurs ---
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        logging.error("Identifiant utilisateur manquant")
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Récupération des fournisseurs pour user_id: {user_id}")
        cur.execute("""
            SELECT numero_fou, nom, solde, reference, contact, adresse
            FROM fournisseur
            WHERE user_id = %s
            ORDER BY nom
        """, (user_id,))
        rows = cur.fetchall()
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
        logging.debug(f"{len(fournisseurs)} fournisseurs récupérés")
        return jsonify(fournisseurs)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des fournisseurs: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

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
        logging.error("Champs obligatoires manquants pour ajouter un fournisseur")
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Ajout d'un fournisseur: {nom}")
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
        logging.debug(f"Fournisseur ajouté avec numero_fou: {numero_fou}")
        return jsonify({'statut': 'Fournisseur ajouté', 'numero_fou': numero_fou})
    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur lors de l'ajout du fournisseur: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --- Gestion des produits ---
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        logging.error("Identifiant utilisateur manquant")
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Récupération des produits pour user_id: {user_id}")
        cur.execute("""
            SELECT bar, designation, prix, qte, prixba
            FROM item
            WHERE user_id = %s
            ORDER BY designation
        """, (user_id,))
        rows = cur.fetchall()
        produits = [
            {
                'BAR': row[0],
                'DESIGNATION': row[1],
                'PRIX': str(row[2]),
                'QTE': row[3],
                'PRIXBA': str(row[4]) if row[4] is not None else '0.00'
            } for row in rows
        ]
        logging.debug(f"{len(produits)} produits récupérés")
        return jsonify(produits)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des produits: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

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
        logging.error("Champs obligatoires manquants pour ajouter un produit")
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Vérification du code-barres: {bar}")
        cur.execute("SELECT bar FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
        if cur.fetchone():
            logging.error("Code-barres déjà existant")
            return jsonify({'erreur': 'Ce code-barres existe déjà'}), 400

        logging.debug(f"Ajout d'un produit: {designation}")
        cur.execute(
            """
            INSERT INTO item (bar, designation, prix, qte, prixba, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (bar, designation, float(prix), int(qte), prixba or '0.00', user_id)
        )
        conn.commit()
        logging.debug("Produit ajouté avec succès")
        return jsonify({'statut': 'Produit ajouté'})
    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur lors de l'ajout du produit: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

# --- Gestion des ventes ---
@app.route('/liste_ventes', methods=['GET'])
def liste_ventes():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        logging.error("Identifiant utilisateur manquant")
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Récupération des ventes pour user_id: {user_id}")
        cur.execute("""
            SELECT a.numero_attache, a.numero_comande, i.designation, a.numero_item, 
                   a.quantite, a.prixt, a.remarque, a.prixbh, a.send
            FROM attache a
            JOIN item i ON a.numero_item = i.bar AND i.user_id = %s
            WHERE a.user_id = %s
            ORDER BY a.numero_comande DESC
        """, (user_id, user_id))
        rows = cur.fetchall()
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
        logging.debug(f"{len(ventes)} ventes récupérées")
        return jsonify(ventes)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des ventes: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

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
        logging.error("Champs obligatoires manquants pour ajouter une vente")
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    conn = get_conn()
    cur = conn.cursor()
    try:
        logging.debug(f"Vérification du produit: {produit_bar}")
        cur.execute("SELECT qte, prix, prixba FROM item WHERE bar = %s AND user_id = %s", (produit_bar, user_id))
        produit = cur.fetchone()
        if not produit:
            logging.error("Produit introuvable")
            return jsonify({'erreur': 'Produit introuvable'}), 404
        if produit[0] < quantite:
            logging.error("Quantité insuffisante en stock")
            return jsonify({'erreur': 'Quantité insuffisante en stock'}), 400
        prixbh = produit[2] or '0.00'  # Utiliser prixba comme prixbh

        if client_id:
            logging.debug(f"Vérification du client: {client_id}")
            cur.execute("SELECT numero_clt FROM client WHERE numero_clt = %s AND user_id = %s", (client_id, user_id))
            if not cur.fetchone():
                logging.error("Client introuvable")
                return jsonify({'erreur': 'Client introuvable'}), 404

        logging.debug("Création d'une nouvelle commande")
        cur.execute(
            """
            INSERT INTO comande (numero_table, date_comande, etat_c, numero_util, nature, user_id, compteur)
            VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, 0)
            RETURNING numero_comande
            """,
            (client_id or None, etat_c, numero_util, nature, user_id)
        )
        numero_comande = cur.fetchone()[0]
        logging.debug(f"Commande créée avec numero_comande: {numero_comande}")

        logging.debug("Ajout de la ligne de vente")
        cur.execute(
            """
            INSERT INTO attache (numero_item, quantite, prixt, remarque, send, numero_comande, prixbh, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_attache
            """,
            (produit_bar, quantite, float(prixt), remarque, False, numero_comande, prixbh, user_id)
        )
        numero_attache = cur.fetchone()[0]
        logging.debug(f"Ligne de vente ajoutée avec numero_attache: {numero_attache}")

        logging.debug("Mise à jour du stock")
        cur.execute("UPDATE item SET qte = qte - %s WHERE bar = %s AND user_id = %s", 
                   (quantite, produit_bar, user_id))

        if client_id:
            logging.debug(f"Mise à jour du solde du client: {client_id}")
            cur.execute("UPDATE client SET solde = solde + %s WHERE numero_clt = %s AND user_id = %s",
                       (float(prixt), client_id, user_id))

        conn.commit()
        logging.debug("Vente enregistrée avec succès")
        return jsonify({'statut': 'Vente enregistrée', 'numero_comande': numero_comande, 'numero_attache': numero_attache})
    except Exception as e:
        conn.rollback()
        logging.error(f"Erreur lors de l'ajout de la vente: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)