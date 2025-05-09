from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os
import logging
from datetime import datetime

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connexion à la base de données
def get_conn():
    return psycopg2.connect(
        dbname=os.getenv('PG_DBNAME', 'firepoz_db'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', ''),
        host=os.getenv('PG_HOST', 'localhost'),
        port=os.getenv('PG_PORT', '5432')
    )

# Helpers
def validate_user_id(user_id):
    if not user_id:
        logging.warning("Tentative d'accès sans user_id")
        return jsonify({'error': 'Authentification requise'}), 401
    return None

def float_to_str(value):
    return str(value) if value is not None else '0.00'

# Routes
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'active',
        'version': '1.0',
        'timestamp': datetime.now().isoformat()
    })

# --- Clients ---
@app.route('/clients', methods=['GET', 'POST'])
def gestion_clients():
    user_id = request.headers.get('X-User-ID')
    if error := validate_user_id(user_id):
        return error

    with get_conn() as conn:
        with conn.cursor() as cur:
            if request.method == 'GET':
                cur.execute("""
                    SELECT numero_clt, nom, solde, reference, contact, adresse 
                    FROM client WHERE user_id = %s ORDER BY nom
                """, (user_id,))
                
                clients = [{
                    'id': row[0],
                    'nom': row[1],
                    'solde': float_to_str(row[2]),
                    'reference': row[3],
                    'contact': row[4] or '',
                    'adresse': row[5] or ''
                } for row in cur.fetchall()]
                
                return jsonify(clients)

            elif request.method == 'POST':
                data = request.get_json()
                required = ['nom', 'solde', 'reference']
                if not all(k in data for k in required):
                    return jsonify({'error': 'Champs manquants'}), 400

                cur.execute("""
                    INSERT INTO client 
                    (nom, solde, reference, contact, adresse, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING numero_clt
                """, (
                    data['nom'],
                    float(data['solde']),
                    data['reference'],
                    data.get('contact'),
                    data.get('adresse'),
                    user_id
                ))
                
                client_id = cur.fetchone()[0]
                conn.commit()
                return jsonify({'id': client_id}), 201

# --- Produits ---
@app.route('/produits', methods=['GET', 'POST'])
def gestion_produits():
    user_id = request.headers.get('X-User-ID')
    if error := validate_user_id(user_id):
        return error

    with get_conn() as conn:
        with conn.cursor() as cur:
            if request.method == 'GET':
                cur.execute("""
                    SELECT bar, designation, prix, qte, prixba 
                    FROM item WHERE user_id = %s ORDER BY designation
                """, (user_id,))
                
                produits = [{
                    'code': row[0],
                    'designation': row[1],
                    'prix': float_to_str(row[2]),
                    'quantite': row[3],
                    'prix_achat': float_to_str(row[4])
                } for row in cur.fetchall()]
                
                return jsonify(produits)

            elif request.method == 'POST':
                data = request.get_json()
                required = ['code', 'designation', 'prix', 'quantite']
                if not all(k in data for k in required):
                    return jsonify({'error': 'Champs manquants'}), 400

                # Vérification doublon
                cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", 
                          (data['code'], user_id))
                if cur.fetchone():
                    return jsonify({'error': 'Produit existe déjà'}), 409

                cur.execute("""
                    INSERT INTO item 
                    (bar, designation, prix, qte, prixba, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    data['code'],
                    data['designation'],
                    float(data['prix']),
                    int(data['quantite']),
                    float(data.get('prix_achat', 0)),
                    user_id
                ))
                
                conn.commit()
                return jsonify({'status': 'created'}), 201

# --- Ventes ---
@app.route('/ventes', methods=['GET', 'POST'])
def gestion_ventes():
    user_id = request.headers.get('X-User-ID')
    if error := validate_user_id(user_id):
        return error

    with get_conn() as conn:
        with conn.cursor() as cur:
            if request.method == 'GET':
                cur.execute("""
                    SELECT a.numero_attache, a.numero_comande, 
                           i.designation, a.numero_item, a.quantite,
                           a.prixt, a.remarque, a.prixbh, a.send
                    FROM attache a
                    JOIN item i ON a.numero_item = i.bar
                    WHERE a.user_id = %s
                    ORDER BY a.numero_comande DESC
                """, (user_id,))
                
                ventes = [{
                    'id': row[0],
                    'commande_id': row[1],
                    'produit': row[2],
                    'code_produit': row[3],
                    'quantite': row[4],
                    'prix_total': float_to_str(row[5]),
                    'remarque': row[6] or '',
                    'prix_achat': float_to_str(row[7]),
                    'envoye': row[8]
                } for row in cur.fetchall()]
                
                return jsonify(ventes)

            elif request.method == 'POST':
                data = request.get_json()
                required = ['produit_id', 'quantite', 'prix', 'client_id']
                if not all(k in data for k in required):
                    return jsonify({'error': 'Champs manquants'}), 400

                # Vérification stock
                cur.execute("""
                    SELECT qte, prix, prixba FROM item 
                    WHERE bar = %s AND user_id = %s FOR UPDATE
                """, (data['produit_id'], user_id))
                
                produit = cur.fetchone()
                if not produit:
                    return jsonify({'error': 'Produit introuvable'}), 404
                
                if produit[0] < data['quantite']:
                    return jsonify({'error': 'Stock insuffisant'}), 400

                # Création commande
                cur.execute("""
                    INSERT INTO comande 
                    (numero_table, date_comande, etat_c, user_id)
                    VALUES (%s, NOW(), 'en_cours', %s)
                    RETURNING numero_comande
                """, (data['client_id'], user_id))
                
                commande_id = cur.fetchone()[0]

                # Enregistrement vente
                cur.execute("""
                    INSERT INTO attache 
                    (numero_item, quantite, prixt, remarque, 
                     send, numero_comande, prixbh, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING numero_attache
                """, (
                    data['produit_id'],
                    data['quantite'],
                    float(data['prix']),
                    data.get('remarque', ''),
                    False,
                    commande_id,
                    produit[2] or 0,
                    user_id
                ))

                # Mise à jour stock
                cur.execute("""
                    UPDATE item SET qte = qte - %s 
                    WHERE bar = %s AND user_id = %s
                """, (data['quantite'], data['produit_id'], user_id))

                conn.commit()
                return jsonify({
                    'commande_id': commande_id,
                    'status': 'Vente enregistrée'
                }), 201

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=os.getenv('DEBUG', 'False') == 'True')