from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os


app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])


# Connexion à la base de données
def get_conn():
    return psycopg2.connect(
        dbname=os.getenv('PG_DBNAME', 'your_db_name'),
        user=os.getenv('PG_USER', 'your_db_user'),
        password=os.getenv('PG_PASSWORD', 'your_db_password'),
        host=os.getenv('PG_HOST', 'your_db_host'),
        port=os.getenv('PG_PORT', '5432')
    )

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

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT numero_clt, nom, solde, reference, contact, adresse
        FROM client
        WHERE user_id = %s
        ORDER BY nom
    """, (user_id,))
    rows = cur.fetchall()
    clients = [{
        'numero_clt': row[0],
        'nom': row[1],
        'solde': str(row[2]),
        'reference': row[3],
        'contact': row[4],
        'adresse': row[5]
    } for row in rows]
    cur.close()
    conn.close()
    return jsonify(clients)

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO client (nom, solde, reference, contact, adresse, user_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING numero_clt
        """,
        (data['nom'], float(data['solde']), data['reference'], 
         data.get('contact'), data.get('adresse'), user_id)
    numero_clt = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'statut': 'Client ajouté', 'numero_clt': numero_clt})

# --- Gestion des fournisseurs ---
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT numero_fou, nom, solde, reference, contact, adresse
        FROM fournisseur
        WHERE user_id = %s
        ORDER BY nom
    """, (user_id,))
    rows = cur.fetchall()
    fournisseurs = [{
        'numero_fou': row[0],
        'nom': row[1],
        'solde': str(row[2]),
        'reference': row[3],
        'contact': row[4],
        'adresse': row[5]
    } for row in rows]
    cur.close()
    conn.close()
    return jsonify(fournisseurs)

# --- Gestion des produits ---
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT bar, designation, prix, qte, prixba
        FROM item
        WHERE user_id = %s
        ORDER BY designation
    """, (user_id,))
    rows = cur.fetchall()
    produits = [{
        'BAR': row[0],
        'DESIGNATION': row[1],
        'PRIX': str(row[2]),
        'QTE': row[3],
        'PRIXBA': str(row[4]) if row[4] is not None else '0.00'
    } for row in rows]
    cur.close()
    conn.close()
    return jsonify(produits)

# --- Gestion des ventes ---
@app.route('/liste_ventes', methods=['GET'])
def liste_ventes():
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

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
    ventes = [{
        'numero_attache': row[0],
        'numero_comande': row[1],
        'designation': row[2],
        'numero_item': row[3],
        'quantite': row[4],
        'prixt': str(row[5]),
        'remarque': row[6],
        'prixbh': str(row[7]) if row[7] is not None else '0.00',
        'send': row[8]
    } for row in rows]
    cur.close()
    conn.close()
    return jsonify(ventes)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)