from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os 

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])
app.debug = True  # Pour voir les erreurs

def get_conn():
    url = os.environ['DATABASE_URL']
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode='require')

@app.route('/')
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}'

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')
    contact = data.get('contact')
    adresse = data.get('adresse')
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête

    if not all([nom, solde, reference, user_id]):  # Vérifier que user_id est requis
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, solde, reference, user_id)'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (nom, solde, reference, contact, adresse, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté'})
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        clients = []
        for row in rows:
            clients.append({
                'numero_clt': row[0],
                'nom': row[1],
                'solde': row[2],
                'reference': row[3],
                'contact': row[4],
                'adresse': row[5]
            })

        return jsonify(clients)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête

    if not all([designation, bar, prix, qte, user_id]):  # Vérifier que user_id est requis
        return jsonify({'erreur': 'Champs manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO item (designation, bar, prix, qte, user_id) VALUES (%s, %s, %s, %s, %s)",
                    (designation, bar, prix, qte, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Item ajouté'})
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_item, bar, designation, qte, prix FROM item WHERE user_id = %s", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        produits = []
        for row in rows:
            produits.append({
                'NUMERO_ITEM': row[0],
                'BAR': row[1],
                'DESIGNATION': row[2],
                'QTE': row[3],
                'PRIX': row[4]
            })

        return jsonify(produits)
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
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête

    if not all([nom, solde, reference, user_id]):  # Vérifier que user_id est requis
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, solde, reference, user_id)'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (nom, solde, reference, contact, adresse, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur ajouté'})
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = request.headers.get('X-User-ID')  # Récupérer l'identifiant utilisateur depuis l'en-tête
    if not user_id:
        return jsonify({'erreur': 'Identifiant utilisateur requis'}), 401

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s", (user_id,))
        fournisseurs = []
        colonnes = [desc[0] for desc in cur.description]
        for row in cur.fetchall():
            fournisseurs.append(dict(zip(colonnes, row)))
        cur.close()
        conn.close()
        return jsonify(fournisseurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))