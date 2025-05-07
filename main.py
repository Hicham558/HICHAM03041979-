from flask import Flask, request, jsonify,CORS,
import psycopg2
import os

app = Flask(__name__)
CORS(app)
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
        return 'API en ligne - Connexion posgresql OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}'

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')

    if not all([nom, solde, reference]):
        return jsonify({'erreur': 'Champs manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO client (nom, solde, reference) VALUES (%s, %s, %s)", (nom, solde, reference))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté'})
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_item, bar, designation, qte, prix FROM item")
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

@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_clt, nom, solde, reference FROM client")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Convertir en liste de dictionnaires
        clients = []
        for row in rows:
            clients.append({
                'numero_clt': row[0],
                'nom': row[1],
                'solde': row[2],
                'reference': row[3]
            })

        return jsonify(clients)

    except Exception as e:
        return jsonify({'erreur': str(e)}), 500
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
