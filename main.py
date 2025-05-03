from flask import Flask, request, jsonify
import psycopg2
import os

app = Flask(__name__)
app.debug = True  # Pour voir les erreurs

def get_conn():
    return psycopg2.connect(os.environ['postgresql://postgres:xKPAiwKoPilwkcgVKECAteEefoDIWIwu@shuttle.proxy.rlwy.net:31746/railway'])

@app.route('/')
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion DB OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}'

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    rin = data.get('rin')

    if not all([nom, solde, rin]):
        return jsonify({'erreur': 'Champs manquants'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO client (nom, solde, rin) VALUES (%s, %s, %s)", (nom, solde, rin))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté'})
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
