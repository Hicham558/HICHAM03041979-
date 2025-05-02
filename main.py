from flask import Flask, request, jsonify
import psycopg2
import os

app = Flask(__KHELLADIHICHAMAPI__)

DATABASE_URL = os.environ.get("postgresql://postgres:xKPAiwKoPilwkcgVKECAteEefoDIWIwu@shuttle.proxy.rlwy.net:31746/railway")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

@app.route('/')
def index():
    return 'API Clients opérationnelle'

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    data = request.get_json()
    numero = data.get('numero_clt')
    nom = data.get('nom')
    solde = data.get('solde')
    rin = data.get('rin')

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO client (numero_clt, nom, solde, ref)
        VALUES (%s, %s, %s, %s)
    """, (numero_clt, nom, solde, ref))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "Client ajouté avec succès"})

@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT numero_clt, nom, solde, ref FROM client")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    clients = []
    for row in rows:
        clients.append({
            "numero_clt": row[0],
            "nom": row[1],
            "solde": row[2],
            "ref": row[3]
        })

    return jsonify(clients)

if __name__ == '__KHELLADIHICHAMAPI__':
    app.run(debug=True, port=31746)
