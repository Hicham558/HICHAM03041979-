from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])  # Autoriser les requêtes depuis ton front-end
app.debug = True  # Activer le mode debug pour voir les erreurs

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

# Route pour vérifier que l'API est en ligne
@app.route('/', methods=['GET'])
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}', 500

# --- Clients ---
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):  # Si validate_user_id retourne une erreur
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_clt, nom, solde, reference, contact, adresse FROM client WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        clients = [
            {
                'numero_clt': row[0],
                'nom': row[1],
                'solde': float(row[2]) if row[2] is not None else 0.0,
                'reference': row[3],
                'contact': row[4] or '',
                'adresse': row[5] or ''
            }
            for row in rows
        ]
        return jsonify(clients)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_client', methods=['POST'])
def ajouter_client():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')
    contact = data.get('contact')
    adresse = data.get('adresse')

    # Validation des champs obligatoires
    if not all([nom, solde is not None, reference]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, solde, reference)'}), 400

    try:
        solde = float(solde)  # Convertir en float
        if solde < 0:
            return jsonify({'erreur': 'Le solde doit être positif'}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO client (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_clt",
            (nom, solde, reference, contact, adresse, user_id)
        )
        client_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Client ajouté', 'id': client_id}), 201
    except ValueError:
        return jsonify({'erreur': 'Le solde doit être un nombre valide'}), 400
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# --- Fournisseurs ---
@app.route('/liste_fournisseurs', methods=['GET'])
def liste_fournisseurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_fou, nom, solde, reference, contact, adresse FROM fournisseur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        fournisseurs = [
            {
                'numero_fou': row[0],
                'nom': row[1],
                'solde': float(row[2]) if row[2] is not None else 0.0,
                'reference': row[3],
                'contact': row[4] or '',
                'adresse': row[5] or ''
            }
            for row in rows
        ]
        return jsonify(fournisseurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_fournisseur', methods=['POST'])
def ajouter_fournisseur():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    nom = data.get('nom')
    solde = data.get('solde')
    reference = data.get('reference')
    contact = data.get('contact')
    adresse = data.get('adresse')

    if not all([nom, solde is not None, reference]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, solde, reference)'}), 400

    try:
        solde = float(solde)
        if solde < 0:
            return jsonify({'erreur': 'Le solde doit être positif'}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO fournisseur (nom, solde, reference, contact, adresse, user_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING numero_fou",
            (nom, solde, reference, contact, adresse, user_id)
        )
        fournisseur_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Fournisseur ajouté', 'id': fournisseur_id}), 201
    except ValueError:
        return jsonify({'erreur': 'Le solde doit être un nombre valide'}), 400
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# --- Produits ---
@app.route('/liste_produits', methods=['GET'])
def liste_produits():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_item, bar, designation, qte, prix, prixba FROM item WHERE user_id = %s ORDER BY designation", (user_id,))
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
                'PRIXBA': row[5] or '0.00'  # prixba est VARCHAR, donc on retourne une chaîne
            }
            for row in rows
        ]
        return jsonify(produits)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

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
    prixba = data.get('prixba')  # Prix d'achat (VARCHAR)

    if not all([designation, bar, prix is not None, qte is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, bar, prix, qte)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        # Vérifier si le code-barres existe déjà
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Ce code-barres existe déjà'}), 409

        # Insérer le produit avec prixba
        cur.execute(
            "INSERT INTO item (designation, bar, prix, qte, prixba, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (designation, bar, prix, qte, prixba or '0.00', user_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Item ajouté'}), 201
    except ValueError:
        return jsonify({'erreur': 'Le prix et la quantité doivent être des nombres valides'}), 400
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# --- Ventes ---
@app.route('/liste_ventes', methods=['GET'])
def liste_ventes():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT a.numero_attache, a.numero_comande, i.designation, a.quantite, a.prixt, a.remarque, a.prixbh, a.send
            FROM attache a
            JOIN item i ON a.numero_item = i.bar
            WHERE a.user_id = %s
            ORDER BY a.numero_comande DESC
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        ventes = [
            {
                'numero_attache': row[0],
                'numero_comande': row[1],
                'designation': row[2],
                'quantite': row[3],
                'prixt': float(row[4]) if row[4] is not None else 0.0,
                'remarque': row[5] or '',
                'prixbh': float(row[6]) if row[6] is not None else 0.0,
                'send': row[7]
            }
            for row in rows
        ]
        return jsonify(ventes)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_vente', methods=['POST'])
def ajouter_vente():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    client_id = data.get('client_id')
    produit_bar = data.get('produit_bar')
    quantite = data.get('quantite')
    prixt = data.get('prixt')
    remarque = data.get('remarque')
    prixbh = data.get('prixbh')
    numero_util = data.get('numero_util')
    etat_c = data.get('etat_c', 'en_cours')  # Valeur par défaut
    nature = data.get('nature')

    if not all([produit_bar, quantite is not None, prixt is not None, remarque is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants (produit_bar, quantite, prixt, remarque)'}), 400

    try:
        quantite = int(quantite)
        prixt = float(prixt)
        prixbh = float(prixbh) if prixbh is not None else 0.0
        if quantite <= 0 or prixt < 0:
            return jsonify({'erreur': 'La quantité doit être positive et le prix non négatif'}), 400

        conn = get_conn()
        cur = conn.cursor()

        # Vérifier si le produit existe et si la quantité est suffisante
        cur.execute("SELECT qte, prix FROM item WHERE bar = %s AND user_id = %s", (produit_bar, user_id))
        produit = cur.fetchone()
        if not produit:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Produit introuvable'}), 404
        if produit[0] < quantite:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Quantité insuffisante en stock'}), 400

        # Vérifier si le client existe (si spécifié)
        if client_id:
            cur.execute("SELECT numero_clt FROM client WHERE numero_clt = %s AND user_id = %s", (client_id, user_id))
            if not cur.fetchone():
                cur.close()
                conn.close()
                return jsonify({'erreur': 'Client introuvable'}), 404

        # Créer une nouvelle commande
        cur.execute(
            "INSERT INTO comande (numero_table, date_comande, etat_c, numero_util, nature, user_id) "
            "VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s) RETURNING numero_comande",
            (client_id, etat_c, numero_util, nature, user_id)
        )
        numero_comande = cur.fetchone()[0]

        # Ajouter la ligne de vente (attache)
        cur.execute(
            "INSERT INTO attache (numero_item, quantite, prixt, remarque, send, numero_comande, prixbh, user_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING numero_attache",
            (produit_bar, quantite, prixt, remarque, False, numero_comande, prixbh, user_id)
        )

        # Mettre à jour le stock
        cur.execute("UPDATE item SET qte = qte - %s WHERE bar = %s AND user_id = %s", (quantite, produit_bar, user_id))

        # Mettre à jour le solde du client (si spécifié)
        if client_id:
            cur.execute("UPDATE client SET solde = solde + %s WHERE numero_clt = %s AND user_id = %s", (prixt, client_id, user_id))

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Vente enregistrée', 'numero_comande': numero_comande}), 201
    except ValueError:
        return jsonify({'erreur': 'Les champs quantité et prix doivent être des nombres valides'}), 400
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))