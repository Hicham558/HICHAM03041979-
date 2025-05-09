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
    # Récupérer l'ID de l'utilisateur depuis l'en-tête
    user_id = request.headers.get('X-User-ID')
    if not user_id:
        return jsonify({'erreur': 'Utilisateur non authentifié'}), 401

    # Récupérer les données JSON de la requête
    data = request.get_json()

    # Vérifier que tous les champs requis sont présents
    required_fields = ['client_id', 'produit_bar', 'quantite', 'prixt', 'remarque', 'prixbh', 'numero_util', 'etat_c', 'nature']
    for field in required_fields:
        if field not in data:
            return jsonify({'erreur': f'Champ {field} manquant'}), 400

    conn = None
    try:
        # Convertir les données en types appropriés
        client_id = data['client_id']  # Peut être '0' pour vente comptoir
        produit_bar = data['produit_bar']
        quantite = int(data['quantite'])
        prixt = float(data['prixt'])
        remarque = float(data['remarque'])  # Prix unitaire
        prixbh = float(data['prixbh'])      # Prix d'achat
        numero_util = data['numero_util']
        etat_c = data['etat_c']
        nature = data['nature']

        # Connexion à la base de données PostgreSQL
        conn = get_conn()
        cursor = conn.cursor()

        # Vérifier si le produit existe et récupérer le stock
        cursor.execute("SELECT QTE FROM item WHERE BAR = %s", (produit_bar,))
        product = cursor.fetchone()
        if not product:
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        stock = product[0]
        if quantite > stock:
            conn.close()
            return jsonify({'erreur': 'Stock insuffisant'}), 400

        # Insérer la vente dans la table comande
        cursor.execute('''
            INSERT INTO comande (numero_table, numero_item, quantite, prixt, remarque, prixbh, numero_util, etat_c, nature)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (client_id, produit_bar, quantite, prixt, remarque, prixbh, numero_util, etat_c, nature))

        # Mettre à jour le stock dans la table item
        new_stock = stock - quantite
        cursor.execute('UPDATE item SET QTE = %s WHERE BAR = %s', (new_stock, produit_bar))

        # Mettre à jour le solde du client uniquement s'il n'est pas '0' (vente comptoir)
        if client_id != '0':
            cursor.execute('UPDATE client SET solde = solde + %s WHERE numero_clt = %s', (prixt, client_id))
            if cursor.rowcount == 0:
                conn.rollback()
                conn.close()
                return jsonify({'erreur': 'Client introuvable'}), 400

        # Valider la transaction
        conn.commit()
        conn.close()

        return jsonify({'message': 'Vente ajoutée avec succès'}), 200

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': f'Erreur PostgreSQL: {str(e)}'}), 500
    except ValueError as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': f'Valeur invalide: {str(e)}'}), 400
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({'erreur': f'Erreur inattendue: {str(e)}'}), 500



# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
