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


# --- Commandes ---
@app.route('/creer_comande', methods=['POST'])
def creer_comande():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_table = data.get('numero_table')
    date_comande = data.get('date_comande')
    etat_c = data.get('etat_c', 'en_cours')
    nature = data.get('nature', 'vente')

    if not all([numero_table is not None, date_comande]):
        return jsonify({'erreur': 'Champs obligatoires manquants (numero_table, date_comande)'}), 400

    try:
        conn = get_conn()
        cursor = conn.cursor()
        query = "INSERT INTO comande (numero_table, date_comande, etat_c, nature, user_id) VALUES (%s, %s, %s, %s, %s) RETURNING id"
        cursor.execute(query, (numero_table, date_comande, etat_c, nature, user_id))
        numero_comande = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'numero_comande': numero_comande}), 200
    except psycopg2.Error as e:
        return jsonify({'erreur': f'Erreur PostgreSQL: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'erreur': f'Erreur inattendue: {str(e)}'}), 500

# --- Lignes de vente ---
@app.route('/ajouter_attache', methods=['POST'])
def ajouter_attache():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    data = request.get_json()
    numero_comande = data.get('numero_comande')
    produit_bar = data.get('produit_bar')
    quantite = data.get('quantite')
    prixt = data.get('prixt')
    remarque = data.get('remarque', '')
    prixbh = data.get('prixbh')

    if not all([numero_comande, produit_bar, quantite is not None, prixt is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants'}), 400

    try:
        quantite = int(quantite)
        prixt = float(prixt)
        prixbh = float(prixbh) if prixbh is not None else 0.0
        if quantite <= 0 or prixt < 0:
            return jsonify({'erreur': 'Quantité doit être positive et prix non négatif'}), 400

        conn = get_conn()
        cursor = conn.cursor()

        # Vérifier si le produit existe
        cursor.execute("SELECT prixba FROM item WHERE bar = %s AND user_id = %s", (produit_bar, user_id))
        result = cursor.fetchone()
        if not result:
            cursor.close()
            conn.close()
            return jsonify({'erreur': 'Produit non trouvé'}), 404

        prixba = result[0]
        if prixbh is not None and str(prixbh) != prixba:
            cursor.close()
            conn.close()
            return jsonify({'erreur': 'prixbh doit correspondre à prixba du produit'}), 400

        # Insérer la ligne dans attache
        query = """
            INSERT INTO attache (numero_comande, user_id, produit_bar, quantite, prixt, remarque, prixbh)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (numero_comande, user_id, produit_bar, quantite, prixt, remarque, prixba))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Ligne ajoutée avec succès'}), 200
    except ValueError:
        return jsonify({'erreur': 'Quantité et prix doivent être des nombres valides'}), 400
    except psycopg2.Error as e:
        return jsonify({'erreur': f'Erreur PostgreSQL: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'erreur': f'Erreur inattendue: {str(e)}'}), 500

# --- Liste des ventes ---
@app.route('/liste_ventes', methods=['GET'])
def liste_ventes():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    try:
        conn = get_conn()
        cursor = conn.cursor()
        query = """
            SELECT c.id AS numero_comande, i.designation, i.bar AS produit_bar, a.quantite,
                   a.remarque, a.prixt, a.prixbh, c.date_comande
            FROM comande c
            JOIN attache a ON c.id = a.numero_comande
            JOIN item i ON a.produit_bar = i.bar
            WHERE c.user_id = %s AND c.nature = 'vente'
            ORDER BY c.date_comande DESC
        """
        cursor.execute(query, (user_id,))
        ventes = cursor.fetchall()

        ventes_list = [
            {
                'numero_comande': vente[0],
                'designation': vente[1],
                'produit_bar': vente[2],
                'quantite': vente[3],
                'remarque': vente[4] or '',
                'prixt': float(vente[5]) if vente[5] is not None else 0.0,
                'prixbh': float(vente[6]) if vente[6] is not None else 0.0,
                'date_comande': vente[7].isoformat() if vente[7] else ''
            }
            for vente in ventes
        ]

        cursor.close()
        conn.close()
        return jsonify(ventes_list), 200
    except psycopg2.Error as e:
        return jsonify({'erreur': f'Erreur PostgreSQL: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'erreur': f'Erreur inattendue: {str(e)}'}), 500
# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
