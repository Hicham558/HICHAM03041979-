from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import json
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime,timedelta

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



@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Retourne l'erreur 401 si user_id est invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de vente invalides, utilisateur ou mot de passe manquant")
        return jsonify({"error": "Données de vente invalides, utilisateur ou mot de passe manquant"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')  # Par défaut "espece"
    amount_paid = float(data.get('amount_paid', 0))  # Montant versé, 0 par défaut
    lignes = data['lignes']
    numero_util = data['numero_util']
    password2 = data['password2']
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    # Validation du mode de paiement et du client
    if payment_mode == 'a_terme' and numero_table == 0:
        print("Erreur: Vente à terme sans client sélectionné")
        return jsonify({"error": "Veuillez sélectionner un client pour une vente à terme"}), 400

    if payment_mode == 'a_terme' and amount_paid < 0:
        print("Erreur: Montant versé négatif")
        return jsonify({"error": "Le montant versé ne peut pas être négatif"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("""
            SELECT Password2
            FROM utilisateur
            WHERE numero_util = %s
        """, (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Récupérer le dernier compteur pour cette nature
        cur.execute("""
            SELECT COALESCE(MAX(compteur), 0) as max_compteur
            FROM comande
            WHERE nature = %s
        """, (nature,))
        compteur = cur.fetchone()['max_compteur'] + 1
        print(f"Compteur calculé: nature={nature}, compteur={compteur}")

        # Insérer la commande avec numero_util
        cur.execute("""
            INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id, numero_util)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_comande
        """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id, numero_util))
        numero_comande = cur.fetchone()['numero_comande']
        print(f"Commande insérée: numero_comande={numero_comande}, nature={nature}, connection1=-1, compteur={compteur}, numero_util={numero_util}")

        # Insérer les lignes et mettre à jour le stock
        for ligne in lignes:
            cur.execute("""
                INSERT INTO attache (user_id, numero_comande, numero_item, quantite, prixt, remarque, prixbh, achatfx)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id,
                  numero_comande,
                  ligne.get('numero_item'),
                  ligne.get('quantite'),
                  ligne.get('prixt'),
                  ligne.get('remarque'),
                  ligne.get('prixbh'),
                  0))
            cur.execute("UPDATE item SET qte = qte - %s WHERE numero_item = %s", (ligne.get('quantite'), ligne.get('numero_item')))

        # Mise à jour du solde du client si vente à terme
        if payment_mode == 'a_terme' and numero_table != 0:
            total_sale = sum(float(ligne.get('prixt', 0)) for ligne in lignes)
            solde_change = amount_paid - total_sale  # Dette = montant versé - total (négatif si dette)

            # Récupérer le solde actuel du client
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s", (numero_table,))
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client avec numero_clt={numero_table} non trouvé")

            # Convertir le solde (VARCHAR) en float, ou 0 si vide/invalide
            current_solde = float(client['solde']) if client['solde'] and client['solde'].strip() else 0.0
            new_solde = current_solde + solde_change  # Soustraire la dette (solde_change est négatif)

            # Convertir le nouveau solde en chaîne avec 2 décimales
            new_solde_str = f"{new_solde:.2f}"

            # Mettre à jour le solde dans la table client
            cur.execute("""
                UPDATE client
                SET solde = %s
                WHERE numero_clt = %s
            """, (new_solde_str, numero_table))
            print(f"Solde client mis à jour: numero_clt={numero_table}, solde_change={solde_change}, amount_paid={amount_paid}, new_solde={new_solde_str}")

        conn.commit()
        print(f"Vente validée: numero_comande={numero_comande}, {len(lignes)} lignes")
        return jsonify({"numero_comande": numero_comande}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur validation vente: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/client_solde', methods=['GET'])
def client_solde():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT numero_clt, COALESCE(solde, '0.00') as solde
            FROM client
            WHERE user_id = %s
        """, (user_id,))
        soldes = cur.fetchall()
        print(f"Soldes récupérés: {len(soldes)} clients")
        return jsonify(soldes), 200
    except Exception as e:
        print(f"Erreur récupération soldes: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                c.numero_comande,
                c.date_comande,
                c.nature,
                c.numero_table,
                cl.nom AS client_nom,
                c.numero_util,
                u.nom AS utilisateur_nom,
                a.numero_item,
                a.quantite,
                CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) AS prixt,
                a.remarque,
                i.designation
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            LEFT JOIN utilisateur u ON c.numero_util = u.numero_util
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        if numero_util:
            if numero_util == '0':
                pass
            else:
                query += " AND c.numero_util = %s"
                params.append(int(numero_util))

        query += " ORDER BY c.numero_comande DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        tickets = []
        bons = []
        total = 0.0
        ventes_map = {}

        for row in rows:
            if row['numero_comande'] not in ventes_map:
                ventes_map[row['numero_comande']] = {
                    'numero_comande': row['numero_comande'],
                    'date_comande': row['date_comande'].isoformat(),
                    'nature': row['nature'],
                    'client_nom': 'Comptoir' if row['numero_table'] == 0 else row['client_nom'],
                    'utilisateur_nom': row['utilisateur_nom'] or 'N/A',
                    'lignes': []
                }

            ventes_map[row['numero_comande']]['lignes'].append({
                'numero_item': row['numero_item'],
                'designation': row['designation'],
                'quantite': row['quantite'],
                'prixt': str(row['prixt']),  # Retourne en tant que chaîne pour respecter le type d'origine
                'remarque': row['remarque'] or ''
            })

            total += float(row['prixt'])  # Conversion en float pour le calcul

        for vente in ventes_map.values():
            if vente['nature'] == 'TICKET':
                tickets.append(vente)
            elif vente['nature'] == 'BON DE L.':
                bons.append(vente)

        cur.close()
        conn.close()

        return jsonify({
            'tickets': tickets,
            'bons': bons,
            'total': f"{total:.2f}"
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération ventes du jour: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Articles les plus vendus ---
@app.route('/articles_plus_vendus', methods=['GET'])
def articles_plus_vendus():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util')

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        # Construire la requête SQL
        query = """
            SELECT 
                i.numero_item,
                i.designation,
                SUM(a.quantite) AS quantite,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)) AS total_vente
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        # Filtre par client
        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        # Filtre par utilisateur
        if numero_util and numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(int(numero_util))

        query += """
            GROUP BY i.numero_item, i.designation
            ORDER BY quantite DESC
            LIMIT 10
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        # Formater la réponse
        articles = [
            {
                'numero_item': row['numero_item'],
                'designation': row['designation'] or 'N/A',
                'quantite': int(row['quantite'] or 0),
                'total_vente': f"{float(row['total_vente'] or 0):.2f}"
            }
            for row in rows
        ]

        return jsonify(articles), 200

    except Exception as e:
        print(f"Erreur récupération articles plus vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')
    numero_util = request.args.get('numero_util', '0')  # Par défaut : Tous les utilisateurs

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates (30 jours si aucune date spécifique)
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = date_end - timedelta(days=30)

        # Construire la requête SQL
        query = """
            SELECT 
                DATE(c.date_comande) AS date,
                SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0') AS FLOAT))) AS profit
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s 
            AND c.date_comande >= %s 
            AND c.date_comande <= %s
        """
        params = [user_id, date_start, date_end]

        # Filtre par client
        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        # Filtre par utilisateur : uniquement si numero_util n'est pas '0'
        if numero_util != '0':
            query += " AND c.numero_util = %s"
            params.append(int(numero_util))

        query += """
            GROUP BY DATE(c.date_comande)
            ORDER BY DATE(c.date_comande) DESC
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        # Formater la réponse
        profits = [
            {
                'date': row['date'].strftime('%Y-%m-%d'),
                'profit': f"{float(row['profit'] or 0):.2f}"
            }
            for row in rows
        ]

        return jsonify(profits), 200

    except Exception as e:
        print(f"Erreur récupération profit par date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/dashboard', methods=['GET'])
def dashboard():
    userId = validate_user_id()
    if not isinstance(userId, str):
        return userId

    period = request.args.get('period', 'day')
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Define the date range
        if period == 'week':
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        # Query for main KPIs
        query_kpi = """
            SELECT 
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS total_ca,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT) - 
                    (a.quantite * CAST(COALESCE(NULLIF(i.prixba, ''), '0') AS FLOAT))), 0) AS total_profit,
                COUNT(DISTINCT c.numero_comande) AS sales_count
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
        """
        cur.execute(query_kpi, (userId, date_start, date_end))
        kpi_data = cur.fetchone()

        # Query for low stock items
        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE user_id = %s AND qte < 10", (userId,))
        low_stock_count = cur.fetchone()['low_stock']

        # Query for top client
        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS client_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY cl.nom
            ORDER BY client_ca DESC
            LIMIT 1
        """
        cur.execute(query_top_client, (userId, date_start, date_end))
        top_client = cur.fetchone()

        # Query for chart data (CA per day)
        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(CAST(COALESCE(NULLIF(a.prixt, ''), '0') AS FLOAT)), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        cur.execute(query_chart, (userId, date_start, date_end))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

        # Format chart data
        chart_labels = []
        chart_values = []
        current_date = date_start
        while current_date <= date_end:
            chart_labels.append(current_date.strftime('%Y-%m-%d'))
            daily_ca = next((row['daily_ca'] for row in chart_data if row['sale_date'].strftime('%Y-%m-%d') == current_date.strftime('%Y-%m-%d')), 0)
            chart_values.append(float(daily_ca))
            current_date += timedelta(days=1)

        return jsonify({
            'total_ca': float(kpi_data['total_ca'] or 0),
            'total_profit': float(kpi_data['total_profit'] or 0),
            'sales_count': int(kpi_data['sales_count'] or 0),
            'low_stock_items': int(low_stock_count or 0),
            'top_client': {
                'name': top_client['nom'] if top_client else 'N/A',
                'ca': float(top_client['client_ca'] or 0) if top_client else 0
            },
            'chart_data': {
                'labels': chart_labels,
                'values': chart_values
            }
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération KPI: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
# GET /liste_utilisateurs
@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_util, nom, statue FROM utilisateur ORDER BY nom")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        utilisateurs = [
            {
                'numero_util': row[0],
                'nom': row[1],
                'statue': row[2]
            }
            for row in rows
        ]
        return jsonify(utilisateurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# POST /ajouter_utilisateur
@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    data = request.get_json()
    nom = data.get('nom')
    password2 = data.get('password2')
    statue = data.get('statue')

    if not all([nom, password2, statue]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password2, statue)'}), 400

    if statue not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statue invalide (doit être "admin" ou "emplo")'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO utilisateur (nom, password2, statue) VALUES (%s, %s, %s) RETURNING numero_util",
            (nom, password2, statue)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur ajouté', 'id': user_id}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# DELETE /supprimer_utilisateur/<numero_util>
@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM utilisateur WHERE numero_util = %s", (numero_util,))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 404
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur supprimé'}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/stock_value', methods=['GET'])
def valeur_stock():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                SUM(COALESCE(CAST(NULLIF(prixba, '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_achat,
                SUM(COALESCE(CAST(NULLIF(prix, '') AS FLOAT), 0) * COALESCE(qte, 0)) AS valeur_vente
            FROM item 
            WHERE user_id = %s
        """, (user_id,))
        result = cur.fetchone()

        # Vérifier si result est None (aucune donnée)
        if result is None:
            valeur_achat = 0.0
            valeur_vente = 0.0
        else:
            valeur_achat = float(result['valeur_achat'] or 0)
            valeur_vente = float(result['valeur_vente'] or 0)

        zakat = valeur_vente * 0.025  # 2.5% de la valeur de vente

        return jsonify({
            'valeur_achat': f"{valeur_achat:.2f}",
            'valeur_vente': f"{valeur_vente:.2f}",
            'zakat': f"{zakat:.2f}"
        }), 200
    except Exception as e:
        print(f"Erreur récupération valeur stock: {str(e)}")
        return jsonify({'erreur': str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/valider_reception', methods=['POST'])
def valider_reception():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes'] or 'numero_four' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de réception invalides")
        return jsonify({"error": "Données de réception invalides, fournisseur, utilisateur ou mot de passe manquant"}), 400

    numero_four = data.get('numero_four')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')
    lignes = data['lignes']
    nature = "Bon de réception"

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier le fournisseur
        cur.execute("SELECT numero_fou FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        if not cur.fetchone():
            print(f"Erreur: Fournisseur {numero_four} non trouvé")
            return jsonify({"error": "Fournisseur non trouvé"}), 400

        # Insérer le mouvement principal
        cur.execute("""
            INSERT INTO mouvement (date_m, etat_m, numero_four, refdoc, vers, nature, connection1, numero_util, cheque, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mouvement
        """, (datetime.utcnow(), "clôture", numero_four, "", "", nature, 0, numero_util, "", user_id))
        numero_mouvement = cur.fetchone()['numero_mouvement']

        # Mettre à jour refdoc
        cur.execute("UPDATE mouvement SET refdoc = %s WHERE numero_mouvement = %s", 
                    (str(numero_mouvement), numero_mouvement))

        # Calculer le coût total
        total_cost = 0.0
        for ligne in lignes:
            numero_item = ligne.get('numero_item')
            qtea = float(ligne.get('qtea', 0))
            prixbh = float(ligne.get('prixbh', 0))

            if qtea <= 0:
                raise Exception("La quantité ajoutée doit être positive")

            # Vérifier l'article
            cur.execute("SELECT qte, prixba FROM item WHERE numero_item = %s AND user_id = %s", (numero_item, user_id))
            item = cur.fetchone()
            if not item:
                raise Exception(f"Article {numero_item} non trouvé")

            current_qte = float(item['qte'] or 0)
            prixba = float(item['prixba'] or 0)

            nqte = current_qte + qtea
            total_cost += qtea * prixbh

            # Insérer les détails dans ATTACHE2 (avec user_id ajouté)
            cur.execute("""
                INSERT INTO attache2 (numero_item, numero_mouvement, qtea, nqte, nprix, pump, send, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (numero_item, numero_mouvement, qtea, nqte, str(prixbh)[:30], str(prixba)[:30], True, user_id))

            # Mettre à jour le stock et le prix d'achat
            cur.execute("UPDATE item SET qte = %s, prixba = %s WHERE numero_item = %s AND user_id = %s", 
                        (nqte, str(prixbh), numero_item, user_id))

        # Mettre à jour le solde du fournisseur
        cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_four, user_id))
        fournisseur = cur.fetchone()
        if not fournisseur:
            raise Exception(f"Fournisseur {numero_four} non trouvé")

        current_solde = float(fournisseur['solde']) if fournisseur['solde'] else 0.0
        new_solde = current_solde - total_cost
        new_solde_str = f"{new_solde:.2f}"

        cur.execute("UPDATE fournisseur SET solde = %s WHERE numero_fou = %s AND user_id = %s", 
                    (new_solde_str, numero_four, user_id))
        print(f"Solde fournisseur mis à jour: numero_fou={numero_four}, total_cost={total_cost}, new_solde={new_solde_str}")

        conn.commit()
        print(f"Réception validée: numero_mouvement={numero_mouvement}, {len(lignes)} lignes")
        return jsonify({"numero_mouvement": numero_mouvement}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur validation réception: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cur.close()
            conn.close()
@app.route('/receptions_jour', methods=['GET'])
def receptions_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_util = request.args.get('numero_util')
    numero_four = request.args.get('numero_four', '')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                m.numero_mouvement,
                m.date_m,
                m.nature,
                m.numero_four,
                f.nom AS fournisseur_nom,
                m.numero_util,
                u.nom AS utilisateur_nom,
                a2.numero_item,
                a2.qtea,
                CAST(COALESCE(NULLIF(a2.nprix, ''), '0') AS FLOAT) AS nprix,
                i.designation
            FROM mouvement m
            LEFT JOIN fournisseur f ON m.numero_four = f.numero_fou
            LEFT JOIN utilisateur u ON m.numero_util = u.numero_util
            JOIN attache2 a2 ON m.numero_mouvement = a2.numero_mouvement
            JOIN item i ON a2.numero_item = i.numero_item
            WHERE m.user_id = %s 
            AND m.date_m >= %s 
            AND m.date_m <= %s
            AND m.nature = 'Bon de réception'
        """
        params = [user_id, date_start, date_end]

        if numero_util and numero_util != '0':
            query += " AND m.numero_util = %s"
            params.append(int(numero_util))
        if numero_four and numero_four != '':
            query += " AND m.numero_four = %s"
            params.append(numero_four)

        query += " ORDER BY m.numero_mouvement DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        receptions = []
        total = 0.0
        receptions_map = {}

        for row in rows:
            if row['numero_mouvement'] not in receptions_map:
                receptions_map[row['numero_mouvement']] = {
                    'numero_mouvement': row['numero_mouvement'],
                    'date_m': row['date_m'].isoformat(),
                    'nature': row['nature'],
                    'fournisseur_nom': row['fournisseur_nom'] or 'N/A',
                    'utilisateur_nom': row['utilisateur_nom'] or 'N/A',
                    'lignes': []
                }

            receptions_map[row['numero_mouvement']]['lignes'].append({
                'numero_item': row['numero_item'],
                'designation': row['designation'],
                'qtea': row['qtea'],
                'nprix': str(row['nprix']),
                'total_ligne': str(float(row['qtea']) * float(row['nprix']))
            })

            total += float(row['qtea']) * float(row['nprix'])

        receptions = list(receptions_map.values())

        cur.close()
        conn.close()

        return jsonify({
            'receptions': receptions,
            'total': f"{total:.2f}"
        }), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération réceptions: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Versements ---

@app.route('/ajouter_versement', methods=['POST'])
def ajouter_versement():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    data = request.get_json()
    if not data or 'type' not in data or 'numero_cf' not in data or 'montant' not in data or 'numero_util' not in data or 'password2' not in data:
        print("Erreur: Données de versement invalides")
        return jsonify({"error": "Type, numéro client/fournisseur, montant, utilisateur ou mot de passe manquant"}), 400

    type_versement = data.get('type')  # 'C' pour client, 'F' pour fournisseur
    numero_cf = data.get('numero_cf')
    montant = data.get('montant')
    justificatif = data.get('justificatif', '')
    numero_util = data.get('numero_util')
    password2 = data.get('password2')

    if type_versement not in ['C', 'F']:
        return jsonify({"error": "Type invalide (doit être 'C' ou 'F')"}), 400

    try:
        montant = float(montant)
        if montant == 0:
            return jsonify({"error": "Le montant ne peut pas être zéro"}), 400

        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Vérifier l'utilisateur et le mot de passe
        cur.execute("SELECT Password2 FROM utilisateur WHERE numero_util = %s", (numero_util,))
        utilisateur = cur.fetchone()
        if not utilisateur:
            print(f"Erreur: Utilisateur {numero_util} non trouvé")
            return jsonify({"error": "Utilisateur non trouvé"}), 400
        if utilisateur['password2'] != password2:
            print(f"Erreur: Mot de passe incorrect pour l'utilisateur {numero_util}")
            return jsonify({"error": "Mot de passe incorrect"}), 401

        # Vérifier client ou fournisseur
        if type_versement == 'C':
            cur.execute("SELECT solde FROM client WHERE numero_clt = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'client'
            id_column = 'numero_clt'
            origine = 'VERSEMENT C'
        else:  # 'F'
            cur.execute("SELECT solde FROM fournisseur WHERE numero_fou = %s AND user_id = %s", (numero_cf, user_id))
            entity = cur.fetchone()
            table = 'fournisseur'
            id_column = 'numero_fou'
            origine = 'VERSEMENT F'

        if not entity:
            print(f"Erreur: {'Client' if type_versement == 'C' else 'Fournisseur'} {numero_cf} non trouvé")
            return jsonify({"error": f"{'Client' if type_versement == 'C' else 'Fournisseur'} non trouvé"}), 400

        # Mettre à jour le solde
        current_solde = float(entity['solde'] or '0.0')
        new_solde = current_solde + montant  # Montant peut être positif ou négatif
        new_solde_str = f"{new_solde:.2f}"

        cur.execute(f"UPDATE {table} SET solde = %s WHERE {id_column} = %s AND user_id = %s",
                    (new_solde_str, numero_cf, user_id))

        # Insérer le versement dans MOUVEMENTC
        now = datetime.utcnow()  # Ex. 2025-05-15 02:12:00.123456
        cur.execute(
            """
            INSERT INTO MOUVEMENTC (date_mc, time_mc, montant, justificatif, numero_util, origine, cf, numero_cf, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_mc
            """,
            (now.date(), now, f"{montant:.2f}", justificatif,
             numero_util, origine, type_versement, numero_cf, user_id)
        )
        numero_mc = cur.fetchone()['numero_mc']

        conn.commit()
        print(f"Versement ajouté: numero_mc={numero_mc}, type={type_versement}, montant={montant}")
        return jsonify({"numero_mc": numero_mc, "statut": "Versement ajouté"}), 201

    except ValueError:
        return jsonify({"error": "Le montant doit être un nombre valide"}), 400
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erreur ajout versement: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route('/historique_versements', methods=['GET'])
def historique_versements():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    type_versement = request.args.get('type')  # 'C', 'F', ou vide pour tous

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates
        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT 
                mc.numero_mc,
                mc.date_mc,
                mc.montant,
                mc.justificatif,
                mc.cf,
                mc.numero_cf,
                mc.numero_util,
                COALESCE(cl.nom, f.nom) AS nom_cf,
                u.nom AS utilisateur_nom
            FROM MOUVEMENTC mc
            LEFT JOIN client cl ON mc.cf = 'C' AND mc.numero_cf = cl.numero_clt
            LEFT JOIN fournisseur f ON mc.cf = 'F' AND mc.numero_cf = f.numero_fou
            LEFT JOIN utilisateur u ON mc.numero_util = u.numero_util
            WHERE mc.user_id = %s
            AND mc.date_mc >= %s
            AND mc.date_mc <= %s
            AND mc.origine IN ('VERSEMENT C', 'VERSEMENT F')
        """
        params = [user_id, date_start, date_end]

        if type_versement in ['C', 'F']:
            query += " AND mc.cf = %s"
            params.append(type_versement)

        query += " ORDER BY mc.date_mc DESC, mc.time_mc DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        versements = [
            {
                'numero_mc': row['numero_mc'],
                'date_mc': row['date_mc'].strftime('%Y-%m-%d'),
                'montant': str(row['montant']),
                'justificatif': row['justificatif'] or '',
                'type': 'Client' if row['cf'] == 'C' else 'Fournisseur',
                'numero_cf': row['numero_cf'],
                'nom_cf': row['nom_cf'] or 'N/A',
                'utilisateur_nom': row['utilisateur_nom'] or 'N/A'
            }
            for row in rows
        ]

        cur.close()
        conn.close()
        return jsonify(versements), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération historique versements: {str(e)}")
        return jsonify({'erreur': str(e)}), 500





# GET /parametres
@app.route('/parametres', methods=['GET'])
def get_parametres():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT PARAM FROM tmp WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['param']:
            try:
                params = json.loads(row['param'])
                print(f"Paramètres récupérés pour user_id={user_id}: {params}")
                return jsonify(params), 200
            except json.JSONDecodeError as e:
                print(f"Erreur décodage JSON pour user_id={user_id}: {str(e)}")
                return jsonify({}), 200
        print(f"Aucun paramètre trouvé pour user_id={user_id}")
        return jsonify({}), 200
    except Exception as e:
        print(f"Erreur récupération paramètres pour user_id={user_id}: {str(e)}")
        return jsonify({"erreur": str(e)}), 500

# PUT /parametres
@app.route('/parametres', methods=['PUT'])
def update_parametres():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401
    data = request.get_json()
    if not data:
        print("Erreur: Données JSON manquantes")
        return jsonify({"erreur": "Données requises"}), 400
    try:
        param_json = json.dumps(data)
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tmp (user_id, PARAM)
            VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET PARAM = EXCLUDED.PARAM
        """, (user_id, param_json))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Paramètres mis à jour pour user_id={user_id}: {param_json}")
        return jsonify({"message": "Paramètres mis à jour"}), 200
    except json.JSONDecodeError as e:
        print(f"Erreur JSON pour user_id={user_id}: {str(e)}")
        return jsonify({"erreur": "Données JSON invalides"}), 400
    except Exception as e:
        print(f"Erreur mise à jour paramètres pour user_id={user_id}: {str(e)}")
        return jsonify({"erreur": str(e)}), 500



# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
