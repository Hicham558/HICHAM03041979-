from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app, origins=["https://hicham558.github.io"])  # Autoriser les requêtes depuis ton front-end
app.debug = True  # Activer le mode debug pour voir les erreurs

# Connexion à la base de données (compatible avec Railway)
def get_conn():
    url = os.environ['DATABASE_URL']
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, sslmode='require')

# Vérification de l'utilisateur (X-User-ID et X-Numero-Util)
def validate_user_id():
    user_id = request.headers.get('X-User-ID')
    numero_util = request.headers.get('X-Numero-Util')
    if not user_id or not numero_util:
        return jsonify({'erreur': 'Identifiant utilisateur ou numéro utilisateur requis'}), 401
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT statut FROM utilisateur WHERE numero_util = %s AND user_id = %s", (numero_util, user_id))
        user = cur.fetchone()
        cur.close()
        conn.close()
        if not user:
            return jsonify({'erreur': 'Utilisateur invalide'}), 401
        return user_id, numero_util, user[0]  # Retourne user_id, numero_util, statut
    except Exception as e:
        return jsonify({'erreur': f'Erreur validation utilisateur : {str(e)}'}), 500

# Route pour vérifier que l'API est en ligne
@app.route('/', methods=['GET'])
def index():
    try:
        conn = get_conn()
        conn.close()
        return 'API en ligne - Connexion PostgreSQL OK'
    except Exception as e:
        return f'Erreur connexion DB : {e}', 500

# Authentification de l'utilisateur
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    nom = data.get('nom')
    password = data.get('password')
    if not nom or not password:
        return jsonify({'erreur': 'Nom et mot de passe requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT numero_util, statut, password2, user_id FROM utilisateur WHERE nom = %s", (nom,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user or user[2] != password:  # Comparaison en texte brut
            return jsonify({'erreur': 'Nom ou mot de passe incorrect'}), 401
        return jsonify({
            'numero_util': user[0],
            'statut': user[1],
            'user_id': user[3],
            'message': 'Connexion réussie'
        }), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# Ajouter un utilisateur (réservé aux admins)
@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, numero_util, statut = validation_result

    if statut != 'admin':
        return jsonify({'erreur': 'Action réservée aux administrateurs'}), 403

    data = request.get_json()
    nom = data.get('nom')
    password = data.get('password')
    if not nom or not password:
        return jsonify({'erreur': 'Nom et mot de passe requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO utilisateur (statut, nom, password2, user_id) VALUES (%s, %s, %s, %s) RETURNING numero_util",
            ('emplo', nom, password, user_id)  # Stockage en texte brut
        )
        new_numero_util = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur ajouté', 'numero_util': new_numero_util}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# Lister les utilisateurs (réservé aux admins)
@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, numero_util, statut = validation_result

    if statut != 'admin':
        return jsonify({'erreur': 'Action réservée aux administrateurs'}), 403

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_util, nom, statut FROM utilisateur WHERE user_id = %s ORDER BY nom", (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        utilisateurs = [
            {
                'numero_util': row['numero_util'],
                'nom': row['nom'],
                'statut': row['statut']
            }
            for row in rows
        ]
        return jsonify(utilisateurs), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

# --- Clients ---
@app.route('/liste_clients', methods=['GET'])
def liste_clients():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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
                'PRIXBA': row[5] or '0.00'
            }
            for row in rows
        ]
        return jsonify(produits)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_item', methods=['POST'])
def ajouter_item():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

    data = request.get_json()
    designation = data.get('designation')
    bar = data.get('bar')
    prix = data.get('prix')
    qte = data.get('qte')
    prixba = data.get('prixba')

    if not all([designation, bar, prix is not None, qte is not None]):
        return jsonify({'erreur': 'Champs obligatoires manquants (designation, bar, prix, qte)'}), 400

    try:
        prix = float(prix)
        qte = int(qte)
        if prix < 0 or qte < 0:
            return jsonify({'erreur': 'Le prix et la quantité doivent être positifs'}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM item WHERE bar = %s AND user_id = %s", (bar, user_id))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Ce code-barres existe déjà'}), 409

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

# Endpoint pour valider une vente
@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, numero_util, _ = validation_result

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes']:
        print("Erreur: Données de vente invalides ou aucune ligne fournie")
        return jsonify({"error": "Données de vente invalides ou aucune ligne fournie"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')
    amount_paid = float(data.get('amount_paid', 0))
    lignes = data['lignes']
    nature = "TICKET" if numero_table == 0 else "BON DE L."

    if payment_mode == 'a_terme' and numero_table == 0:
        print("Erreur: Vente à terme sans client sélectionné")
        return jsonify({"error": "Veuillez sélectionner un client pour une vente à terme"}), 400

    if payment_mode == 'a_terme' and amount_paid < 0:
        print("Erreur: Montant versé négatif")
        return jsonify({"error": "Le montant versé ne peut pas être négatif"}), 400

    total_sale = sum(float(ligne.get('prixt', 0)) for ligne in lignes)
    if payment_mode == 'a_terme' and amount_paid > total_sale:
        print("Erreur: Montant versé supérieur au total de la vente")
        return jsonify({"error": "Le montant versé ne peut pas dépasser le total de la vente"}), 400

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT COALESCE(MAX(compteur), 0) as max_compteur
            FROM comande
            WHERE nature = %s
        """, (nature,))
        compteur = cur.fetchone()['max_compteur'] + 1
        print(f"Compteur calculé: nature={nature}, compteur={compteur}")

        cur.execute("""
            INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id, numero_util)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_comande
        """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id, numero_util))
        numero_comande = cur.fetchone()['numero_comande']
        print(f"Commande insérée: numero_comande={numero_comande}, nature={nature}, connection1=-1, compteur={compteur}, numero_util={numero_util}")

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

        if payment_mode == 'a_terme' and numero_table != 0:
            total_sale = sum(float(ligne.get('prixt', 0)) for ligne in lignes)
            solde_change = amount_paid - total_sale

            cur.execute("SELECT solde FROM client WHERE numero_clt = %s", (numero_table,))
            client = cur.fetchone()
            if not client:
                raise Exception(f"Client avec numero_clt={numero_table} non trouvé")

            current_solde = float(client['solde']) if client['solde'] and client['solde'].strip() else 0.0
            new_solde = current_solde + solde_change
            new_solde_str = f"{new_solde:.2f}"

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

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

# --- Ventes du Jour ---
@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

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
                a.numero_item,
                a.quantite,
                a.prixt,
                a.remarque,
                i.designation
            FROM comande c
            LEFT JOIN client cl ON c.numero_table = cl.numero_clt
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
                    'lignes': []
                }

            ventes_map[row['numero_comande']]['lignes'].append({
                'numero_item': row['numero_item'],
                'designation': row['designation'],
                'quantite': row['quantite'],
                'prixt': str(row['prixt']),
                'remarque': str(row['remarque'])
            })

            total += float(row['prixt'] or 0)

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
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

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
            date_start = None
            date_end = None

        query = """
            SELECT 
                i.designation,
                SUM(a.quantite) AS quantite
            FROM attache a
            JOIN comande c ON a.numero_comande = c.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s
        """
        params = [user_id]

        if date_start and date_end:
            query += " AND c.date_comande >= %s AND c.date_comande <= %s"
            params.extend([date_start, date_end])

        if numero_clt:
            if numero_clt == '0':
                query += " AND c.numero_table = 0"
            else:
                query += " AND c.numero_table = %s"
                params.append(int(numero_clt))

        query += """
            GROUP BY i.designation
            ORDER BY quantite DESC
            LIMIT 10
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        articles = [
            {
                'designation': row['designation'],
                'quantite': int(row['quantite'])
            }
            for row in rows
        ]

        cur.close()
        conn.close()

        return jsonify(articles), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération articles les plus vendus: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Bénéfice par date ---
@app.route('/profit_by_date', methods=['GET'])
def profit_by_date():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if selected_date:
            try:
                date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
                date_end = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
                date_start = (date_obj - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
            except ValueError:
                return jsonify({'erreur': 'Format de date invalide (attendu: YYYY-MM-DD)'}), 400
        else:
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = """
            SELECT 
                DATE(c.date_comande) AS date,
                COALESCE(SUM(a.prixt::float - (a.quantite * COALESCE(NULLIF(i.prixba, '')::float, 0))), 0) AS profit
            FROM attache a
            JOIN comande c ON a.numero_comande = c.numero_comande
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

        query += """
            GROUP BY DATE(c.date_comande)
            ORDER BY DATE(c.date_comande) ASC
        """

        cur.execute(query, params)
        rows = cur.fetchall()

        profits = []
        current_date = date_start
        while current_date <= date_end:
            date_str = current_date.strftime('%Y-%m-%d')
            row = next((r for r in rows if r['date'].strftime('%Y-%m-%d') == date_str), None)
            profits.append({
                'date': date_str,
                'profit': float(row['profit']) if row else 0.0
            })
            current_date += timedelta(days=1)

        cur.close()
        conn.close()

        return jsonify(profits), 200

    except Exception as e:
        if conn:
            cur.close()
            conn.close()
        print(f"Erreur récupération bénéfice par date: {str(e)}")
        return jsonify({'erreur': str(e)}), 500

# --- Dashboard ---
@app.route('/dashboard', methods=['GET'])
def dashboard():
    validation_result = validate_user_id()
    if isinstance(validation_result, tuple):
        return validation_result
    user_id, _, _ = validation_result

    period = request.args.get('period', 'day')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if period == 'week':
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        query_kpi = """
            SELECT 
                COALESCE(SUM(a.prixt::float), 0) AS total_ca,
                COALESCE(SUM(a.prixt::float - (a.quantite * COALESCE(NULLIF(i.prixba, '')::float, 0))), 0) AS total_profit,
                COUNT(DISTINCT c.numero_comande) AS sales_count
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            JOIN item i ON a.numero_item = i.numero_item
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
        """
        cur.execute(query_kpi, (user_id, date_start, date_end))
        kpi_data = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE user_id = %s AND qte < 10", (user_id,))
        low_stock_count = cur.fetchone()['low_stock']

        query_top_client = """
            SELECT 
                cl.nom,
                COALESCE(SUM(a.prixt::float), 0) AS client_ca
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
        cur.execute(query_top_client, (user_id, date_start, date_end))
        top_client = cur.fetchone()

        query_chart = """
            SELECT 
                DATE(c.date_comande) AS sale_date,
                COALESCE(SUM(a.prixt::float), 0) AS daily_ca
            FROM comande c
            JOIN attache a ON c.numero_comande = a.numero_comande
            WHERE c.user_id = %s
            AND c.date_comande >= %s
            AND c.date_comande <= %s
            GROUP BY DATE(c.date_comande)
            ORDER BY sale_date
        """
        cur.execute(query_chart, (user_id, date_start, date_end))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

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

# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))