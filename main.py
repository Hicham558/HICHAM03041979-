from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
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



# Endpoint pour valider une vente
@app.route('/valider_vente', methods=['POST'])
def valider_vente():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Retourne l'erreur 401 si user_id est invalide

    data = request.get_json()
    if not data or 'lignes' not in data or not data['lignes']:
        print("Erreur: Données de vente invalides ou aucune ligne fournie")
        return jsonify({"error": "Données de vente invalides ou aucune ligne fournie"}), 400

    numero_table = data.get('numero_table', 0)
    date_comande = data.get('date_comande', datetime.utcnow().isoformat())
    payment_mode = data.get('payment_mode', 'espece')  # Par défaut "espece"
    amount_paid = float(data.get('amount_paid', 0))  # Montant versé, 0 par défaut
    lignes = data['lignes']
    nature = "TICKET" if numero_table == 0 else "BON DE L."  # Déterminer nature

    # Validation du mode de paiement et du client
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

        # Récupérer le dernier compteur pour cette nature
        cur.execute("""
            SELECT COALESCE(MAX(compteur), 0) as max_compteur
            FROM comande
            WHERE nature = %s
        """, (nature,))
        compteur = cur.fetchone()['max_compteur'] + 1
        print(f"Compteur calculé: nature={nature}, compteur={compteur}")

        # Insérer la commande
        cur.execute("""
            INSERT INTO comande (numero_table, date_comande, etat_c, nature, connection1, compteur, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING numero_comande
        """, (numero_table, date_comande, 'cloture', nature, -1, compteur, user_id))
        numero_comande = cur.fetchone()['numero_comande']
        print(f"Commande insérée: numero_comande={numero_comande}, nature={nature}, connection1=-1, compteur={compteur}")

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

# --- Ventes du Jour ---
@app.route('/ventes_jour', methods=['GET'])
def ventes_jour():
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    # Paramètres de filtre
    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

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

        # Requête SQL
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

        # Organiser les données
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
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id  # Erreur 401 si user_id invalide

    # Paramètres de filtre
    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

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
            date_start = None
            date_end = None

        # Requête SQL
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

        # Formater les données
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
    user_id = validate_user_id()
    if not isinstance(user_id, str):
        return user_id

    selected_date = request.args.get('date')
    numero_clt = request.args.get('numero_clt')

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates (7 jours, incluant la date sélectionnée)
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

        # Requête SQL pour calculer le bénéfice
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

        # Remplir les 7 jours, même ceux sans données
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



@app.route('/dashboard', methods=['GET'])
def dashboard():
    userId = validate_user_id()
    if not isinstance(userId, str):
        return userId

    period = request.args.get('period', 'day')  # Par défaut : aujourd'hui
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Définir la plage de dates
        if period == 'week':
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            date_start = (datetime.now() - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:  # day
            date_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

        # Requête pour les KPI principaux
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
        cur.execute(query_kpi, (userId, date_start, date_end))
        kpi_data = cur.fetchone()

        # Requête pour les articles en rupture de stock
        cur.execute("SELECT COUNT(*) AS low_stock FROM item WHERE user_id = %s AND qte < 10", (userId,))
        low_stock_count = cur.fetchone()['low_stock']

        # Requête pour le top client
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
        cur.execute(query_top_client, (userId, date_start, date_end))
        top_client = cur.fetchone()

        # Requête pour les données du graphique (CA par jour)
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
        cur.execute(query_chart, (userId, date_start, date_end))
        chart_data = cur.fetchall()

        cur.close()
        conn.close()

        # Formater les données du graphique
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

# --- Utilisateurs ---
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    nom = data.get('nom')
    password = data.get('password')

    if not nom or not password:
        return jsonify({'erreur': 'Nom et mot de passe requis'}), 400

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT numero_util, nom, statut, password2 FROM utilisateur WHERE nom = %s", (nom,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user:
            return jsonify({'erreur': 'Utilisateur non trouvé'}), 401

        # Vérifier le mot de passe (en texte clair)
        if password != user['password2']:
            return jsonify({'erreur': 'Mot de passe incorrect'}), 401

        # Générer un user_id (par exemple, en encodant nom:password en base64)
        user_id = f"{user['numero_util']}:{nom}"
        role = user['statut']

        return jsonify({'user_id': user_id, 'role': role}), 200
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/liste_utilisateurs', methods=['GET'])
def liste_utilisateurs():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):  # Si validate_user_id retourne une erreur
        return user_id

    # Vérifier si l'utilisateur est admin
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT statut FROM utilisateur WHERE numero_util = %s", (user_id.split(':')[0],))
        user = cur.fetchone()
        if not user or user[0] != 'admin':
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Accès réservé aux administrateurs'}), 403
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

    try:
        cur.execute("SELECT numero_util, nom, statut FROM utilisateur ORDER BY nom")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        utilisateurs = [
            {
                'numero_util': row[0],
                'nom': row[1],
                'statut': row[2]
            }
            for row in rows
        ]
        return jsonify(utilisateurs)
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/ajouter_utilisateur', methods=['POST'])
def ajouter_utilisateur():
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    # Vérifier si l'utilisateur est admin
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT statut FROM utilisateur WHERE numero_util = %s", (user_id.split(':')[0],))
        user = cur.fetchone()
        if not user or user[0] != 'admin':
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Accès réservé aux administrateurs'}), 403
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

    data = request.get_json()
    nom = data.get('nom')
    password = data.get('password')
    statut = data.get('statut')

    # Validation des champs obligatoires
    if not all([nom, password, statut]):
        return jsonify({'erreur': 'Champs obligatoires manquants (nom, password, statut)'}), 400

    if statut not in ['admin', 'emplo']:
        return jsonify({'erreur': 'Statut invalide (doit être "admin" ou "emplo")'}), 400

    try:
        cur.execute(
            "INSERT INTO utilisateur (nom, password2, statut) VALUES (%s, %s, %s) RETURNING numero_util",
            (nom, password, statut)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'statut': 'Utilisateur ajouté', 'id': user_id}), 201
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

@app.route('/supprimer_utilisateur/<numero_util>', methods=['DELETE'])
def supprimer_utilisateur(numero_util):
    user_id = validate_user_id()
    if isinstance(user_id, tuple):
        return user_id

    # Vérifier si l'utilisateur est admin
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT statut FROM utilisateur WHERE numero_util = %s", (user_id.split(':')[0],))
        user = cur.fetchone()
        if not user or user[0] != 'admin':
            cur.close()
            conn.close()
            return jsonify({'erreur': 'Accès réservé aux administrateurs'}), 403
    except Exception as e:
        return jsonify({'erreur': str(e)}), 500

    try:
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

# Lancer l'application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
