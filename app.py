from flask import Flask, render_template, request, redirect
from db import get_connection
import queries
app = Flask(__name__)

# Default IDs for empty DB
DEFAULT_ZONE_ID = 1
DEFAULT_TYPE_ID = 1
DEFAULT_OWNER_ID = 1

@app.route("/")
def index():
    conn = get_connection()
    cur = conn.cursor()
    
    # Ensure default entries exist
    cur.execute("INSERT INTO zone (nom_zone) VALUES ('Default Zone') ON CONFLICT DO NOTHING;")
    cur.execute("INSERT INTO type_batiment (libelle) VALUES ('Default Type') ON CONFLICT DO NOTHING;")
    cur.execute("INSERT INTO proprietaire (nom_proprietaire, type_proprietaire) VALUES ('Default Owner','Privé') ON CONFLICT DO NOTHING;")
    conn.commit()

    # Fetch all tables
    cur.execute("""
        SELECT b.Id_batiment, b.nom_batiment, b.date_construction, b.niveau_protection,
               z.nom_zone, t.libelle, p.nom_proprietaire
        FROM batiment b
        JOIN zone z ON b.Id_zone = z.Id_zone
        JOIN type_batiment t ON b.Id_type = t.Id_type
        JOIN proprietaire p ON b.Id_proprietaire = p.Id_proprietaire
        ORDER BY b.Id_batiment
    """)
    batiments = cur.fetchall()

    cur.execute("SELECT Id_prestataire, nom_prestataire, type_prestataire, contact FROM prestataire ORDER BY Id_prestataire")
    prestataires = cur.fetchall()

    cur.execute("""
        SELECT i.Id_intervention, b.nom_batiment, p.nom_prestataire, i.date_travaux, i.type_travaux, i.cout
        FROM intervention i
        JOIN batiment b ON i.Id_batiment = b.Id_batiment
        JOIN prestataire p ON i.Id_prestataire = p.Id_prestataire
        ORDER BY i.Id_intervention
    """)
    interventions = cur.fetchall()

    cur.execute("""
        SELECT ins.Id_inspection, b.nom_batiment, ins.date_inspection, ins.rapport
        FROM inspection ins
        JOIN batiment b ON ins.Id_batiment = b.Id_batiment
        ORDER BY ins.Id_inspection
    """)
    inspections = cur.fetchall()

    cur.execute("""
        SELECT d.Id_doc, b.nom_batiment, d.chemin, d.type_doc
        FROM document d
        JOIN batiment b ON d.Id_batiment = b.Id_batiment
        ORDER BY d.Id_doc
    """)
    documents = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("index.html",
                           batiments=batiments,
                           prestataires=prestataires,
                           interventions=interventions,
                           inspections=inspections,
                           documents=documents)

# ---------- Routes to add entries ----------

@app.route("/add_batiment", methods=["POST"])
def add_batiment():
    try:
        nom_batiment = request.form["name"]
        year = str(request.form["year"])
        date_construction = f"{year}-01-01"
        etat = request.form["etat"]
        altitude = float(request.form["latitude"])
        longitude = float(request.form["longitude"])

        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO batiment 
            (nom_batiment, Id_zone, Id_type, Id_proprietaire, date_construction, altitude, longitude, niveau_protection)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING Id_batiment
        """, (nom_batiment, DEFAULT_ZONE_ID, DEFAULT_TYPE_ID, DEFAULT_OWNER_ID,
              date_construction, altitude, longitude, etat))
        batiment_id = cur.fetchone()[0]

        # initial etat_conservation
        cur.execute("""
            INSERT INTO etat_conservation (Id_batiment, date_etat, etat)
            VALUES (%s, CURRENT_DATE, %s)
        """, (batiment_id, etat))

        conn.commit()
        cur.close()
        conn.close()
        return redirect("/")
    except Exception as e:
        return f"Error: {e}"

@app.route("/add_prestataire", methods=["POST"])
def add_prestataire():
    try:
        nom = request.form["nom"]
        type_p = request.form["type"]
        contact = request.form["contact"]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO prestataire (nom_prestataire, type_prestataire, contact)
            VALUES (%s, %s, %s)
        """, (nom, type_p, contact))
        conn.commit()
        cur.close()
        conn.close()
        return redirect("/")
    except Exception as e:
        return f"Error: {e}"

@app.route("/add_intervention", methods=["POST"])
def add_intervention():
    try:
        id_batiment = int(request.form["batiment"])
        id_prestataire = int(request.form["prestataire"])
        date_travaux = request.form["date_travaux"]
        type_travaux = request.form["type_travaux"]
        cout = float(request.form["cout"]) if request.form["cout"] else None

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO intervention (Id_batiment, Id_prestataire, date_travaux, type_travaux, cout)
            VALUES (%s, %s, %s, %s, %s)
        """, (id_batiment, id_prestataire, date_travaux, type_travaux, cout))
        conn.commit()
        cur.close()
        conn.close()
        return redirect("/")
    except Exception as e:
        return f"Error: {e}"

@app.route("/add_inspection", methods=["POST"])
def add_inspection():
    try:
        id_batiment = int(request.form["batiment"])
        date_inspection = request.form["date_inspection"]
        rapport = request.form["rapport"]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO inspection (Id_batiment, date_inspection, rapport)
            VALUES (%s, %s, %s)
        """, (id_batiment, date_inspection, rapport))
        conn.commit()
        cur.close()
        conn.close()
        return redirect("/")
    except Exception as e:
        return f"Error: {e}"

@app.route("/add_document", methods=["POST"])
def add_document():
    try:
        id_batiment = int(request.form["batiment"])
        chemin = request.form["chemin"]
        type_doc = request.form["type_doc"]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO document (Id_batiment, chemin, type_doc)
            VALUES (%s, %s, %s)
        """, (id_batiment, chemin, type_doc))
        conn.commit()
        cur.close()
        conn.close()
        return redirect("/")
    except Exception as e:
        return f"Error: {e}"
    
@app.route("/rapports")
def rapports():
    mauvais = queries.batiments_mauvais_etat()
    interventions = queries.interventions_par_entreprise()
    annee = request.args.get("annee", 2025, type=int)
    restaures = queries.batiments_restaures_annee(annee)
    cout_quartier = queries.cout_total_par_quartier()
    prestataires = queries.prestataires_plus_de_3_chantiers()

    return render_template("rapports.html",
                           mauvais=mauvais,
                           interventions=interventions,
                           annee=annee,
                           restaures=restaures,
                           cout_quartier=cout_quartier,
                           prestataires=prestataires)

if __name__ == "__main__":
    app.run(debug=True)
