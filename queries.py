from db import get_connection

def batiments_mauvais_etat():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.nom_batiment, e.etat, e.date_etat
        FROM batiment b
        JOIN etat_conservation e ON b.Id_batiment = e.Id_batiment
        WHERE e.etat = 'mauvais'
        ORDER BY e.date_etat DESC;
    """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def interventions_par_entreprise():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.nom_prestataire, COUNT(i.Id_intervention) AS nb_interventions
        FROM prestataire p
        JOIN intervention i ON p.Id_prestataire = i.Id_prestataire
        GROUP BY p.nom_prestataire
        ORDER BY nb_interventions DESC;
    """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def batiments_restaures_annee(annee):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT b.nom_batiment, i.date_travaux
        FROM batiment b
        JOIN intervention i ON b.Id_batiment = i.Id_batiment
        WHERE EXTRACT(YEAR FROM i.date_travaux) = %s;
    """, (annee,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def cout_total_par_quartier():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT z.nom_zone, SUM(i.cout) AS total_cout
        FROM batiment b
        JOIN zone z ON b.Id_zone = z.Id_zone
        JOIN intervention i ON b.Id_batiment = i.Id_batiment
        GROUP BY z.nom_zone
        ORDER BY total_cout DESC;
    """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def prestataires_plus_de_3_chantiers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.nom_prestataire, COUNT(i.Id_intervention) AS nb_chantiers
        FROM prestataire p
        JOIN intervention i ON p.Id_prestataire = i.Id_prestataire
        GROUP BY p.nom_prestataire
        HAVING COUNT(i.Id_intervention) > 3;
    """)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result
