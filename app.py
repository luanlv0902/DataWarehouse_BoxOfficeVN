from flask import Flask, jsonify, render_template
import mysql.connector
from utils.db_connection import get_db_config
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATE_DIR)

@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/api/dm_daily_revenue")
def api_daily_revenue():
    cfg = get_db_config("datamart")
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()

    cur.execute("""
        SELECT movie_name, full_date, revenue_vnd, tickets_sold, showtimes
        FROM dm_daily_revenue
        ORDER BY full_date ASC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "movie": r[0],
            "date": str(r[1]),
            "revenue": int(r[2]),
            "tickets": int(r[3]),
            "showtimes": int(r[4])
        }
        for r in rows
    ])

@app.route("/api/dm_top_movies")
def api_top_movies():
    cfg = get_db_config("datamart")
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            movie_name,
            MAX(total_revenue) AS revenue,
            MAX(total_tickets) AS tickets,
            MAX(total_showtimes) AS showtimes,
            MIN(ranking) AS rank
        FROM dm_top_movies
        GROUP BY movie_name
        ORDER BY rank ASC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([
        {
            "movie": r[0],
            "revenue": int(r[1]),
            "tickets": int(r[2]),
            "showtimes": int(r[3]),
            "rank": int(r[4])
        }
        for r in rows
    ])


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
