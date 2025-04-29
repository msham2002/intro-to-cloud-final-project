from flask import Flask, render_template, request, redirect, url_for, session
import os, pymssql
import tempfile, pandas as pd
import json, joblib

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev")

def get_conn():
    return pymssql.connect(
        server=os.getenv("AZURE_SQL_SERVER"),
        user=os.getenv("AZURE_SQL_USER"),
        password=os.getenv("AZURE_SQL_PASSWORD"),
        database=os.getenv("AZURE_SQL_DATABASE"),
        port=1433
    )

@app.route("/", methods=["GET"])
def home():
    return redirect(url_for("login"))

@app.route("/login", methods=["GET","POST"])
def login():
    error = None
    if request.method == "POST":
        #validate these against a user store
        username = request.form["username"]
        password = request.form["password"]
        email    = request.form["email"]
        if username and password and email:
            #for now, accept any non-empty trio
            session["user"] = username
            return redirect(url_for("dashboard"))
        else:
            error = "All fields are required."
    return render_template("login.html", error=error)

@app.route("/search", methods=["GET","POST"])
def search():
    if "user" not in session:
        return redirect(url_for("login"))

    results = []
    if request.method == "POST":
        hshd = request.form["hshd_num"]
        with get_conn().cursor(as_dict=True) as cur:
            cur.execute("""
                SELECT TOP 100
                       t.HSHD_NUM, t.BASKET_NUM, t.PURCHASE,
                       t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
                FROM   retail.cleaned_400_transactions t
                JOIN   retail.cleaned_400_products p
                  ON t.PRODUCT_NUM = p.PRODUCT_NUM
                WHERE  t.HSHD_NUM = %s
                ORDER  BY t.PURCHASE DESC
            """, (hshd,))
            results = cur.fetchall()
    return render_template("search.html", results=results, user=session.get("user"))

@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))

def bulk_insert(df, table_name, cursor):
    cols = ",".join(df.columns)            #"HSHD_NUM,Loyalty,…"
    params = ",".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({params})"
    cursor.fast_executemany = True
    cursor.executemany(insert_sql, df.values.tolist())

@app.route("/upload", methods=["GET", "POST"])
def upload():
    if "user" not in session:                 #reuse simple auth
        return redirect(url_for("login"))

    message = None
    if request.method == "POST":
        table  = request.form["table"]
        file   = request.files["csv"]
        if file and file.filename.endswith(".csv"):
            tmp_path = tempfile.mktemp(suffix=".csv")
            file.save(tmp_path)

            df = pd.read_csv(tmp_path)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {table};")  #wipe old
                    bulk_insert(df, table, cur)
                conn.commit()
            os.remove(tmp_path)
            message = f"✔ Reloaded {len(df):,} rows into {table}"
        else:
            message = "Only .csv files are accepted."
    return render_template("upload.html", user=session["user"], msg=message)


@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))

    with get_conn() as conn, conn.cursor(as_dict=True) as cur:
        # 1) weekly spend (2-year horizon)
        cur.execute("""
            SELECT CONCAT(t.Year,'-',RIGHT('0'+CAST(t.Week_Num AS varchar),2)) AS PERIOD,
                   SUM(t.Spend) AS TOTAL
            FROM   retail.cleaned_400_transactions t
            GROUP  BY t.Year, t.Week_Num
            ORDER  BY t.Year, t.Week_Num
        """)
        weekly = cur.fetchall()

        # 2) top-10 departments by spend
        cur.execute("""
            SELECT TOP 10 p.Department, SUM(t.Spend) AS TOTAL
            FROM retail.cleaned_400_transactions t
            JOIN retail.cleaned_400_products     p ON t.Product_Num = p.Product_Num
            GROUP BY p.Department
            ORDER BY TOTAL DESC
        """)
        depts = cur.fetchall()

        # 3) simple basket cross-sell: top 15 product-pairs (same basket)
        cur.execute("""
            WITH pairs AS (
              SELECT TOP 15
                     CONCAT(MIN(p1.Department), ' & ', MIN(p2.Department)) AS PAIR,
                     COUNT(*) AS CNT
              FROM retail.cleaned_400_transactions t1
              JOIN retail.cleaned_400_transactions t2
                     ON  t1.Basket_Num = t2.Basket_Num
                     AND t1.Product_Num < t2.Product_Num
              JOIN retail.cleaned_400_products p1 ON t1.Product_Num = p1.Product_Num
              JOIN retail.cleaned_400_products p2 ON t2.Product_Num = p2.Product_Num
              GROUP BY t1.Product_Num, t2.Product_Num
              ORDER BY CNT DESC
            )
            SELECT PAIR, CNT FROM pairs;
        """)
        combos = cur.fetchall()

        cur.execute("""
            SELECT TOP 10
                   SEED_PROD,
                   TARGET_PROB,
                   PROB_ATTATCH
            FROM   retail.cross_sell
            ORDER  BY PROB_ATTATCH DESC;
        """)
        seeds = cur.fetchall()

    #prepare arrays for Chart.js
    ws_labels  = [r["PERIOD"] for r in weekly]
    ws_values  = [float(r["TOTAL"]) for r in weekly]

    dept_labels = [r["Department"] for r in depts]
    dept_values = [float(r["TOTAL"])     for r in depts]

    combo_labels = [r["PAIR"] for r in combos]
    combo_values = [int(r["CNT"]) for r in combos]

    with get_conn().cursor() as cur:
        cur.execute("SELECT DISTINCT SEED_PROD FROM retail.cross_sell")
        seeds = [r[0] for r in cur.fetchall()]

    return render_template(
        "dashboard.html",
        ws_labels=ws_labels, ws_values=ws_values,
        dept_labels=dept_labels, dept_values=dept_values,
        combo_labels=[], combo_values=[],
        seed_list=seeds,
        user=session["user"]
    )

@app.route("/clv")
def clv():
    if "user" not in session:
        return redirect(url_for("login"))

    with get_conn().cursor(as_dict=True) as cur:
        cur.execute("""
            SELECT TOP 20 HSHD_NUM, CAST(CLV_PRED AS FLOAT) AS CLV
            FROM   retail.clv_scores
            ORDER  BY CLV_PRED DESC;
        """)
        rows = cur.fetchall()

    return render_template("clv.html", rows=rows)

@app.route("/api/cross_sell/<int:seed_id>")
def api_cross_sell(seed_id):
    if "user" not in session:
        return ("", 401)
    with get_conn().cursor(as_dict=True) as cur:
        cur.execute("""
           SELECT TOP 10 TARGET_PROD, PROB_ATTACH
           FROM retail.cross_sell
           WHERE SEED_PROD = %s
           ORDER BY PROB_ATTACH DESC
        """, (seed_id,))
        rows = cur.fetchall()
    return json.dumps(rows), 200, {"Content-Type": "application/json"}
