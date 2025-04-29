from flask import Flask, render_template, request, redirect, url_for, session
import os, pymssql
import tempfile, pandas as pd

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
            return redirect(url_for("search"))
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