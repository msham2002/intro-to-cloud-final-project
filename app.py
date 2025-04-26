from flask import Flask, render_template, request, redirect, url_for, session
import os, pymssql

app = Flask(__name__)

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
                       t.HSHD_NUM, t.BASKET_NUM, t.Purchase_DT,
                       t.Product_Num, p.Department, p.Commodity
                FROM   retail.Transactions t
                JOIN   retail.Products p
                  ON t.Product_Num = p.Product_Num
                WHERE  t.HSHD_NUM = %s
                ORDER  BY t.Purchase_DT DESC
            """, (hshd,))
            results = cur.fetchall()
    return render_template("search.html", results=results, user=session.get("user"))

@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))
