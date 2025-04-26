from flask import Flask, render_template, request, redirect, url_for
import pymssql, os

app = Flask(__name__)


conn = pymssql.connect(
    server=os.getenv("AZURE_SQL_SERVER"),  # intro-to-cloud-final-project-server.database.windows.net
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("AZURE_SQL_DB"),    # retaildb
    port=1433
)
@app.route('/')
def home():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET','POST'])
def login():
    error = None
    if request.method == 'POST':
        # verify user/pass against a credentials store
        # For now, just redirect
        return redirect(url_for('search'))
    return render_template('login.html', error=error)

@app.route('/search', methods=['GET','POST'])
def search():
    results = []
    if request.method == 'POST':
        hshd = request.form['hshd_num']
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 100
                  t.BASKET_NUM, t.PURCHASE_DT,
                  t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
                FROM Transactions t
                JOIN Products p ON t.PRODUCT_NUM=p.PRODUCT_NUM
                WHERE t.HSHD_NUM = ?
                ORDER BY t.PURCHASE_DT DESC
            """, hshd)
            results = cursor.fetchall()
    return render_template('search.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)