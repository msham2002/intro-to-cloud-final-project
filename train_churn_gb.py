import pandas as pd, joblib, pymssql, os
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

conn = pymssql.connect(
        server   = "intro-to-cloud-final-project-server.database.windows.net",
        user     = "intro-to-cloud-final-project-server-admin",
        password = "HXtG8$v2AlqfUuCL",
        database = "intro-to-cloud-final-project-database",
        port     = 1433)

sql = """
WITH base AS (
  SELECT  HSHD_NUM,
          SUM(SPEND)                              AS tot_spend,
          COUNT(DISTINCT BASKET_NUM)              AS tot_baskets,
          MAX(PURCHASE)                           AS last_date
  FROM retail.cleaned_400_transactions
  GROUP BY HSHD_NUM
)
SELECT *,
       DATEDIFF(day, last_date, '2020-08-15')     AS recency_days,
       CASE WHEN DATEDIFF(day, last_date, '2020-08-15') > 56 THEN 1 ELSE 0 END AS churn
FROM   base;
"""
df = pd.read_sql(sql, conn, parse_dates=["last_date"])

X = df[["tot_spend", "tot_baskets", "recency_days"]]
y = df["churn"]

pipe = Pipeline([
        ('imp',  SimpleImputer(strategy="median")),
        ('gb',   GradientBoostingClassifier(random_state=42))
])

X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=.2, stratify=y, random_state=117)
pipe.fit(X_tr, y_tr)

print("AUC on hold-out:",
      round(roc_auc_score(y_te, pipe.predict_proba(X_te)[:,1]), 3))

df["churn_prob"] = pipe.predict_proba(X)[:,1]

with conn.cursor() as cur:
    cur.execute("IF OBJECT_ID('retail.churn_scores') IS NOT NULL DROP TABLE retail.churn_scores;")
    cur.execute("""
        CREATE TABLE retail.churn_scores(
            HSHD_NUM    INT PRIMARY KEY,
            CHURN_PROB  FLOAT
        )
    """)
    cur.executemany("INSERT INTO retail.churn_scores VALUES (%s,%s)",
                    df[["HSHD_NUM","churn_prob"]].values.tolist())
    conn.commit()
conn.close()

joblib.dump(pipe, "gb_churn.pkl")
print("wrote", len(df), "rows -> retail.churn_scores   |   model -> gb_churn.pkl")
