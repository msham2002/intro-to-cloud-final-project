import os
import pandas as pd
import pymssql
import joblib

from sklearn.ensemble          import HistGradientBoostingClassifier
from sklearn.calibration      import CalibratedClassifierCV
from sklearn.pipeline         import Pipeline
from sklearn.impute           import SimpleImputer
from sklearn.model_selection  import train_test_split
from sklearn.metrics          import roc_auc_score

#connect
conn = pymssql.connect(
        server   = "intro-to-cloud-final-project-server.database.windows.net",
        user     = "intro-to-cloud-final-project-server-admin",
        password = "HXtG8$v2AlqfUuCL",
        database = "intro-to-cloud-final-project-database",
        port     = 1433)

#churn‐flag SQL
sql = """
WITH base AS (
  SELECT
    HSHD_NUM,
    SUM(SPEND)           AS tot_spend,
    COUNT(DISTINCT BASKET_NUM) AS tot_baskets,
    MAX(PURCHASE)        AS last_date
  FROM retail.cleaned_400_transactions
  GROUP BY HSHD_NUM
)
SELECT
  HSHD_NUM,
  tot_spend,
  tot_baskets,
  last_date,
  DATEDIFF(day, last_date, '2020-08-15') AS recency_days,
  CASE
    WHEN DATEDIFF(day, last_date, '2020-08-15') > 56 THEN 1
    ELSE 0
  END AS churn
FROM base;
"""

df = pd.read_sql(sql, conn, parse_dates=["last_date"])

#prepare features & label
features = ["tot_spend", "tot_baskets", "recency_days"]
X = df[features]
y = df["churn"]

# split off calibration set
X_tr, X_tmp, y_tr, y_tmp = train_test_split(
    X, y, test_size=0.40, stratify=y, random_state=42
)
X_cal, X_te, y_cal, y_te = train_test_split(
    X_tmp, y_tmp, test_size=0.50, stratify=y_tmp, random_state=42
)

#build & train
base = HistGradientBoostingClassifier(
    random_state=42,
    max_leaf_nodes=15,
    learning_rate=0.05,
    max_iter=200
)
#prefit base, then calibrate with sigmoid
base.fit(X_tr, y_tr)
calibrated = CalibratedClassifierCV(base, method="sigmoid", cv="prefit")

pipe = Pipeline([
    ("impute", SimpleImputer(strategy="median")),
    ("clf",    calibrated)
])
pipe.fit(X_cal, y_cal)

#eval
probs = pipe.predict_proba(X_te)[:,1]
print("Test AUC:", round(roc_auc_score(y_te, probs), 3))

#score full data & write back
df["churn_prob"] = pipe.predict_proba(df[features])[:,1]

with conn.cursor() as cur:
    cur.execute("IF OBJECT_ID('retail.churn_scores') IS NOT NULL DROP TABLE retail.churn_scores;")
    cur.execute("""
      CREATE TABLE retail.churn_scores(
        HSHD_NUM   INT   PRIMARY KEY,
        CHURN_PROB FLOAT
      );
    """)
    cur.executemany(
      "INSERT INTO retail.churn_scores VALUES (%s,%s)",
      df[["HSHD_NUM","churn_prob"]].values.tolist()
    )
    conn.commit()

conn.close()

joblib.dump(pipe, "gb_churn.pkl")
print("Done → wrote", len(df), "rows and gb_churn.pkl")
