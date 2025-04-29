import os, pandas as pd, numpy as np, joblib, pymssql
from sklearn.model_selection import train_test_split
from sklearn.metrics      import mean_absolute_error
from sklearn.ensemble     import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer

#connection
conn = pymssql.connect(
        server   = "intro-to-cloud-final-project-server.database.windows.net",
        user     = "intro-to-cloud-final-project-server-admin",
        password = "HXtG8$v2AlqfUuCL",
        database = "intro-to-cloud-final-project-database",
        port     = 1433)

#training frame
sql = """
WITH base AS (
    SELECT  HSHD_NUM,
            SUM(CASE WHEN PURCHASE < '2019-08-17' THEN SPEND END)                    AS hist_spend,
            COUNT(DISTINCT CASE WHEN PURCHASE < '2019-08-17' THEN BASKET_NUM END)    AS hist_baskets,
            MAX  (CASE WHEN PURCHASE < '2019-08-17' THEN PURCHASE END)               AS last_purchase,
            SUM(CASE WHEN PURCHASE >= '2019-08-17' THEN SPEND END)                   AS future_spend
    FROM retail.cleaned_400_transactions
    GROUP BY HSHD_NUM
)
SELECT * FROM base
WHERE future_spend IS NOT NULL;
"""
train = pd.read_sql(sql, conn, parse_dates=["last_purchase"])

#feature engineering stuff
cutoff = pd.Timestamp("2019-08-17")
train["recency_days"] = (cutoff - train["last_purchase"]).dt.days
X = train[["hist_spend", "hist_baskets", "recency_days"]]
y = train["future_spend"]

num_cols = X.columns
pre = ColumnTransformer([
        ('num', SimpleImputer(strategy='median'), num_cols)
      ])

gbr = GradientBoostingRegressor(random_state=42)

pipe = Pipeline([("prep", pre), ("gbr", gbr)])


#model training
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=.20, random_state=117)

pipe.fit(X_tr, y_tr)
print("MAE on hold-out: $", round(mean_absolute_error(y_te, pipe.predict(X_te)), 2))

#predict
train["clv_pred"] = pipe.predict(X)

with conn.cursor() as cur:
    cur.execute("IF OBJECT_ID('retail.clv_scores') IS NOT NULL DROP TABLE retail.clv_scores;")
    cur.execute("""
        CREATE TABLE retail.clv_scores (
            HSHD_NUM  INT PRIMARY KEY,
            CLV_PRED  MONEY
        );
    """)
    cur.executemany("INSERT INTO retail.clv_scores VALUES (%d, %s)",
                    train[["HSHD_NUM", "clv_pred"]].values.tolist())
    conn.commit()
conn.close()

joblib.dump(pipe, "gbr_clv.pkl")
print("Done → wrote", len(train), "rows   &   gbr_clv.pkl")
