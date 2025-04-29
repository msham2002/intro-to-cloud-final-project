import os, pandas as pd, pymssql, joblib, json
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingClassifier

#connection 
conn = pymssql.connect(
    server   = "intro-to-cloud-final-project-server.database.windows.net",
    user     = "intro-to-cloud-final-project-server-admin",
    password = "HXtG8$v2AlqfUuCL",
    database = "intro-to-cloud-final-project-database",
    port=1433)

#Pick seed product
TOP_N = 40
top_sql = f"""
SELECT TOP {TOP_N} PRODUCT_NUM
FROM   retail.cleaned_400_transactions
GROUP BY PRODUCT_NUM
ORDER BY COUNT(*) DESC;
"""
seeds = pd.read_sql(top_sql, conn)["PRODUCT_NUM"].tolist()

#fit model for one seed product
def train_for_seed(seed: int):
    print("seed =", seed)
    sql = f"""
    WITH bx AS (
        SELECT t.BASKET_NUM,
               MAX(CASE WHEN t.PRODUCT_NUM = {seed} THEN 1 ELSE 0 END) AS has_seed,
               SUM(t.Spend)   AS basket_spend,
               COUNT(*)       AS basket_items,
               DATENAME(weekday, MIN(t.PURCHASE)) AS dow
        FROM retail.cleaned_400_transactions t
        GROUP BY t.BASKET_NUM
        HAVING MAX(CASE WHEN t.PRODUCT_NUM = {seed} THEN 1 ELSE 0 END) = 1
    ),
    label AS (
        SELECT b.BASKET_NUM,
               MAX(CASE WHEN PRODUCT_NUM <> {seed} THEN PRODUCT_NUM END) AS target_prod
        FROM   bx b
        JOIN   retail.cleaned_400_transactions t  ON b.BASKET_NUM = t.BASKET_NUM
        GROUP  BY b.BASKET_NUM
    )
    SELECT l.target_prod,
           b.basket_spend, b.basket_items, b.dow
    FROM label l
    JOIN bx    b ON l.BASKET_NUM = b.BASKET_NUM;
    """
    df = pd.read_sql(sql, conn)
    df["label"] = 1
    neg = df.copy()
    neg["label"] = 0
    neg["target_prod"] = -1                                    
    df = pd.concat([df, neg]).sample(frac=1, random_state=42)

    X = df[["basket_spend", "basket_items", "dow"]]
    y = df["label"]

    pre = ColumnTransformer([("num", SimpleImputer(strategy="median"),
                              ["basket_spend", "basket_items"]),
                             ("cat", OneHotEncoder(handle_unknown="ignore"),
                              ["dow"])])
    pipe = Pipeline([("prep", pre),
                     ("gb" , GradientBoostingClassifier(random_state=42))])

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=.2, stratify=y, random_state=42)
    pipe.fit(X_tr, y_tr)
    auc = roc_auc_score(y_te, pipe.predict_proba(X_te)[:, 1])
    print("  AUC =", round(auc, 3))

    probs = pipe.predict_proba(X_tr)[:, 1].mean()
    return dict(seed=seed, model=pipe, auc=auc, base_prob=probs)

#train per-seed, keep top-10 recs
cross_sell_rows = []
models = {}

for seed in seeds:
    info = train_for_seed(seed)
    models[seed] = info["model"]

    others = [p for p in seeds if p != seed]
    test = pd.DataFrame({
        "basket_spend": [20]*len(others),
        "basket_items": [4]*len(others),
        "dow": ['Saturday']*len(others)
    })
    probs = info["model"].predict_proba(test)[:, 1]
    top_idx = probs.argsort()[::-1][:10]
    for idx in top_idx:
        cross_sell_rows.append(
            (seed, int(others[idx]), float(probs[idx])) )

print("writing cross-sell table …")
with conn.cursor() as cur:
    cur.execute("IF OBJECT_ID('retail.cross_sell') IS NOT NULL DROP TABLE retail.cross_sell;")
    cur.execute("""
        CREATE TABLE retail.cross_sell (
           SEED_PROD   INT,
           TARGET_PROD INT,
           PROB_ATTACH FLOAT,
           PRIMARY KEY (SEED_PROD, TARGET_PROD)
        );
    """)
    cur.executemany("INSERT INTO retail.cross_sell VALUES (%s,%s,%s)",
                    cross_sell_rows)
    conn.commit()
conn.close()

joblib.dump(models, "gb_basket_models.pkl")
print("DONE: models + table saved")
