import json, os, boto3, pandas as pd
from io import BytesIO

def _load_pop_df(key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    BUCKET = os.environ["BUCKET"]
    obj  = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    data = json.loads(obj)
    return (
        pd.json_normalize(data["data"])[["Year", "Population"]]
        .rename(columns={"Year": "year", "Population": "population"})
        .astype({"year": int})
    )

def _load_bls_df() -> pd.DataFrame:
    s3 = boto3.client("s3")
    BUCKET = os.environ["BUCKET"]
    obj = s3.get_object(Bucket=BUCKET, Key="pr.data.0.Current")["Body"].read()
    df  = (
        pd.read_csv(BytesIO(obj), sep="\t")
        .rename(columns=lambda c: c.strip())
        .assign(series_id=lambda d: d["series_id"].str.strip())
        .query("period.str.startswith('Q')", engine="python")
        .astype({"year": int, "value": float})
    )
    return df

def run_reports(pop, bls):
    # 1) Mean + std-dev for years 2013-2018
    subset = pop.query("year.between(2013, 2018)", engine="python")
    print(f"Mean pop 2013-2018: {subset.population.mean():,.0f}")
    print(f"Std  pop 2013-2018: {subset.population.std():,.0f}")

    # 2) Best year per series_id
    best_year = (
        bls.groupby(['series_id','year'], as_index=False).value.sum()
           .rename(columns={'value':'year_sum'})
           .sort_values(['series_id','year_sum'], ascending=[True,False])
           .drop_duplicates('series_id')
    )
    best_year["year_sum"] = best_year["year_sum"].round(1)
    print("Best-year sample:\n", best_year.head())

    # 3) PRS30006032 / Q01 joined with population
    target = bls.query(
        "series_id == 'PRS30006032' and period == 'Q01'"
    )[["series_id", "year", "period", "value"]]

    joined = (
        target.merge(pop, on="year", how="left")
              .dropna(subset=["population"])
              .rename(columns={"population": "Population"})
              .astype({"Population": "int64"})
              .sort_values("year")
              .reset_index(drop=True)
    )
    print("Joined sample (PRS30006032 Q01):\n", joined.head())

def handler(event, context):
    try:
        body = json.loads(event["Records"][0]["body"])
        key  = body["Records"][0]["s3"]["object"]["key"]
    except (KeyError, IndexError, json.JSONDecodeError):
        raise ValueError(f"Could not find S3 key in event payload: {event}")
    print("Triggered for:", key)

    pop_df = _load_pop_df(key)
    bls_df = _load_bls_df()
    run_reports(pop_df, bls_df)

    return {"status": "analytics-done", "key": key}