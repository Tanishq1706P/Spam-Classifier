"""P2 CLEAN. Error range SPM-PRE-3xx. Shared by training and streaming."""
from pyspark.sql import functions as F
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import tracked


def clean_text(col):
    """lowercase, strip HTML, URLs, numbers, punctuation, collapse spaces."""
    c = F.lower(F.coalesce(col, F.lit("")))
    c = F.regexp_replace(c, r"<[^>]+>", " ")
    c = F.regexp_replace(c, r"(https?://|www\.)\S+", " url ")
    c = F.regexp_replace(c, r"[0-9]+", " ")
    c = F.regexp_replace(c, r"[^a-z\s]", " ")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def remove_nulls_dupes(df):
    return df.dropna(subset=["label", "text"]).dropDuplicates(["text"])


def balance_classes(df, seed=42):
    """Down-sample the majority class (use when spam ratio < ~15%)."""
    counts = {r["label"]: r["count"] for r in df.groupBy("label").count().collect()}
    if len(counts) < 2:
        raise SpamError(ErrorCode.TRN_502, cause=f"classes={counts}")
    small = min(counts.values())
    fr = {k: small / v for k, v in counts.items()}
    return df.sampleBy("label", fr, seed)


@tracked("CLEAN")
def clean_dataframe(df, balance=False):
    out = remove_nulls_dupes(df)
    out = out.withColumn("label", F.when(F.lower(F.col("label")) == "spam", 1.0).otherwise(0.0))
    out = out.withColumn("text", clean_text(F.col("text"))).filter(F.length("text") > 0)
    if balance:
        out = balance_classes(out)
    if out.rdd.isEmpty():
        raise SpamError(ErrorCode.PRE_301)
    return out
