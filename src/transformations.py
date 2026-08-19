import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import unicodedata
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

# Ajustez l'import selon la structure de votre dossier src/
#from src.cleaning import unifier_manquants, normaliser_email


# ----------------------------------------------------------------------
# 1. Tests pour unifier_manquants
# ----------------------------------------------------------------------
#fonction unifier_manquant
def unifier_manquants(df: DataFrame) -> DataFrame:
    """Emails "" / "N/A" -> null."""
    e = F.trim(F.col("email"))
    return df.withColumn(
        "email",
        F.when(e.isin("", "N/A", "n/a", "NULL"), None).otherwise(e))

#normaliser email
def normaliser_email(df: DataFrame) -> DataFrame:
    """Email en minuscules + trim ; drapeau de validite."""
    motif = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    return df.withColumn(
        "email_valide",
        F.when(F.col("email").isNull(), F.lit(None))
         .otherwise(F.col("email").rlike(motif)))

def test_unifier_manquants_remplace_valeurs_vides_et_na(spark: SparkSession):
    """
    Vérifie que '', 'N/A', 'n/a', 'NULL' et les espaces autour
    sont bien transformés en None (Null Spark).
    """
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True)
    ])

    data = [
        ("1", ""),           # Chaîne vide -> None
        ("2", "   "),        # Espaces seuls -> None (grâce à trim)
        ("3", "N/A"),        # Majuscule -> None
        ("4", "n/a"),        # Minuscule -> None
        ("5", "NULL"),       # Mot-clé NULL -> None
        ("6", " N/A "),      # N/A avec espaces -> None
        ("7", "user@test.com") # Email valide -> Doit rester intact
    ]

    df_in = spark.createDataFrame(data, schema)
    df_out = unifier_manquants(df_in)

    resultats = {row["id"]: row["email"] for row in df_out.collect()}

    # Assertions
    assert resultats["1"] is None
    assert resultats["2"] is None
    assert resultats["3"] is None
    assert resultats["4"] is None
    assert resultats["5"] is None
    assert resultats["6"] is None
    assert resultats["7"] == "user@test.com"


# ----------------------------------------------------------------------
# 2. Tests pour normaliser_email
# ----------------------------------------------------------------------

def test_normaliser_email_casse_et_validite(spark: SparkSession):
    """
    Vérifie le passage en minuscules, le trim et la création 
    du drapeau de validité 'email_valide'.
    """
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("email", StringType(), True)
    ])

    data = [
        ("1", "  USER@EXAMPLE.COM "), # Majuscule + espaces -> user@example.com (True)
        ("2", "invalide-email.com"),   # Format invalide -> False
        ("3", "test@domaine"),        # Sans TLD (.com, .fr, etc.) -> False
        ("4", None)                   # Email Null -> email_valide doit être None
    ]

    df_in = spark.createDataFrame(data, schema)
    df_out = normaliser_email(df_in)

    resultats = {row["id"]: (row["email"], row["email_valide"]) for row in df_out.collect()}

    # Client 1 : Normalisation + Valide
    assert resultats["1"][0] == "user@example.com"
    assert resultats["1"][1] is True

    # Client 2 : Format incorrect
    assert resultats["2"][0] == "invalide-email.com"
    assert resultats["2"][1] is False

    # Client 3 : Pas de domaine principal (.com, etc.)
    assert resultats["3"][1] is False

    # Client 4 : Gestion des Nulls
    assert resultats["4"][0] is None
    assert resultats["4"][1] is None


# ----------------------------------------------------------------------
# 3. Test de combinaison (Chaining .transform)
# ----------------------------------------------------------------------

def test_combinaison_unifier_et_normaliser(spark: SparkSession):
    """
    Vérifie l'enchaînement des deux transformations comme dans votre pipeline.
    """
    schema = StructType([StructField("email", StringType(), True)])
    data = [("  N/A  ",), ("  EXEMPLE@DOMAIN.SN  ",)]

    df = spark.createDataFrame(data, schema)
    df_res = df.transform(unifier_manquants).transform(normaliser_email)

    rows = df_res.collect()

    # Le "N/A" doit devenir Null puis garder email_valide à Null
    assert rows[0]["email"] is None
    assert rows[0]["email_valide"] is None

    # L'email valide doit être nettoyé et valide à True
    assert rows[1]["email"] == "exemple@domain.sn"
    assert rows[1]["email_valide"] is True



def sans_accent(s: str) -> str:
    if s is None:
        return None
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


sans_accent_udf = F.udf(sans_accent, StringType())


def normaliser_ville(df: DataFrame) -> DataFrame:
    """ville (affichage) + ville_norm (cle sans accent)."""
    df = df.withColumn("ville", F.initcap(F.trim(F.col("ville"))))
    return df.withColumn(
        "ville_norm",
        F.lower(sans_accent_udf(F.trim(F.col("ville"))))
    )

def normaliser_telephone(df: DataFrame) -> DataFrame:
    """9 chiffres, prefixe 70/75/76/77/78 ; drapeau de validite."""
    tel = F.regexp_replace(F.col("telephone"), r"[^0-9]", "")
    tel = F.regexp_replace(tel, r"^221", "")
    return (df.withColumn("tel_norm", tel)
              .withColumn("tel_valide",
                  tel.rlike(r"^(70|75|76|77|78)\d{7}$")))

def valider_naissance(df: DataFrame) -> DataFrame:
    """Date plausible entre 1920 et aujourd'hui, sinon null."""
    d = F.to_date(F.col("date_naissance"), "yyyy-MM-dd")
    return df.withColumn(
        "date_naissance",
        F.when((d >= F.lit("1920-01-01")) & (d <= F.current_date()), d)
         .otherwise(None))

def dedupliquer_clients(df: DataFrame) -> DataFrame:
    """Doublons exacts puis 1 ligne par customer_id."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy(
        F.col("date_inscription").desc())
    return (df.dropDuplicates()
              .withColumn("_r", F.row_number().over(w))
              .filter(F.col("_r") == 1)
              .drop("_r"))

def nettoyer_clients(df: DataFrame) -> DataFrame:
    return (df
        .transform(unifier_manquants)
        .transform(normaliser_email)
        .transform(normaliser_ville)
        .transform(normaliser_telephone)
        .transform(valider_naissance)
        .transform(dedupliquer_clients))