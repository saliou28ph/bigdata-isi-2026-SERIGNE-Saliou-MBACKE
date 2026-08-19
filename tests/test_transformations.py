from datetime import date

from chispa import assert_df_equality
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from src.transformations import (
    unifier_manquants,
    normaliser_email,
    normaliser_ville,
    normaliser_telephone,
    dedupliquer_clients,
    valider_naissance,
)


# ============================================================
# 1. UNIFIER LES VALEURS MANQUANTES
# ============================================================
def test_unifier_manquants(spark):
    """Teste l'unification des emails manquants et le trim."""

    source_data = [
        ("",),                    # vide -> null
        ("N/A",),                 # N/A -> null
        ("n/a",),                 # n/a -> null
        ("NULL",),                # NULL -> null
        ("  N/A  ",),             # espaces + N/A -> null
        ("  NULL  ",),            # espaces + NULL -> null
        (None,),                  # null reste null
        ("user@test.com",),       # email normal conservé
        ("  user@test.com  ",),   # espaces supprimés
        ("N/A@test.com",),        # ne doit PAS devenir null
    ]

    schema = StructType([
        StructField("email", StringType(), True)
    ])

    df_source = spark.createDataFrame(source_data, schema)

    df_res = unifier_manquants(df_source)

    expected_data = [
        (None,),
        (None,),
        (None,),
        (None,),
        (None,),
        (None,),
        (None,),
        ("user@test.com",),
        ("user@test.com",),
        ("N/A@test.com",),
    ]

    df_expected = spark.createDataFrame(expected_data, schema)

    assert_df_equality(df_res, df_expected)


# ============================================================
# 2. NORMALISER LES VILLES
# ============================================================

def test_normaliser_ville(spark):
    """
    Vérifie que différentes écritures d'une même ville produisent
    la même valeur normalisée.
    """

    source_data = [
        ("C1", " Thiès "),
        ("C2", "THIES"),
        ("C3", "thiès"),
        ("C4", " Dakar "),
        ("C5", None),
    ]

    schema = ["customer_id", "ville"]

    df_source = spark.createDataFrame(source_data, schema)

    df_res = normaliser_ville(df_source)

    resultats = {
        row["customer_id"]: row["ville_norm"]
        for row in df_res.select("customer_id", "ville_norm").collect()
    }

    assert resultats["C1"] == "thies"
    assert resultats["C2"] == "thies"
    assert resultats["C3"] == "thies"
    assert resultats["C4"] == "dakar"
    assert resultats["C5"] is None


# ============================================================
# 3. NORMALISER LES TÉLÉPHONES
# ============================================================

def test_normaliser_telephone(spark):
    """
    Vérifie la validation de plusieurs formats de numéros
    de téléphone.
    """

    source_data = [
        ("C1", "+221 77 123 45 67"),  # valide
        ("C2", "77-123-45-67"),       # valide
        ("C3", "77 123 45 67"),       # valide
        ("C4", "33 820 00 00"),       # invalide selon ta règle
        ("C5", "abc"),                 # invalide
        ("C6", None),                  # manquant
    ]

    df_source = spark.createDataFrame(
        source_data,
        ["customer_id", "telephone"]
    )

    df_res = normaliser_telephone(df_source)

    resultats = {
        row["customer_id"]: row["tel_valide"]
        for row in df_res.select("customer_id", "tel_valide").collect()
    }

    assert resultats["C1"] is True
    assert resultats["C2"] is True
    assert resultats["C3"] is True
    assert resultats["C4"] is False
    assert resultats["C5"] is False

    # À conserver seulement si ta fonction retourne NULL pour téléphone NULL
    assert resultats["C6"] is None


# ============================================================
# 4. NORMALISER LES EMAILS
# ============================================================

def test_normaliser_email(spark):
    """
    Vérifie la normalisation et la validation des adresses email.
    """

    source_data = [
        ("C1", "  USER@EXAMPLE.COM  "),
        ("C2", "test@gmail.com"),
        ("C3", "email-invalide"),
        ("C4", "test@domaine"),
        ("C5", None),
    ]

    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
    ])

    df_source = spark.createDataFrame(source_data, schema)

    df_res = normaliser_email(df_source)

    resultats = {
        row["customer_id"]: (row["email"], row["email_valide"])
        for row in df_res.select(
            "customer_id",
            "email",
            "email_valide"
        ).collect()
    }

    assert resultats["C1"] == ("user@example.com", True)
    assert resultats["C2"] == ("test@gmail.com", True)

    assert resultats["C3"] == ("email-invalide", False)
    assert resultats["C4"] == ("test@domaine", False)

    assert resultats["C5"] == (None, None)


# ============================================================
# 5. VALIDER LA DATE DE NAISSANCE
# ============================================================

def test_valider_naissance(spark):
    """
    Vérifie qu'une date comprise entre 1920 et aujourd'hui est
    conservée et que les dates impossibles sont transformées en NULL.
    """

    source_data = [
        ("C1", "2000-05-10"),  # valide
        ("C2", "1920-01-01"),  # limite basse valide
        ("C3", "1919-12-31"),  # trop ancienne
        ("C4", "2999-01-01"),  # future
        ("C5", "abc"),         # format invalide
        ("C6", None),          # manquante
    ]

    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("date_naissance", StringType(), True),
    ])

    df_source = spark.createDataFrame(source_data, schema)

    df_res = valider_naissance(df_source)

    resultats = {
        row["customer_id"]: row["date_naissance"]
        for row in df_res.select(
            "customer_id",
            "date_naissance"
        ).collect()
    }

    assert resultats["C1"] == date(2000, 5, 10)

    # 1920-01-01 est exactement la borne autorisée
    assert resultats["C2"] == date(1920, 1, 1)

    assert resultats["C3"] is None
    assert resultats["C4"] is None
    assert resultats["C5"] is None
    assert resultats["C6"] is None


# ============================================================
# 6. DÉDUPLIQUER LES CLIENTS
# ============================================================

def test_dedupliquer_clients(spark):
    """
    Vérifie :
    1. la suppression des doublons exacts ;
    2. la conservation d'une seule ligne par customer_id ;
    3. la conservation de l'inscription la plus récente.
    """

    source_data = [
        # Même customer_id, ancienne version
        ("C001", "ancien@ucad.sn", "2025-01-01"),

        # Même customer_id, version récente
        ("C001", "nouveau@ucad.sn", "2026-01-01"),

        # Doublon exact de la version récente
        ("C001", "nouveau@ucad.sn", "2026-01-01"),

        # Client unique
        ("C002", "autre@ucad.sn", "2026-01-15"),

        # C003 : deux versions
        ("C003", "ancien3@ucad.sn", "2024-01-01"),
        ("C003", "recent3@ucad.sn", "2026-02-01"),
    ]

    df_source = spark.createDataFrame(
        source_data,
        ["customer_id", "email", "date_inscription"]
    )

    df_res = dedupliquer_clients(df_source)

    # Il doit rester C001, C002 et C003
    assert df_res.count() == 3

    # Un seul enregistrement par customer_id
    assert (
        df_res
        .select("customer_id")
        .distinct()
        .count()
        == 3
    )

    resultats = {
        row["customer_id"]: (
            row["email"],
            row["date_inscription"]
        )
        for row in df_res.collect()
    }

    # C001 : garder le plus récent
    assert resultats["C001"] == (
        "nouveau@ucad.sn",
        "2026-01-01"
    )

    # C002 : client unique conservé
    assert resultats["C002"] == (
        "autre@ucad.sn",
        "2026-01-15"
    )

    # C003 : garder le plus récent
    assert resultats["C003"] == (
        "recent3@ucad.sn",
        "2026-02-01"
    )