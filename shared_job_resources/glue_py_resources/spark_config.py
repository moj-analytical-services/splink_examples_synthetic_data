from pyspark.sql.types import DoubleType, StringType, StructField, ArrayType, StructType


def add_udfs(spark):
    # Register UDFs
    udfs = [
        ("jaro_winkler_sim", "JaroWinklerSimilarity", DoubleType()),
        ("jaccard_sim", "JaccardSimilarity", DoubleType()),
        ("cosine_distance", "CosineDistance", DoubleType()),
        ("Dmetaphone", "DoubleMetaphone", StringType()),
        ("Dmetaphone", "DoubleMetaphone", StringType()),
        ("DmetaphoneAlt", "DoubleMetaphoneAlt", StringType()),
        ("QgramTokeniser", "QgramTokeniser", StringType()),
    ]

    for a, b, c in udfs:
        spark.udf.registerJavaFunction(a, "uk.gov.moj.dash.linkage." + b, c)

    # Register UDFs
    rt = ArrayType(
        StructType([StructField("_1", StringType()), StructField("_2", StringType())])
    )

    spark.udf.registerJavaFunction(
        name="DualArrayExplode",
        javaClassName="uk.gov.moj.dash.linkage.DualArrayExplode",
        returnType=rt,
    )

    rt2 = ArrayType(
        StructType(
            (
                StructField(
                    "place1",
                    StructType(
                        (
                            StructField("lat", DoubleType()),
                            StructField("long", DoubleType()),
                        )
                    ),
                ),
                StructField(
                    "place2",
                    StructType(
                        (
                            StructField("lat", DoubleType()),
                            StructField("long", DoubleType()),
                        )
                    ),
                ),
            )
        )
    )

    spark.udf.registerJavaFunction(
        name="latlongexplode",
        javaClassName="uk.gov.moj.dash.linkage.latlongexplode",
        returnType=rt2,
    )
