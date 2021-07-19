from splink.case_statements import sql_gen_case_stmt_name_inversion_4


from postcode_location import expr_distance_in_km


def dob_case_statement_leven(dob_colname, leven_distance=1):
    # It's often the case that too many dates of birth are on the first of january
    # typically because when exact dob is unavailable they round to nearest year
    return f"""
    case
    when {dob_colname}_l is null or {dob_colname}_r is null then -1
    when {dob_colname}_l = {dob_colname}_r  and substr({dob_colname}_l, -5) = '01-01'  then 4
    when {dob_colname}_l = {dob_colname}_r  then 5
    when levenshtein({dob_colname}_l, {dob_colname}_r) <= {leven_distance} then 3
    when datediff({dob_colname}_l, {dob_colname}_r) <= 365 then 2
    when datediff({dob_colname}_l, {dob_colname}_r) <= 10*365 then 1
    else 0 end
    """


def get_postcode_custom_case_expression():

    birth_place_equal = "(birth_place_l = birth_place_r)"

    postcode_custom_expression = f"""
    case
    when (postcode_l is null or postcode_r is null) and (birth_place_l is null or birth_place_r is null) then -1
    when postcode_l = postcode_r then 5
    when  ({expr_distance_in_km('lat_lng')} < 5) then 4
    when  ({expr_distance_in_km('lat_lng')} < 50)  then 3
    when {birth_place_equal} then 2
    when  ({expr_distance_in_km('lat_lng')} < 150)  then 1
    else 0
    end
    """

    return postcode_custom_expression


settings = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "unique_id",
    "blocking_rules": ["l.postcode = r.postcode"],
    "comparison_columns": [
        {
            "custom_name": "surname_std",
            "case_expression": sql_gen_case_stmt_name_inversion_4(
                "surname_std",
                [
                    "forename1_std",
                    "forename2_std",
                    "forename3_std",
                    "forename4_std",
                    "forename5_std",
                ],
                include_dmeta=True,
            ),
            "custom_columns_used": [
                "surname_std",
                "forename1_std",
                "forename2_std",
                "forename3_std",
                "forename4_std",
                "forename5_std",
            ],
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "custom_name": "forename1_std",
            "case_expression": sql_gen_case_stmt_name_inversion_4(
                "forename1_std",
                [
                    "surname_std",
                    "forename2_std",
                    "forename3_std",
                    "forename4_std",
                    "forename5_std",
                ],
                include_dmeta=True,
            ),
            "custom_columns_used": [
                "surname_std",
                "forename1_std",
                "forename2_std",
                "forename3_std",
                "forename4_std",
                "forename5_std",
            ],
            "num_levels": 4,
            "term_frequency_adjustments": True,
        },
        {
            "custom_name": "forename2_std",
            "case_expression": sql_gen_case_stmt_name_inversion_4(
                "forename2_std",
                [
                    "surname_std",
                    "forename1_std",
                    "forename3_std",
                    "forename4_std",
                    "forename5_std",
                ],
                include_dmeta=True,
            ),
            "custom_columns_used": [
                "surname_std",
                "forename1_std",
                "forename2_std",
                "forename3_std",
                "forename4_std",
                "forename5_std",
            ],
            "num_levels": 4,
        },
        {
            "col_name": "occupation",
            "num_levels": 2,
        },
        {
            "col_name": "dob",
            "case_expression": dob_case_statement_leven("dob"),
            "num_levels": 6,
            "u_probabilities": [1, 2, 3, 4, 5, 6],
            "m_probabilities": [6, 5, 4, 3, 2, 1],
        },
        {
            "custom_name": "custom_postcode_distance_comparison",
            "custom_columns_used": [
                "postcode",
                "lat_lng",
                "birth_place",
            ],
            "case_expression": get_postcode_custom_case_expression(),
            "num_levels": 6,
            "u_probabilities": [1, 2, 3, 4, 5, 6],
            "m_probabilities": [6, 5, 4, 3, 2, 1],
        },
    ],
    "additional_columns_to_retain": ["id"],
    "em_convergence": 0.01,
}
