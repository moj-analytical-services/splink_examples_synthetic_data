def expr_distance_in_km(lat_lng_colname):
    """Use the haversine formula to transform comparisons of lat,lngs
    into distances measured in kilometers

    Arguments:
        lat_lng_colname: The column name of the comparison column e.g. lat_lng
                               The _l and _r suffixes will be added automatically

    """

    partial_distance_sql = f"""
    (
    pow(sin(radians({lat_lng_colname}_r['lat'] - {lat_lng_colname}_l['lat']))/2, 2) +
    cos(radians({lat_lng_colname}_l['lat'])) * cos(radians({lat_lng_colname}_r['lat'])) *
    pow(sin(radians({lat_lng_colname}_r['long'] - {lat_lng_colname}_l['long'])/2),2)
    )
    """

    distance_km_sql = f"cast(atan2(sqrt({partial_distance_sql}), sqrt(-1*{partial_distance_sql} + 1)) * 12742 as float)"

    return distance_km_sql
