import pandas as pd
from techchallenge.config.mapping_loader import load_mapping


def enrich_dataframe(
    df,
    mapping_file,
    left_on,
    right_on,
    drop_columns=None,
    validate="many_to_one",
):

    mapping = load_mapping(mapping_file)

    mapping[right_on] = mapping[right_on].astype(str)
    df[left_on] = df[left_on].astype(str)

    df = df.merge(
        mapping,
        how="left",
        left_on=left_on,
        right_on=right_on,
        validate=validate,
    )

    if drop_columns:
        df = df.drop(columns=drop_columns)

    return df