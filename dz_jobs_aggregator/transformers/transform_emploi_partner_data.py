from dz_jobs_aggregator.utils import (
    handle_dtypes,
    create_job_id_pkey,
    french_titlecase,
    replace_attribute_ids_with_values,
    replace_values,
)
import requests
import pandas as pd
import re

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # drop exact duplicates
    # Select only hashable columns for drop_duplicates
    hashable_cols = [
        col
        for col in data.columns
        if pd.api.types.is_hashable(data[col].dropna().iloc[0])
    ]
    duplicates_mask = data.duplicated(subset=hashable_cols)
    print(f"Shape before deduplication: {data.shape}")
    print(f"Exact duplicates: {duplicates_mask.sum()}")
    data = data[~duplicates_mask]
    print(f"Shape after deduplication: {data.shape}")

    # create job_id surrogate key
    data = create_job_id_pkey(data, ["sector_id"])

    # merge rows with same job_id but different properties
    group_by_cols = ["job_id", "country", "region", "state", "city"]
    agg_cols = [col for col in data.columns if col not in group_by_cols]
    agg_dict = {col: "first" for col in agg_cols}
    # Add a temporary column to count duplicates
    data["_dup_count"] = 1
    for col in ["_dup_count", "nb_views", "nb_applicants"]:
        agg_dict[col] = "sum"
    data = data.groupby(group_by_cols, as_index=False, dropna=False).agg(agg_dict)
    duplicate_group_count = (data["_dup_count"] > 1).sum()
    total_duplicate_count = data[data["_dup_count"] > 1]["_dup_count"].sum()
    duplicates = total_duplicate_count - duplicate_group_count
    print(f"Merged duplicates: {duplicates}")
    print(f"Shape after merge deduplication: {data.shape}")
    # drop the temporary column
    data = data.drop(columns=["_dup_count"])

    # replace ids with names for experience_years_id, sector_id and function_id
    # merge experience_years
    experience_url = "https://api-v4.emploipartner.com/api/params_experiences"
    data = replace_attribute_ids_with_values(
        df=data,
        attribute_url=experience_url,
        json_results_key="hydra:member",
        df_join_key="experience_years_id",
        attribute_join_key="id",
        attribute_source_name="label",
        attribute_final_name="experience_years",
    )

    # merge sector
    sector_url = "https://api-v4.emploipartner.com/api/params_activity_sector_groups?pagination=false"
    data = replace_attribute_ids_with_values(
        df=data,
        attribute_url=sector_url,
        json_results_key="hydra:member",
        df_join_key="sector_id",
        attribute_join_key="id",
        attribute_source_name="name",
        attribute_final_name="sector",
    )

    # merge function
    function_url = (
        "https://api-v4.emploipartner.com/api/params_functions?pagination=false"
    )
    data = replace_attribute_ids_with_values(
        df=data,
        attribute_url=function_url,
        json_results_key="hydra:member",
        df_join_key="function_id",
        attribute_join_key="id",
        attribute_source_name="name",
        attribute_final_name="function",
    )

    # add date_scraped column
    data["date_scraped"] = kwargs.get("execution_date").date()

    # convert dtypes
    data = handle_dtypes(data)
    print(data.dtypes)

    # standardize the column values
    # - replace slashes with commas
    slash_to_comma = lambda x: re.sub("\s*\/\s*", ", ", x)
    data["function"] = data["function"].apply(slash_to_comma)
    data["sector"] = data["sector"].apply(slash_to_comma)

    # - transform values to title case
    data["region"] = data["region"].str.title()
    data["job_level"] = data["job_level"].apply(french_titlecase)
    data["experience_years"] = data["experience_years"].apply(french_titlecase)
    data["function"] = data["function"].apply(french_titlecase)

    # - replace the values with the standardized ones
    values_to_replace = {
        "experience_years": {
            "Moins de 1 An": "<1 An",
            "Aucune Expérience": "<1 An",
            "3 À 4 Ans": "3 À 5 Ans",
            "4 À 5 Ans": "3 À 5 Ans",
            "Plus de 10 Ans": ">10 Ans",
        },
        "education_level": {
            "Secondaire": "Niveau Secondaire",
            "Bac": "Baccalauréat",
            "Bac +1": "Bac + 1",
            "Bac +2": "Bac + 2",
            "Bac +3": "Licence (LMD), Bac + 3",
            "Bac +4": "Master 1, Licence  Bac + 4",
            "Bac +5": "Master 2, Ingéniorat, Bac + 5",
            ">Bac +5": ">Bac + 5",
        },
        "contract_type": {
            "Pre-emploi": "Pré-emploi",
        },
        "sector": {
            "Construction BTP": "BTP, Construction, Immobilier",
            "Immobilier": "BTP, Construction, Immobilier",
            "service publics, Administrations": "Fonction Publique, Administration",
            "Énergie, Mines, Matière première": "Energie, Mines, Matière Première",
        },
        "function": {
            "Formation, Enseignement, Langues": "Education, Formation, Enseignement, Langues",
            "Industrie, Electrique et Électrotechnique, Production, Maintenance": "Electrique et Électrotechnique, Industrie, Maintenance, Méthode, Production",
            "Informatique, Systèmes d'Information, Analyse et Science des Données": "Analyse et Science des Données, Informatique, Internet, Systèmes d'Information",
            "Ingénierie, Bureau d'Études, Projet, R&D": "Bureau d'Études, Etudes, Ingénierie, Projet, R&D",
            "Juridique, Fiscal": "Audit, Conseil, Fiscal, Juridique",
            "Marketing, Communication, RP": "Commercial, Communication, Création, Marketing, RP",
            "Tourisme, Hôtellerie, Restauration": "Hôtellerie, Loisirs, Restauration, Tourisme",
            "Transport, Achat, Logistique, Emballage": "Achat, Emballage, Logistique, Stock, Transport",
            "Télécommunications, Systèmes, Réseau": "Télécommunications, Systèmes, Réseaux",
            "Ressources Humaines-Grh": "RH, Personnel, Formation",
            "Santé Hopital, Pharmaceutique": "Santé, Médical, Pharmaceutique, Délégué Médical",
            "Sport": "Sports",
            "Autres": "Autre",
        },
        "work_mode": {
            "en partie depuis la maison": "hybride",
            "la plupart depuis la maison": "hybride",
            "100% depuis la maison": "remote",
        },
    }
    data = replace_values(data, values_to_replace)
    data["work_mode"] = data["work_mode"].str.capitalize()

    # add job_source column
    data["job_source"] = "EmploiPartner"

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.duplicated(["job_id", "country", "region", "state", "city"]).sum() == 0, "output has duplicates"
    assert "datetime" in str(
        output.datetime_published.dtype
    ), f"datetime_published is not of type `datetime`. Got `{output.datetime_published.dtype}` instead"
    assert "datetime" in str(
        output.expire_date.dtype
    ), f"expire_date is not of type `datetime`. Got `{output.expire_date.dtype}` instead"
    assert "datetime" in str(
        output.date_scraped.dtype
    ), f"date_scraped is not of type `datetime`. Got `{output.date_scraped.dtype}` instead"
