from dz_jobs_aggregator.utils import handle_dtypes, create_job_id_pkey, french_titlecase, replace_values
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
    # create job_id surrogate key
    data = create_job_id_pkey(data)

    # drop_duplicates
    data = data.drop_duplicates("job_id")

    # replace ids with names for experience_years_id, sector_id and function_id
    experience_url = "https://api-v4.emploipartner.com/api/params_experiences"
    sector_url = "https://api-v4.emploipartner.com/api/params_activity_sector_groups?pagination=false"
    function_url = (
        "https://api-v4.emploipartner.com/api/params_functions?pagination=false"
    )

    experience_json = requests.get(experience_url).json()["hydra:member"]
    experience_df = pd.DataFrame(experience_json).convert_dtypes()
    sector_json = requests.get(sector_url).json()["hydra:member"]
    sector_df = pd.DataFrame(sector_json).convert_dtypes()
    function_json = requests.get(function_url).json()["hydra:member"]
    function_df = pd.DataFrame(function_json).convert_dtypes()

    # merge experience_years
    data = data.merge(
        experience_df[["id", "label"]],
        how="left",
        left_on="experience_years_id",
        right_on="id",
    )
    data = data.drop(columns=["id", "experience_years_id"])
    data = data.rename(columns={"label": "experience_years"})

    # merge sector
    data = data.merge(
        sector_df[["id", "name"]], how="left", left_on="sector_id", right_on="id"
    )
    data = data.drop(columns=["id", "sector_id"])
    data = data.rename(columns={"name": "sector"})

    # merge function
    data = data.merge(
        function_df[["id", "name"]], how="left", left_on="function_id", right_on="id"
    )
    data = data.drop(columns=["id", "function_id"])
    data = data.rename(columns={"name": "function"})
    
    # convert dtypes
    data = handle_dtypes(data)
    print(data.dtypes)

    # standardize the columns values following Emploitic values
    slash_to_comma = lambda x: re.sub("\s*\/\s*", ", ", x)
    data["function"] = data["function"].apply(slash_to_comma)
    data["sector"] = data["sector"].apply(slash_to_comma)
    
    data["region"] = data["region"].str.title()
    data["job_level"] = data["job_level"].apply(french_titlecase)
    data["experience_years"] = data["experience_years"].apply(french_titlecase)
    data["function"] = data["function"].apply(french_titlecase)
    
    values_to_replace = {
        "experience_years": {
            "Moins de 1 An": "Moins D’un An",
            "Aucune Expérience": "Sans Expérience",
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
            "Pre-emploi": "Pré-emploi"
        },
        "sector": {
            "Construction BTP": "BTP, Construction, Immobilier",
            "Immobilier": "BTP, Construction, Immobilier",
            "service publics, Administrations": "Fonction Publique, Administration",
            "Énergie, Mines, Matière première": "Energie, Mines, Matière Première",
        },
        "function": {
            "Industrie, Electrique et Électrotechnique, Production, Maintenance": "Electrique et Électrotechnique, Industrie, Maintenance, Méthode, Production",
            "Informatique, Systèmes d'Information, Analyse et Science des Données": "Analyse et Science des Données, Informatique, Internet, Systèmes d'Information",
            "Ingénierie, Bureau d'Études, Projet, R&D": "Bureau d'Études, Etudes, Ingénierie, Projet, R&D",
            "Marketing, Communication, RP": "Commercial, Communication, Création, Marketing, RP",
            "Transport, Achat, Logistique, Emballage": "Achat, Emballage, Logistique, Stock, Transport",
            "Télécommunications, Systèmes, Réseau": "Télécommunications, Systèmes, Réseaux",
            "Sport": "Sports",
            "Autres": "Autre"
        },
    }    
    data = replace_values(data, values_to_replace)

    # add job_source column
    data["job_source"] = "EmploiPartner"

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "datetime" in str(
        output.datetime_published.dtype
    ), f"datetime_published is not of type `datetime`. Got `{output.datetime_published.dtype}` instead"
    assert "datetime" in str(
        output.expire_date.dtype
    ), f"expire_date is not of type `datetime`. Got `{output.expire_date.dtype}` instead"
    assert "datetime" in str(
        output.date_scraped.dtype
    ), f"date_scraped is not of type `datetime`. Got `{output.date_scraped.dtype}` instead"
    assert output.duplicated("job_id").sum() == 0, "output has duplicates"
