from dz_jobs_aggregator.utils import (
    handle_dtypes,
    create_job_id_pkey,
    replace_attribute_ids_with_values,
    replace_values,
)
from time import sleep

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


@transformer
def transform(data: pd.DataFrame, *args, **kwargs):
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

    # convert dtypes
    data = handle_dtypes(data)

    ### replace ids with values to standardize their language
    # change "sector" type to list[dict] to apply this step
    data["sector"] = data["sector"].apply(lambda dict_: [dict_])
    attribute_urls = {
        "education_level": "https://emploitic.com/api/v4/lists/niveau-de-formation",
        "experience_years": "https://emploitic.com/api/v4/lists/annees-dexperience",
        "function": "https://emploitic.com/api/v4/lists/metiers",
        "job_level": "https://emploitic.com/api/v4/lists/niveau-de-poste",
        "sector": "https://emploitic.com/api/v4/lists/secteurs",
    }
    for attribute in attribute_urls:
        # flag rows that may contain mixed-langauge values
        data[f"{attribute}_in_fr"] = data[attribute].apply(
            lambda dict_list: (
                all([dict_.get("lang") == "fr" for dict_ in dict_list])
                if dict_list
                else True
            )
        )

        # get value IDs in rows with mixed-language values
        df_attribute_not_in_fr = data.loc[
            ~data[f"{attribute}_in_fr"], ["job_id", attribute]
        ].copy()
        df_attribute_not_in_fr[f"{attribute}_id_not_fr"] = df_attribute_not_in_fr[
            attribute
        ].apply(
            lambda dict_list: (
                [dict_.get("id") for dict_ in dict_list] if dict_list else None
            )
        )
        df_attribute_not_in_fr = df_attribute_not_in_fr.explode(
            f"{attribute}_id_not_fr"
        )
        # replace the IDs with their respective values in the desired language using the attribute API endpoint
        attribute_url = attribute_urls[attribute]
        df_attribute_not_in_fr = replace_attribute_ids_with_values(
            df=df_attribute_not_in_fr,
            attribute_url=attribute_url,
            json_results_key="collection",
            df_join_key=f"{attribute}_id_not_fr",
            attribute_join_key="id",
            attribute_source_name="label",
            attribute_final_name=f"{attribute}_tr",
        )
        # array_agg back the values after exploding them
        df_attribute_not_in_fr = df_attribute_not_in_fr.groupby(
            "job_id", sort=False
        ).agg({f"{attribute}_tr": lambda x: x.tolist()})

        # rebuild final attribute column
        # - translated values
        data.loc[~data[f"{attribute}_in_fr"], attribute] = df_attribute_not_in_fr[
            f"{attribute}_tr"
        ].values
        # - values already in FR
        data.loc[data[f"{attribute}_in_fr"], attribute] = data.loc[
            data[f"{attribute}_in_fr"], attribute
        ].apply(
            lambda dict_list: (
                [dict_.get("label") for dict_ in dict_list] if dict_list else dict_list
            )
        )

        # try not to get blacklisted by the API
        sleep(3)

    # change "sector" type to string using `explode` since it always has one value
    data["sector"] = data["sector"].explode()

    # standardize the column values
    values_to_replace = {
        # "job_level": {
        #     # EN -> FR :|
        #     "Freshly Graduated": "Jeune Diplômé",
        #     "Confirmed / Experienced": "Confirmé / Expérimenté",
        #     "Beginner / Junior": "Débutant / Junior",
        #     "Intern / Student": "Stagiaire / Etudiant",
        #     "Manager / Department Manager": "Manager / Responsable Département",
        #     "Team Leader": "Responsable d'Équipe",
        # },
        "education_level": {
            "TS Bac +2": "Bac +2",
            # EN -> FR :|
            # "Senior Technician, Bac +2": "Bac +2",
            # "University Without A Degree": "Universitaire Sans Diplôme",
            # "Bachelor's Degree (LMD), Bac + 3": "Licence (LMD), Bac + 3",
            # "Master 1, Bachelor's Degree  Bac + 4": "Master 1, Licence  Bac + 4",
            # "Master 2, Engineering, Bac + 5": "Master 2, Ingéniorat, Bac + 5",
            # "PhD": "Doctorat",
        },
        "experience_years": {
            "Moins D’un An": "<1 An",
            "Sans Expérience": "<1 An",
            "6 À 10 Ans": "5 À 10 Ans",
            "Plus de 10 Ans": ">10 Ans",
        },
        "function": {
            "Administration, Moyens Généraux": "Accueil, Administration - Services Généraux",
            "Autre": "Autres",
            "Education, Enseignement": "Education, Formation, Enseignement, Langues",
            "Métiers Banque et Assurances": "Banque, Bourse, Assurance",
            "Création, Design": "Création, Design, Médias Numériques",
            "Production, Méthode, Industrie": "Electrique et Électrotechnique, Industrie, Maintenance, Méthode, Production",
            "Informatique, Systèmes d'Information, Internet": "Analyse et Science des Données, Informatique, Internet, Systèmes d'Information",
            "Ingénierie, Etudes, Projet, R&D": "Bureau d'Études, Etudes, Ingénierie, Projet, R&D",
            "Marketing, Communication": "Commercial, Communication, Création, Marketing, RP",
            "Métiers de l'Agriculture": "Métiers de l'Agriculture, Nature",
            "Métiers Banque et Assurances": "Banque, Bourse, Assurance",
            "Santé, Médical, Pharmacie": "Santé, Médical, Pharmaceutique, Délégué Médical",
            "Logistique, Achat, Stock, Transport": "Achat, Emballage, Logistique, Stock, Transport",
            "Télécommunication, Réseaux": "Télécommunications, Systèmes, Réseaux",
            "Informatique, Télécommunication & Réseaux": "Télécommunications, Systèmes, Réseaux",
        },
        "contract_type": {
            # EN -> FR :| no API endpoint to fix this
            "Permanent Contract": "CDI",
            "Fixed-term Contract": "CDD",
            "Fixed-term Contract Or Mission": "CDD Ou Mission",
            "Internship - Part Time": "Stage – Temps Partiel",
            "Independent / Seasonal": "Indépendant/Saisonnier",
        },
        "work_mode": {
            # EN -> FR :| no API endpoint to fix this
            "onsite": "sur site",
            "hybrid": "hybride",
        },
    }
    data = replace_values(data, values_to_replace)
    data["work_mode"] = data["work_mode"].str.capitalize()

    # add job_source column
    data["job_source"] = "Emploitic"

    print(data.dtypes)
    return data
    # return data[~(
    #     data["education_level_in_fr"] &
    #     data["experience_years_in_fr"] &
    #     data["function_in_fr"] &
    #     data["job_level_in_fr"]
    # )]


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "datetime" in str(
        output.datetime_published.dtype
    ), f"datetime_published is not of type `datetime`. Got `{output.datetime_published.dtype}` instead"
    assert "datetime" in str(
        output.date_scraped.dtype
    ), f"date_scraped is not of type `datetime`. Got `{output.date_scraped.dtype}` instead"
    assert output.duplicated("job_id").sum() == 0, "output has duplicates"
