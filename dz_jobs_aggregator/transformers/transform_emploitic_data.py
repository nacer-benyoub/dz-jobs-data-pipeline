from dz_jobs_aggregator.utils import handle_dtypes, create_job_id_pkey, replace_values

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

    # standardize the columns values following Emploi Partner values
    values_to_replace = {
        "job_level": {
            # EN -> FR :|
            "Freshly Graduated": "Jeune Diplômé",
            "Confirmed / Experienced": "Confirmé / Expérimenté",
            "Beginner / Junior": "Débutant / Junior",
            "Intern / Student": "Stagiaire / Etudiant",
            "Manager / Department Manager": "Manager / Responsable Département",
            "Team Leader": "Responsable d'Équipe",
        },
        "education_level": {
            "TS Bac +2": "Bac +2",
            # EN -> FR :|
            "Senior Technician, Bac +2": "Bac +2",
            "University Without A Degree": "Universitaire Sans Diplôme",
            "Bachelor's Degree (LMD), Bac + 3": "Licence (LMD), Bac + 3",
            "Master 1, Bachelor's Degree  Bac + 4": "Master 1, Licence  Bac + 4",
            "Master 2, Engineering, Bac + 5": "Master 2, Ingéniorat, Bac + 5",
            "PhD": "Doctorat",
        },
        "function": {
            "Création, Design": "Création, Design, Médias Numériques",
            "Production, Méthode, Industrie": "Electrique et Électrotechnique, Industrie, Maintenance, Méthode, Production",
            "Informatique, Systèmes d'Information, Internet": "Analyse et Science des Données, Informatique, Internet, Systèmes d'Information",
            "Ingénierie, Etudes, Projet, R&D": "Bureau d'Études, Etudes, Ingénierie, Projet, R&D",
            "Marketing, Communication": "Commercial, Communication, Création, Marketing, RP",
            "Logistique, Achat, Stock, Transport": "Achat, Emballage, Logistique, Stock, Transport",
            "Télécommunication, Réseaux": "Télécommunications, Systèmes, Réseaux",
        },
        "work_mode": {
            "remote": "100% depuis la maison",
            # EN -> FR :|
            "onsite": "sur site",
            "hybrid": "hybride",
        },
    }
    data = replace_values(data, values_to_replace)

    # add job_source column
    data["job_source"] = "Emploitic"

    print(data.dtypes)
    return data


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
