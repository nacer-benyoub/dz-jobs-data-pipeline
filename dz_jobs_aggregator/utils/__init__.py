import re
from typing import Iterable
import pandas as pd
from pandas import DataFrame
from hashlib import md5

import requests


def snake_case_to_camelcase(word):
    if "_" not in word:
        return word
    split_word = re.split("_+", word)
    camelcase_word = split_word[0] + "".join(
        x.capitalize() or "_" for x in split_word[1:]
    )
    return camelcase_word


def french_titlecase(s):
    if s is pd.NA or not s:
        return s
    # Define exceptions: words that should remain lowercase unless first word.
    exceptions = {"et", "de", "du", "des", "la", "le", "les", "d'", "l'"}
    words = s.split()
    new_words = []
    for i, w in enumerate(words):
        lower_w = w.lower()
        # Check if the word starts with "d'" or "l'"
        if lower_w.startswith("d'") or lower_w.startswith("l'"):
            # Capitalize only the part after the apostrophe.
            prefix = w[:2]
            rest = w[2:]
            new_words.append(prefix + rest.capitalize())
        elif i != 0 and lower_w in exceptions:
            new_words.append(lower_w)
        # if word is in all caps, leave as is
        elif w == w.upper():
            new_words.append(w)
        else:
            new_words.append(w.title())
    return " ".join(new_words)


def parse_emploitic_json(raw_json: str) -> list:
    """parse and extract the job listings from the response json object returned from Emploitic api jobs request

    Args:
        raw_json (str): raw_json containing details about the job listings

    Returns:
        list[dict]: list of job listings as dicts
    """
    jobs = []

    all_listings = raw_json.get("results")
    if not all_listings:
        return jobs

    for listing in all_listings:
        job_item = {}
        job_item["title"] = listing.get("title")
        job_item["positions"] = listing.get("openPositions")
        job_item["datetime_published"] = listing.get("publishedAt")
        job_item["work_mode"] = listing.get("workMode")
        company = listing["company"]
        job_item["company"] = company.get("name")
        sector = company.get("sector")
        keys = ["id", "label", "lang"]
        if sector:
            job_item["sector"] = {key: sector.get(key) for key in keys}

        job_item["is_anonymous"] = listing.get("isAnonymous")

        # These fields don't have a fixed type
        job_criteria = listing["criteria"]
        loosely_typed_fields = [
            "location",
            "function",
            "job_level",
            "education_level",
            "contract_type",
            "experience_years",
        ]
        for field in loosely_typed_fields:
            # convert the field name for reading from the raw json
            if field == "function":
                camelcase_field = "profession"
            else:
                camelcase_field = snake_case_to_camelcase(field)

            # get the field value
            # raw_field_obj = job_criteria[camelcase_field]
            # raw field is either a dict or a list of dicts
            # we need the `label` value
            # if isinstance(raw_field_obj, list):
            #     job_item[field] = [
            #         field_value["label"] for field_value in raw_field_obj
            #     ]

            # elif isinstance(raw_field_obj, dict):
            #     job_item[field] = raw_field_obj["label"]

            # else:
            #     job_item[field] = raw_field_obj

            # get the whole object because we need the `label`, 'id' and 'lang' values
            raw_field_obj = job_criteria[camelcase_field]
            if field == "contract_type":
                job_item[field] = (
                    [field_value["label"] for field_value in raw_field_obj]
                    if raw_field_obj
                    else raw_field_obj
                )
            elif isinstance(raw_field_obj, list):
                job_item[field] = [
                    {key: field_value.get(key) for key in keys}
                    for field_value in raw_field_obj
                ]
            elif isinstance(raw_field_obj, dict):
                job_item[field] = {key: raw_field_obj.get(key) for key in keys}

            else:
                job_item[field] = raw_field_obj

        jobs.append(job_item)
    return jobs


def parse_emploi_partner_json(raw_json: str) -> list:
    """parse and extract the job listings from the response json object returned from EmploiPartner api jobs request

    Args:
        raw_json (str): raw_json containing details about the job listings

    Returns:
        list[dict]: list of job listings as dicts
    """
    jobs = []

    all_listings = raw_json.get("hydra:member")
    if not all_listings:
        return jobs

    for listing in all_listings:
        job_item = {}
        first_order_fields = {
            "title": "title",
            "company": "companyName",
            "positions": "nbPosition",
            "datetime_published": "publishedDate",
            "expire_date": "expireDate",
            "nb_applicants": "nbApplicant",
            "nb_views": "nbView",
            "is_anonymous": "hideCompany",
            "experience_years_id": "nbMonthExperience",
        }
        for k, v in first_order_fields.items():
            job_item[k] = listing.get(v)

        nested_fields = {
            "city": {"name": "city", "value_key": "name"},
            "state": {"name": "region", "value_key": "name"},
            "country": {"name": "country", "value_key": "name"},
            "sector_id": {"name": "sectorGroup", "value_key": "id"},
            "function_id": {"name": "function", "value_key": "id"},
            "job_level": {"name": "careerLevel", "value_key": "name"},
            "education_level": {"name": "studyLevel", "value_key": "name"},
            "work_mode": {"name": "workplace", "value_key": "name"},
        }
        for k, v in nested_fields.items():
            field_name = v.get("name")
            field_value_key = v.get("value_key")
            listing_value = listing.get(field_name)
            if listing_value:
                job_item[k] = listing_value.get(field_value_key)
                if k == "state":
                    job_item["region"] = listing_value.get("cardinal")
            else:
                job_item[k] = listing_value

        # only contractTypes is a list
        contract_types = listing.get("contractTypes")
        if contract_types:
            job_item["contract_type"] = [val.get("name") for val in contract_types]
        else:
            job_item["contract_type"] = contract_types

        # salary has only min and max
        salary = listing.get("salary")
        if salary:
            job_item["has_salary"] = True
            job_item["min_salary"] = salary.get("min")
            job_item["max_salary"] = salary.get("max")
        else:
            job_item["has_salary"] = False
            job_item["min_salary"] = job_item["max_salary"] = salary

        jobs.append(job_item)
    return jobs


def handle_dtypes(df: DataFrame) -> DataFrame:

    typed_df = df.convert_dtypes()
    typed_df["datetime_published"] = pd.to_datetime(typed_df["datetime_published"])
    typed_df["date_scraped"] = pd.to_datetime(typed_df["date_scraped"])
    if "expire_date" in typed_df.columns:
        typed_df["expire_date"] = pd.to_datetime(typed_df["expire_date"])

    return typed_df


def create_job_id_pkey(df: DataFrame, cols: Iterable[str] = None) -> DataFrame:
    """Create a job_id column using md5 hash of the title, company and datetime_published columns and any other columns specified in `cols`."""
    default_cols = ["title", "company", "datetime_published"]
    if cols is not None:
        for col in cols:
            if col not in df.columns:
                raise KeyError(f"{col} is not a column in the input DataFrame")
        default_cols.extend(cols)
    # create a combined primary key column
    df.loc[:, "combined_pkey"] = (
        df[default_cols].astype(str).fillna("").agg(lambda x: "".join(x), axis=1)
    )
    # generate a job_id using md5 hash of the combined_pkey
    # and the first word of the title in lowercase
    generate_id = lambda x: md5(x.encode()).hexdigest()
    df["job_id"] = (
        df["title"].str.split(" ").str.get(0).str.lower()
        + "-"
        + df["combined_pkey"].apply(generate_id).str.slice(stop=8)
    )
    df = df.drop(columns=["combined_pkey"])

    return df


def replace_attribute_ids_with_values(
    df: DataFrame,
    attribute_url: str,
    df_join_key: str,
    attribute_source_name: str,
    attribute_final_name: str,
    attribute_join_key: str,
    json_results_key: str = None,
    values_lang: str = "fr",
    api_json_page_size: int = 1000,
    params: dict = {},
    join_method="left",
) -> DataFrame:
    """Fetch attribute values from its url and join with df
    on given keys to replace them wih attribute values in df

    Args:
        df (DataFrame): original dataframe
        attribute_url (str): url to fetch attribute data
        json_results_key (str): key under which is the mapping of attribute ids and values in response json, If no value is supplied, the mapping is assumed to be the non-nested first level objects
        df_join_key (str): attribute join key in original dataframe
        attribute_join_key (str): attribute join key in attribute dataframe
        attribute_source_name (str): attribute name in attribute dataframe
        attribute_final_name (str): attribute name in final dataframe
        values_lang (str): prefered language for attribute values. To be passed as `lang` url query parameter. Defaults to "fr"
        api_json_page_size (int): number of results returned from url call. To be passed as `per_page` url query parameter. Defaults to 1000,
        params (dict): additional url query parameters. Defaults to empty dict.,
        join_method (str, optional): original and attribute dataframe join method. Defaults to "left".

    Returns:
        DataFrame: final dataframe with attribute keys replaced by their respective values
    """
    params = params | {"lang": values_lang, "per_page": api_json_page_size}
    headers = {"Accept-Language": values_lang}
    response = requests.get(url=attribute_url, params=params, headers=headers)
    attribute_json_data = response.json()
    if json_results_key:
        attribute_json_data = attribute_json_data[json_results_key]
    attribute_df = DataFrame(attribute_json_data).convert_dtypes()
    merged_df = df.merge(
        right=attribute_df[[attribute_join_key, attribute_source_name]],
        how=join_method,
        left_on=df_join_key,
        right_on=attribute_join_key,
    )
    merged_df = merged_df.drop(columns=[attribute_join_key, df_join_key])
    merged_df = merged_df.rename(columns={attribute_source_name: attribute_final_name})
    return merged_df


def replace_values(df: DataFrame, values_to_replace: dict) -> DataFrame:
    # convert dtypes to ensure that 'object' fields are arrays and not strings
    df = df.convert_dtypes()

    for field in values_to_replace:
        if field not in df.columns:
            raise KeyError(f"{field} is not a column in the input DataFrame")
        if df[field].dtype == "object":
            # replaced_column = (
            #     df.explode(field)
            #     .replace(values_to_replace[field])
            #     .groupby("job_id", sort=False)
            #     .agg({field: lambda x: x.tolist()})
            #     .loc[:, field]
            #     .values
            # )
            val_map = values_to_replace[field]

            def update_list(val_list, val_map):
                if not val_list:
                    return val_list
                mapped_val_list = []
                for val in val_list:
                    if val in val_map:
                        mapped_val_list.append(val_map[val])
                    else:
                        mapped_val_list.append(val)
                return mapped_val_list

            df[field] = df[field].apply(update_list, args=(val_map,))
        else:
            df[field] = df[field].replace(values_to_replace[field])
    return df
