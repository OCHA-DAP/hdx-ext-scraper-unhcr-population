#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
UNHCR:
-----

Generates HXlated API urls from the UNHCR data.

"""
import logging
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify
from urllib.parse import urljoin
from fields import convert_fields_in_iterator, convert_headers, hxltags_mapping

logger = logging.getLogger(__name__)

WORLD = "world"


def get_countriesdata(download_url, resources, downloader):
    countriesdata = {WORLD: {"iso3": WORLD, "countryname": "World"}}
    countries = set()
    if not download_url.endswith("/"):
        download_url += "/"

    all_headers = {}
    for name, record in resources.items():
        filename = record["file"]
        specific_download_url = urljoin(download_url, filename)
        headers, iterator = downloader.get_tabular_rows(
            specific_download_url, headers=1, dict_form=True
        )
        country_columns = sorted(
            set(column for column in headers if column in ["ISO3CoO", "ISO3CoA"])
        )
        country_name_columns = [
            country_column.replace("ISO3", "") + "_name"
            for country_column in country_columns
        ]
        resource_names = [
            f"{name}_"
            + dict(ISO3CoO="originating", ISO3CoA="residing").get(
                country_column, country_column
            )
            for country_column in country_columns
        ]

        for row in iterator:
            for country_column, country_name_column, resource_name in zip(
                country_columns, country_name_columns, resource_names
            ):
                countryiso = row[country_column]
                countryname = Country.get_country_name_from_iso3(countryiso)
                logger.info(
                    f"Processing {countryiso} - {countryname}, resource {resource_name}"
                )
                countries.add((countryiso, countryname))
                row[country_name_column] = countryname
                if countryiso not in countriesdata:
                    countriesdata[countryiso] = {}
                if resource_name not in countriesdata[countryiso]:
                    countriesdata[countryiso][resource_name] = []
                if resource_name not in countriesdata[WORLD]:
                    countriesdata[WORLD][resource_name] = []
                countriesdata[countryiso][resource_name].append(row)
                countriesdata[WORLD][resource_name].append(row)
        for country_name_column in country_name_columns:
            headers.insert(3, country_name_column)
        for resource_name in resource_names:
            all_headers[resource_name] = headers
    countries = [{"iso3": x[0], "countryname": x[1]} for x in sorted(list(countries))]
    return countries, all_headers, countriesdata


def generate_dataset_and_showcase(folder, country, countrydata, headers, resources, fields):
    """
    """
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title = "%s - Data on forcibly displaced populations and stateless persons" % countryname
    logger.info("Creating dataset: %s" % title)
    slugified_name = slugify("UNHCR Population Data for %s" % countryiso).lower()
    dataset = Dataset({"name": slugified_name, "title": title,})
    dataset.set_maintainer("8d70b12b-7247-48d2-b426-dbb4bf82eb7c")
    dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
    dataset.set_expected_update_frequency("Every year")
    dataset.set_subnational(True)
    # Check for unknown country names
    try:
        dataset.add_country_location(countryiso)
    except HDXError:
        logger.error(f"{countryname} ({countryiso})  not recognised!")
        return None, None

    tags = ["hxl", "refugees", "asylum", "population"]
    dataset.add_tags(tags)

    def process_dates(row):
        year = int(row["Year"])
        startdate = datetime(year, 1, 1)
        enddate = datetime(year, 12, 31)
        return {"startdate": startdate, "enddate": enddate}

    for resource_name, resource_rows in countrydata.items():
        resource_id = "_".join(resource_name.split("_")[:-1])
        originating_residing = resource_name.split("_")[-1] # originating or residing
        record = resources[resource_id]

        if countryiso == WORLD:  # refugees and asylants contain the same data for WORLD
            if originating_residing=="originating":
                continue
        format_parameters = dict(countryiso=countryiso.lower(), countryname=countryname)
        filename = f"{resource_name}_{countryiso}.csv"
        resourcedata = {
            "name": record[originating_residing]["title"].format(**format_parameters),
            "description": record[originating_residing]["description"].format(**format_parameters),
        }

        #        quickcharts = {
        #            "cutdown": 2,
        #            "cutdownhashtags": ["#date+year+end", "#adm1+name", "#affected+killed"],
        #        }
        success, results = dataset.generate_resource_from_iterator(
            convert_headers(headers[resource_name], fields),
            convert_fields_in_iterator(resource_rows, fields),
            hxltags_mapping(fields),
            folder,
            filename,
            resourcedata,
            date_function=process_dates,
            #           quickcharts=quickcharts,
        )

        if success is False:
            logger.warning(f"{countryname} - {resource_name}  has no data!")

    if len(dataset.get_resources()) == 0:
        logger.error(f"{countryname}  has no data!")
        return None, None

    showcase = Showcase(
        {
            "name": "%s-showcase" % slugified_name,
            "title": title,
            "notes": "UNHCR Population Data Dashboard for %s" % countryname,
            "url": "https://www.unhcr.org/refugee-statistics/",
            "image_url": "https://www.unhcr.org/assets/img/unhcr-logo.png",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase
