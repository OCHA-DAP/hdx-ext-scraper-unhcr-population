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
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify
from urllib.parse import urljoin

logger = logging.getLogger(__name__)
hxltags = {
    "Year": "#date+year",
    "ISO3CoO": "#country+code+iso+origin",
    "ISO3CoA": "#country+code+iso+asylum",
    "ProcedureType": "#action+procedure+code",
    "ApplicationType": "#meta+application+kind+code",
    "ApplicationDataType": "#meta+application+unit+code",
    "ApplicationAveragePersonsPerCase": "#indicator+average+num+applications+percase",
    "Applications": "#indicator+num+applications+total",
    "DecisionType": "#meta+decision+code",
    "DecisionDataType": "#meta+decision+unit+code",
    "DecisionsAveragePersonsPerCase": "#indicator+average+num+decisions+percase",
    "Recognized": "#population+recognized+num",
    "RecognizedOther": "#population+recognized+other+num",
    "OtherwiseClosed": "#population+otherwise+closed+num",
    "Rejected": "#population+rejected+num",
    "TotalDecided": "#population+total+decided+num",
    "PT": "#meta+code+pt",
    "location": "#loc",
    "urbanRural": "#indicator+urban+rural",
    "accommodationType": "#indicator+accomodation",
    "Female_0_4": "#population+f+infants+num",
    "Female_5_11": "#population+f+children+num",
    "Female_12_17": "#population+f+adolescents+num",
    "Female_18_59": "#population+f+adults+num",
    "Female_60": "#population+f+elderly+num",
    "Female_Unknown": "#population+f+unknown+num",
    "Female_total": "#population+f+total+num",
    "Male_0_4": "#population+m+infants+num",
    "Male_5_11": "#population+m+children+num",
    "Male_12_17": "#population+m+adolescents+num",
    "Male_18_59": "#population+m+adults+num",
    "Male_60": "#population+m+elderly+num",
    "Male_Unknown": "#population+m+unknown+num",
    "Male_total": "#population+m+total+num",
    "Total": "#population+i+total+num",
    "REF": "#indicator+num+ref",
    "IDP": "#population+flow+num+idp",
    "ASY": "#indicator+num+asy",
    "OOC": "#indicator+num+ooc",
    "STA": "#indicator+num+sta",
    "VDA": "#indicator+num+vda",
    "RST": "#indicator+num+rst",
    "NAT": "#indicator+num+nat",
    "RET": "#indicator+num+ret",
    "RDP": "#population+refugees+total+num",
}

WORLD = "world"


def get_countriesdata(download_url, files, downloader):
    countriesdata = {WORLD: {"iso3": WORLD, "countryname": "World"}}
    countries = list()
    if not download_url.endswith("/"):
        download_url += "/"

    all_headers = {}
    for name, filename in files.items():
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
            + dict(ISO3CoO="refugees", ISO3CoA="asylants").get(
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
                countries.append({"iso3": countryiso, "countryname": countryname})
                row[country_name_column] = countryname
                if countryiso not in countriesdata:
                    countriesdata[countryiso] = {}
                if resource_name not in countriesdata[countryiso]:
                    countriesdata[countryiso][resource_name] = []
                if resource_name not in countriesdata[WORLD]:
                    countriesdata[WORLD][resource_name] = []
                countriesdata[countryiso][resource_name].append(row)
                countriesdata[WORLD][resource_name].append(row)
        for country_name_column in reversed(country_name_columns):
            headers.insert(3, country_name_column)
        for resource_name in resource_names:
            all_headers[resource_name] = headers
    return countries, all_headers, countriesdata


def generate_dataset_and_showcase(folder, country, countrydata, headers):
    """
    """
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title = "%s - Data on UNHCR population" % countryname
    logger.info("Creating dataset: %s" % title)
    slugified_name = slugify("UNHCR Population Data for %s" % countryname).lower()
    dataset = Dataset({"name": slugified_name, "title": title,})
    dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
    dataset.set_organization("hdx")
    dataset.set_expected_update_frequency("Every month")
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ["hxl", "refugees", "asylum", "population"]
    dataset.add_tags(tags)

    def process_dates(row):
        year = int(row["Year"])
        startdate = datetime(year, 1, 1)
        enddate = datetime(year, 12, 31)
        return {"startdate": startdate, "enddate": enddate}

    for resource_name, resource_rows in countrydata.items():
        if countryiso == WORLD:  # refugees and asylants contain the same data for WORLD
            if resource_name.endswith("asylants"):
                continue
            resource_name.replace("_refugees", "")
        filename = f"{resource_name}_%s.csv" % countryiso
        name = resource_name.replace("_", " ").capitalize()
        resourcedata = {
            "name": f"{name} Data for {countryname}",
            "description": f"{name} data with HXL tags",
        }

        #        quickcharts = {
        #            "cutdown": 2,
        #            "cutdownhashtags": ["#date+year+end", "#adm1+name", "#affected+killed"],
        #        }
        success, results = dataset.generate_resource_from_iterator(
            headers[resource_name],
            resource_rows,
            hxltags,
            folder,
            filename,
            resourcedata,
            date_function=process_dates,
            #           quickcharts=quickcharts,
        )

        if success is False:
            logger.warning(f"{countryname} - {name}  has no data!")

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
