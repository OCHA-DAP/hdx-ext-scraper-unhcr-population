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
from slugify import slugify
from urllib.parse import urljoin
from fields import RowIterator, ListIterator

logger = logging.getLogger(__name__)

WORLD = "world"


def get_countriesdata(download_url, resources, fields, downloader):
    countriesdata = {WORLD: {}}
    qc_rows = dict()
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
                qc_country = qc_rows.get(countryiso, dict())
                year = row["Year"]
                origin = row["ISO3CoO"]
                asylum = row["ISO3CoA"]
                row_key = f"{year}_{origin}_{asylum}"
                qc_row = qc_country.get(row_key, dict())
                qc_row["Year"] = year
                qc_row["ISO3CoO"] = origin
                qc_row["ISO3CoA"] = asylum
                qc_row["CoO_name"] = Country.get_country_name_from_iso3(
                    origin
                )
                qc_row["CoA_name"] = Country.get_country_name_from_iso3(
                    asylum
                )
                if countryiso == origin:
                    attribute = "outgoing"
                else:
                    attribute = "incoming"
                for field in ["Applications", "ASY", "IDP", "OOC", "REF", "STA", "VDA"]:
                    value = row.get(field)
                    if value is None:
                        continue
                    qc_field = f"{field}_{attribute}"
                    qc_row[qc_field] = value

                qc_country[row_key] = qc_row
                qc_rows[countryiso] = qc_country
        for country_name_column in country_name_columns:
            headers.insert(3, country_name_column)
        for resource_name in resource_names:
            all_headers[resource_name] = headers
    countries = [{"iso3": WORLD, "countryname": "World"}] + [
        {"iso3": x[0], "countryname": x[1]} for x in sorted(list(countries))
    ]
    qc_rows[WORLD] = None
    return countries, all_headers, countriesdata, qc_rows


def generate_dataset_and_showcase(
    folder, country, countrydata, qc_rows, headers, resources, fields
):
    """
    """
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title_text = "Data on forcibly displaced populations and stateless persons"
    if countryname == "World":
        title = f"{title_text} (Global)"
    else:
        title = f"{countryname} - {title_text}"
    logger.info("Creating dataset: %s" % title)
    slugified_name = slugify("UNHCR Population Data for %s" % countryiso).lower()
    dataset = Dataset({"name": slugified_name, "title": title})
    dataset.set_maintainer("8d70b12b-7247-48d2-b426-dbb4bf82eb7c")
    dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
    dataset.set_expected_update_frequency("Every year")
    dataset.set_subnational(True)
    if countryiso == WORLD:
        dataset.add_other_location("world")
    else:
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

    earliest_startdate = None
    latest_enddate = None
    for resource_name, resource_rows in countrydata.items():
        resource_id = "_".join(resource_name.split("_")[:-1])
        originating_residing = resource_name.split("_")[-1]  # originating or residing
        record = resources[resource_id]

        if countryiso == WORLD:  # refugees and asylants contain the same data for WORLD
            if originating_residing == "originating":
                continue
        format_parameters = dict(countryiso=countryiso.lower(), countryname=countryname)
        filename = f"{resource_name}_{countryiso}.csv"
        resourcedata = {
            "name": record[originating_residing]["title"].format(**format_parameters),
            "description": record[originating_residing]["description"].format(
                **format_parameters
            ),
        }
        resourcedata["name"] = resourcedata["name"].replace(
            "residing in World", "(Global)"
        )

        #        quickcharts = {
        #            'cutdown': 2,
        #            'cutdownhashtags': ['#date+year+end', '#adm1+name', '#affected+killed'],
        #        }
        rowit = RowIterator(headers[resource_name], resource_rows).with_fields(fields)
        success, results = dataset.generate_resource_from_iterator(
            rowit.headers(),
            rowit,
            rowit.hxltags_mapping(),
            folder,
            filename,
            resourcedata,
            date_function=process_dates,
            #           quickcharts=quickcharts,
        )

        if success is False:
            logger.warning(f"{countryname} - {resource_name}  has no data!")
        else:
            startdate = results["startdate"]
            if earliest_startdate is None or startdate < earliest_startdate:
                earliest_startdate = startdate
            enddate = results["enddate"]
            if latest_enddate is None or enddate > latest_enddate:
                latest_enddate = enddate

    if len(dataset.get_resources()) == 0:
        logger.error(f"{countryname}  has no data!")
        return None, None
    dataset.set_dataset_date_from_datetime(earliest_startdate, latest_enddate)
    if countryiso != WORLD:
        filename = "qc_data.csv"
        resourcedata = {
            "name": filename,
            "description": "QuickCharts data for %s" % countryname,
        }
        rowit = ListIterator(data =list(qc_rows.values()), headers=["Year"]).auto_headers().with_fields(fields)
        success, results = dataset.generate_resource_from_iterator(
            rowit.headers(),
            rowit,
            rowit.hxltags_mapping(),
            folder,
            filename,
            resourcedata,
            date_function=process_dates,
            #           quickcharts=quickcharts,
        )
        if success is False:
            logger.warning(f"QuickChart {countryname} - {resource_name}  has no data!")
#        dataset.generate_resource_from_rows(
#            folder, filename, rows, resourcedata, list(rows[0].keys())
#        )
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
