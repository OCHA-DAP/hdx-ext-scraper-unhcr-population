#!/usr/bin/python
"""
UNHCR:
-----

Generates API urls from the UNHCR data.

See: https://github.com/OCHA-DAP/hdx-scraper-unhcr-population
(formerly): https://github.com/orest-d/hdx-scraper-unhcr-population/tree/master
Output examples:
Aruba: https://feature.data-humdata-org.ahconu.org/dataset/unhcr-population-data-for-abw
Afghanistan: https://feature.data-humdata-org.ahconu.org/dataset/unhcr-population-data-for-afg
Andorra: https://feature.data-humdata-org.ahconu.org/dataset/unhcr-population-data-for-and
Anguilla: https://feature.data-humdata-org.ahconu.org/dataset/unhcr-population-data-for-aia

"""

import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

from fields import ListIterator, RowIterator
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)

WORLD = "world"

# Dec-2020 - add a switch for the latest year and if the data is ASR or MYSR
# If MYSR, then the date in the latest year should be 30-June not 31-Dec
LATEST_YEAR = 2025
# 2024
# 2023
# 2022
# 2020
IS_ASR = False
    #True



# NOTE - change also the three references to mid/end year in hdx_resource_view_static
# End-year data (mid-year for the latest year)
# Latest mid-year data

# Check also we have the latest version of HDX (see the link above)
# and https://pypi.org/project/python-slugify/

# IS_ASR = True
###### Remember also to review the caveats in the hdx_dataset_static.yml #####

# The data is sourced from....


# -----------------------------------------------------------------------------------------------------------------------------------------------------
def get_countriesdata(download_url, resources, downloader):
    countriesdata = {WORLD: {}}
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
            {column for column in headers if column in ["ISO3CoO", "ISO3CoA"]}
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
                #                countryname = Country.get_country_name_from_iso3(countryiso)
                countryname = Get_Country_Name_From_ISO3_Extended(countryiso)
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

    # June-22 - seems like we have some odd blank / null entries that need fixing here
    # This line should remove them
    print("Removing NULL countries")
    print(len(countries))
    countries = {x for x in countries if x[0] is not None}
    print(len(countries))

    # Then produce a sorted list...
    countries = [{"iso3": WORLD, "countryname": "World"}] + [
        {"iso3": x[0], "countryname": x[1]} for x in sorted(list(countries))
    ]
    return countries, all_headers, countriesdata


# -----------------------------------------------------------------------------------------------------------------------------------------------------
def generate_dataset_and_showcase(
    folder, country, countrydata, headers, resources, fields
):
    """ """
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title_text = "Data on forcibly displaced populations and stateless persons"
    if countryname == "World":
        title = f"{title_text} (Global)"
    else:
        title = f"{countryname} - {title_text}"
    logger.info(f"Creating dataset: {title}")
    slugified_name = slugify(f"UNHCR Population Data for {countryiso}").lower()
    dataset = Dataset({"name": slugified_name, "title": title})
    dataset.set_maintainer("8d70b12b-7247-48d2-b426-dbb4bf82eb7c")
    dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
    dataset.set_expected_update_frequency("Every year")
    dataset.set_subnational(True)
    if countryiso == WORLD:
        dataset.add_other_location("world")
    # Feb-26 - add exception for STA
    elif countryiso == "STA":
        print("Skipping trying to add STA country")
        return None, None
    else:
        # Check for unknown country names
        try:
            dataset.add_country_location(countryiso)
        except HDXError:
            logger.error(f"{countryname} ({countryiso})  not recognised!")
            return None, None

    tags = [
        "refugees",
        "asylum seekers",
        "internally displaced persons-idp",
        "stateless persons",
        "population",
    ]
    dataset.add_tags(tags)

    def process_dates(row):
        year = int(row["Year"])
        startdate = datetime(year, 1, 1, tzinfo=timezone.utc)
        # For mid-year data it should be 30-June...
        # enddate = datetime(year, 12, 31, tzinfo=timezone.utc)
        if IS_ASR is False and year == LATEST_YEAR:
            enddate = datetime(year, 6, 30, tzinfo=timezone.utc)
        else:
            enddate = datetime(year, 12, 31, tzinfo=timezone.utc)
        return {"startdate": startdate, "enddate": enddate}

    earliest_startdate = None
    latest_enddate = None
    for resource_name, resource_rows in countrydata.items():
        resource_id = "_".join(resource_name.split("_")[:-1])
        originating_residing = resource_name.split("_")[-1]  # originating or residing

        #print("Looping through the resources - this is the current one: ", resource_id)
        #print(resources[resource_id])
        record = resources[resource_id]

        if (
            countryiso == WORLD
        ):  # refugees and asylum applicants contain the same data for WORLD
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
        rowit = RowIterator(headers[resource_name], resource_rows).with_fields(fields)
        success, results = dataset.generate_resource(
            folder,
            filename,
            rowit,
            resourcedata,
            rowit.headers(),
            date_function=process_dates,
            encoding="utf-8",
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
    dataset.set_time_period(earliest_startdate, latest_enddate)
    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": title,
            "notes": f"UNHCR Population Data Dashboard for {countryname}",
            "url": "https://www.unhcr.org/refugee-statistics/",
            "image_url": "https://www.unhcr.org/assets/img/unhcr-logo.png",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------
def Get_Country_Name_From_ISO3_Extended(countryISO):
    """
    Gets country name from iso3 with UNHCR specific changes
    """

    countryName = ""

    # June-22 - This function has been updated to include a to upper without a check on if the data is null or not
    # So we need to wrap it in a try catch
    try:
        countryName = Country.get_country_name_from_iso3(countryISO)
    except:
        print("Failed to get the country from get_country_name_from_iso3.")

    # Now lets try to find it for the three typical non-standard codes
    if countryName is None or countryName == "":
        print("Non-standard ISO code:", countryISO)

        if countryISO == "UKN":
            countryName = "Various / unknown"
        elif countryISO == "STA":
            countryName = "Stateless"
        elif countryISO == "TIB":
            countryName = "Tibetan"
        else:
            print("!!SERIOUS!! Unknown ISO code identified:", countryISO)
            # Lets add a sensible default here...
            countryName = "Various / unknown"

    return countryName
