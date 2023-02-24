#!/usr/bin/python
"""
UNHCR:
-----

Generates HXlated API urls from the UNHCR data.

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
LATEST_YEAR = 2021
# 2020
# IS_ASR = False
IS_ASR = True
###### Remember also to review the caveats in the hdx_dataset_static.yml #####

# The data is sourced from....

# -----------------------------------------------------------------------------------------------------------------------------------------------------
def get_countriesdata(download_url, resources, downloader):
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
                year = row["Year"]
                origin = row["ISO3CoO"]
                asylum = row["ISO3CoA"]
                row_key = f"{year}_{origin}_{asylum}"
                qc_row = qc_rows.get(row_key, dict())
                qc_row["Year"] = year
                qc_row["ISO3CoO"] = origin
                qc_row["ISO3CoA"] = asylum
                qc_row["CoO_name"] = Get_Country_Name_From_ISO3_Extended(origin)
                qc_row["CoA_name"] = Get_Country_Name_From_ISO3_Extended(asylum)
                # qc_row['CoO_name'] = Country.get_country_name_from_iso3(origin)
                # qc_row['CoA_name'] = Country.get_country_name_from_iso3(asylum)
                attributes = list()
                if countryiso == origin:
                    attributes.append("outgoing")
                if countryiso == asylum:
                    attributes.append("incoming")
                # Added HST June 2022
                for attribute in attributes:
                    for field in [
                        "Applications",
                        "REF",
                        "ASY",
                        "OIP",
                        "IDP",
                        "STA",
                        "OOC",
                        "HST",
                    ]:
                        value = row.get(field)
                        if value is None:
                            continue
                        qc_field = f"{field}_{attribute}"
                        qc_row[qc_field] = value
                qc_rows[row_key] = qc_row
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
    return countries, all_headers, countriesdata, qc_rows


# -----------------------------------------------------------------------------------------------------------------------------------------------------
def generate_dataset_and_showcase(
    folder, country, countrydata, qc_rows, headers, resources, fields
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
    dataset.set_expected_update_frequency("Every six months")
    dataset.set_subnational(True)
    if countryiso == WORLD:
        dataset.add_other_location("world")
    else:
        # Check for unknown country names
        try:
            dataset.add_country_location(countryiso)
        except HDXError:
            logger.error(f"{countryname} ({countryiso})  not recognised!")
            return None, None, None

    tags = [
        "hxl",
        "refugees",
        "asylum seekers",
        "internally displaced persons-idp",
        "stateless persons",
        "population",
    ]
    dataset.add_tags(tags)

    # Filter the quick chart data to only include the relevant data for the current country
    qcRowSubset = SubsetQuickChartData(country, qc_rows)

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
        success, results = dataset.generate_resource_from_iterator(
            rowit.headers(),
            rowit,
            rowit.hxltags_mapping(),
            folder,
            filename,
            resourcedata,
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
        return None, None, None
    dataset.set_reference_period(earliest_startdate, latest_enddate)
    bites_disabled = [True, True, True]
    if countryiso != WORLD:
        filename = "qc_data.csv"
        resourcedata = {
            "name": filename,
            "description": f"QuickCharts data for {countryname}",
        }

        rowit = (
            ListIterator(
                data=list(qcRowSubset.values()),
                headers=[
                    "Year",
                    "ISO3CoO",
                    "CoO_name",
                    "ISO3CoA",
                    "CoA_name",
                    "Displaced From",
                    "Displaced Stateless Within",
                    "Displaced Stateless From",
                ],
            )
            .auto_headers()
            .to_list_iterator()
        )
        years = sorted(set(rowit.column("Year")))[-10:]  # Last 10 years
        headers = rowit.headers()
        rowit = (
            rowit.select(
                lambda row, years=years: row.get("Year") in years
            )  # Restrict data to only last 10 years
            .with_sum_field(
                "Displaced From",
                "#affected+displaced+outgoing",
                [
                    x
                    for x in headers
                    if x.startswith(("REF", "ASY", "OIP")) and x.endswith("_outgoing")
                ],
            )
            .with_sum_field(
                "Displaced Stateless Within",
                "#affected+displaced+stateless+incoming",
                [
                    x
                    for x in headers
                    if x.startswith(("REF", "ASY", "IDP", "OIP", "STA"))
                    and x.endswith("_incoming")
                ],
            )
            .with_sum_field(
                "Displaced Stateless From",
                "#affected+displaced+stateless+outgoing",
                [
                    x
                    for x in headers
                    if x.startswith(("REF", "ASY", "IDP", "OIP", "STA"))
                    and x.endswith("_outgoing")
                ],
            )
            .with_fields(fields)
        )

        for row in rowit:
            if (
                row["Country of Origin Code"] == countryiso
                and row["Displaced From"] > 0
            ):
                bites_disabled[0] = False
            if row["Year"] != years[-1]:
                continue
            if (
                row["Country of Asylum Code"] == countryiso
                and row["Displaced Stateless Within"] > 0
            ):
                bites_disabled[1] = False
            if (
                row["Country of Origin Code"] == countryiso
                and row["Displaced Stateless From"] > 0
            ):
                bites_disabled[2] = False

        rowit.reset()
        success, results = dataset.generate_resource_from_iterator(
            rowit.headers(),
            rowit,
            rowit.hxltags_mapping(),
            folder,
            filename,
            resourcedata,
            date_function=process_dates,
            encoding="utf-8",
        )
        if success is False:
            logger.warning(f"QuickCharts {countryname} - {filename}  has no data!")
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
    return dataset, showcase, bites_disabled


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------
def SubsetQuickChartData(country, qc_rows):
    """
    Creates a subset of the quick chart data for a specific country.  The subset includes all those rows containing
    the given country either as the origin or as the country of asylum.
    """
    countryISO = country["iso3"]
    country["countryname"]

    # The new dictionary to store the subset of the data
    qcRowSubset = dict()

    # If this is the global dataset, return all, otherwise continue to filter by origin and asylum values
    # This should not be necessary as the call to this function is after the switch on world / non-world, but it makes sense to keep it.
    if countryISO == WORLD:
        print("Special case - processing the world")
        qcRowSubset = qc_rows
    else:
        # filter the data by iterating though the values
        for key, value in qc_rows.items():
            if value["ISO3CoO"] == countryISO or value["ISO3CoA"] == countryISO:
                qcRowSubset[key] = value

    print("Filtered ", countryISO, " to ", len(qcRowSubset), " rows")

    return qcRowSubset


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------
def Get_Country_Name_From_ISO3_Extended(countryISO):
    """
    Creates a subset of the quick chart data for a specific country.  The subset includes all those rows containing
    the given country either as the origin or as the country of asylum.
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
