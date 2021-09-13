#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging

# To switch on the structure of the file paths depending on the operating system
import sys

# Added Dec-2020
from os import getenv
from os.path import join
from pathlib import Path

from hdx.facades.simple import facade
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir
from hdx.utilities.text import multiple_replace

from unhcr import generate_dataset_and_showcase, get_countriesdata

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unhcr-population"


def main():
    """Generate dataset and create it in HDX"""
    configuration = Configuration.read()
    resources = configuration["resources"]
    fields = configuration["fields"]

    # print out the WHERE TO START Parameter
    print("Starting at country (WHERETOSTART): ", getenv("WHERETOSTART"))

    # Switch on platforms as the way the download_url is consumed seems to be inconsistent
    if sys.platform == "win32":
        # And just as it comes on Windows (set in the .hdx_configuration.yml file which is outside of the project, typically in the users home directory)
        print(
            "Using the following data directory: ", configuration["hdx_data_directory"]
        )
        download_url = configuration["hdx_data_directory"]
    #'/Dropbox/UNHCR Statistics/Data/HDX/'
    else:
        # Set the download_url as a path on linux
        download_url = Path("data").resolve().as_uri()

    with Download() as downloader:
        countries, headers, countriesdata, qc_rows = get_countriesdata(
            download_url, resources, downloader
        )
        logger.info(f"Number of countries: {len(countriesdata)}")
        for info, country in progress_storing_tempdir(
            "UNHCR_population", countries, "iso3"
        ):
            folder = info["folder"]

            countryiso = country["iso3"]
            dataset, showcase, bites_disabled = generate_dataset_and_showcase(
                folder,
                country,
                countriesdata[countryiso],
                qc_rows,
                headers,
                resources,
                fields,
            )
            if dataset:
                dataset.update_from_yaml()
                dataset["notes"] = dataset["notes"].replace(
                    "\n", "  \n"
                )  # ensure markdown has line breaks
                resourceview = dataset.generate_resource_view(
                    -1, bites_disabled=bites_disabled
                )
                if resourceview:
                    resourceview["hxl_preview_config"] = multiple_replace(
                        resourceview["hxl_preview_config"],
                        {
                            "{{#country+iso}}": countryiso,
                            "{{#country+name}}": country["countryname"],
                        },
                    )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script="UNHCR population",
                    batch=info["batch"],
                )
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent="UNHCR_POPULATION",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
