#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser
from pathlib import Path

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from hdx.utilities.path import progress_storing_tempdir

from unhcr import generate_dataset_and_showcase, get_countriesdata

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unhcr-population"


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    resources = configuration["resources"]
    fields = configuration["fields"]
    source_directory = configuration["source_directory"]
    download_url = Path(source_directory).resolve().as_uri()
       
    with Download() as downloader:
        countries, headers, countriesdata = get_countriesdata(
            download_url, resources, downloader
        )
        logger.info("Number of countries: %d" % len(countriesdata))
        for info, country in progress_storing_tempdir(
            "UNHCR_population", countries, "iso3"
        ):
            folder = info["folder"]

            dataset, showcase = generate_dataset_and_showcase(
                folder, country, countriesdata[country["iso3"]], headers, resources, fields
            )
            if dataset:
                dataset.update_from_yaml()
                dataset["notes"] = dataset["notes"].replace(
                    "\n", "  \n"
                )  # ensure markdown has line breaks
                dataset.preview_off()
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script="HDX Scraper: UNHCR population",
                    batch=info["batch"],
                )
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        # WARNING hard coded user agent
        user_agent='UNHCR_POPULATION',
        project_config_yaml=join("config", "project_configuration.yml"),
    )
