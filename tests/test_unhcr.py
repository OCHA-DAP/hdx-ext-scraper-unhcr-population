#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for UCDP

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from pathlib import Path

from unhcr import generate_dataset_and_showcase, get_countriesdata


class TestUNHCR:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            user_agent="test",
            hdx_key="12345",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations([{"name": "bgd", "title": "Bangladesh"}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "hxl"},
                {"name": "refugees"},
                {"name": "asylum"},
                {"name": "population"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }

    @pytest.fixture(scope="class")
    def data(self):
        download_url = (Path(__file__).resolve().parent / "fixtures").as_uri()
        files = dict(
            asylum_applications="HDX_AsylumApplications.csv",
            asylum_decisions="HDX_AsylumDecisions.csv",
            demographics="HDX_Demographics.csv",
            end_year_population_totals="HDX_EndYearPopulationTotals.csv",
            solutions="HDX_Solutions.csv",
        )

        print(download_url)
        return get_countriesdata(download_url, files, Download(user_agent="test"))

    def test_get_countriesdata(self, data):
        countries, headers, countriesdata = data
        assert len(headers) == 10
        assert headers["asylum_applications_refugees"] == [
            "Year",
            "ISO3CoO",
            "ISO3CoA",
            "CoA_name",
            "CoO_name",
            "ProcedureType",
            "ApplicationType",
            "ApplicationDataType",
            "ApplicationAveragePersonsPerCase",
            "Applications",
        ]
        assert len(countriesdata) == 73
        assert countriesdata["BGD"]["asylum_applications_refugees"][1] == {
            "Year": "2008",
            "ISO3CoO": "BGD",
            "ISO3CoA": "IRN",
            "CoA_name": "Iran (Islamic Republic of)",
            "CoO_name": "Bangladesh",
            "ProcedureType": "U",
            "ApplicationType": "N",
            "ApplicationDataType": "P",
            "ApplicationAveragePersonsPerCase": "3.3",
            "Applications": "5",
        }

    def test_generate_dataset_and_showcase(self, configuration, data):
        with temp_dir("ucdp") as folder:
            countries, headers, countriesdata = data
            index = [i for i, c in enumerate(countries) if c["iso3"] == "BGD"][0]
            dataset, showcase = generate_dataset_and_showcase(
                folder, countries[index], countriesdata["BGD"], headers
            )
            assert dataset["name"] == "unhcr-population-data-for-bangladesh"
            assert dataset["title"] == "Bangladesh - Data on UNHCR population"

            resources = dataset.get_resources()
            assert len(resources) == 4  # should be 10 if all data is available

            assert showcase["name"] == "unhcr-population-data-for-bangladesh-showcase"
