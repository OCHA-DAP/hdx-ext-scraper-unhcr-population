#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for UCDP

'''
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from pathlib import Path

from unhcr import generate_dataset_and_showcase, get_countriesdata


class TestUNHCR:
    @pytest.fixture(scope='class')
    def configuration(self):
        Configuration._create(
            user_agent='test',
            hdx_key='12345',
            project_config_yaml=join('tests', 'config', 'project_configuration.yml'),
        )
        Locations.set_validlocations([{'name': 'bgd', 'title': 'Bangladesh'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            'tags': [
                {'name': 'hxl'},
                {'name': 'refugees'},
                {'name': 'asylum'},
                {'name': 'population'},
            ],
            'id': '4e61d464-4943-4e97-973a-84673c1aaa87',
            'name': 'approved',
        }
        return Configuration.read()

    @pytest.fixture(scope='class')
    def data(self, configuration):
        resources = configuration['resources']
        download_url = (Path(__file__).resolve().parent / 'fixtures').as_uri()

        print(download_url)
        return get_countriesdata(download_url, resources, Download(user_agent='test'))

    def test_get_countriesdata(self, data):
        countries, headers, countriesdata, qc_rows = data
        assert len(headers) == 10
        assert headers['asylum_applications_residing'] == [
            'Year',
            'ISO3CoO',
            'ISO3CoA',
            'CoO_name',
            'CoA_name',
            'ProcedureType',
            'ApplicationType',
            'ApplicationDataType',
            'ApplicationAveragePersonsPerCase',
            'Applications',
        ]
        assert len(countriesdata) == 73
        assert countriesdata['BGD']['asylum_applications_originating'][1] == {
            'Year': '2008',
            'ISO3CoO': 'BGD',
            'ISO3CoA': 'IRN',
            'CoO_name': 'Bangladesh',
            'CoA_name': 'Iran (Islamic Republic of)',
            'ProcedureType': 'U',
            'ApplicationType': 'N',
            'ApplicationDataType': 'P',
            'ApplicationAveragePersonsPerCase': '3.3',
            'Applications': '5',
        }
        assert len(qc_rows) == 1067
        assert qc_rows['2019_AFG_PAK'] == {
            'Year': '2019',
            'ISO3CoO': 'AFG',
            'ISO3CoA': 'PAK',
            'CoO_name': 'Afghanistan',
            'CoA_name': 'Pakistan',
            'Applications_incoming': '3545',
            'Applications_outgoing': '3545',
            'ASY_incoming': '8406',
            'ASY_outgoing': '8406',
            'IDP_incoming': '0',
            'IDP_outgoing': '0',
            'OOC_incoming': '0',
            'OOC_outgoing': '0',
            'REF_incoming': '1419084',
            'REF_outgoing': '1419084',
            'STA_incoming': '0',
            'STA_outgoing': '0',
            'VDA_incoming': '0',
            'VDA_outgoing': '0'
        }

    def test_generate_dataset_and_showcase(self, configuration, data):
        with temp_dir('ucdp') as folder:
            resources = configuration['resources']
            fields = configuration['fields']
            countries, headers, countriesdata, qc_rows = data
            index = [i for i, c in enumerate(countries) if c['iso3'] == 'BGD'][0]
            dataset, showcase = generate_dataset_and_showcase(
                folder, countries[index], countriesdata['BGD'], qc_rows, headers, resources, fields
            )
            assert dataset['name'] == 'unhcr-population-data-for-bgd'
            assert dataset['title'] == 'Bangladesh - Data on forcibly displaced populations and stateless persons'

            resources = dataset.get_resources()
            assert len(resources) == 5  # should be 10 if all data is available

            assert showcase['name'] == 'unhcr-population-data-for-bgd-showcase'
