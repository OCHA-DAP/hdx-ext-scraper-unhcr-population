### Collector for UNHCR Population Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-unhcr-population/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-unhcr-population/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-unhcr-population/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-unhcr-population?branch=main)

This script connects to the UNHCR internal API and extracts data from each dataset creating a dataset per country in HDX.  


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-unhcr-population** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY