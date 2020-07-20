import sys

sys.path.append("..")

import pandas as pd
import logging
import liquer.blueprint as bp
import webbrowser
from flask import Flask, make_response, redirect
from liquer.cache import FileCache, set_cache, MemoryCache
from liquer.state import set_var, get_vars
from liquer import *
from fields import convert_headers, convert_fields_in_iterator, hxltags_mapping
import liquer.ext.basic
import liquer.ext.meta
import liquer.ext.lq_pandas
import liquer.ext.lq_hxl
import liquer.ext.lq_python
import liquer.ext.lq_pygments
import yaml
import os.path
from hdx.location.country import Country
import plotly.express as px

app = Flask(__name__)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.setLevel(logging.INFO)
url_prefix = "/liquer"
app.register_blueprint(bp.app, url_prefix=url_prefix)
set_var("api_path", url_prefix + "/q/")
set_var("server", "http://localhost:5000")

def data_path():
    if os.path.exists("../data/HDX_AsylumApplications.csv"):
        return "../data"
    else:
        return "../tests/fixtures"

@first_command
def config():
    return yaml.load(open("../config/project_configuration.yml"))


@first_command
def asylum_applications():
    return pd.read_csv(f"{data_path()}/HDX_AsylumApplications.csv")


@first_command
def asylum_decisions():
    return pd.read_csv(f"{data_path()}/HDX_AsylumDecisions.csv")


@first_command
def demographics():
    return pd.read_csv(f"{data_path()}/HDX_Demographics.csv")


@first_command
def population_totals():
    return pd.read_csv(f"{data_path()}/HDX_EndYearPopulationTotals.csv")


@first_command
def solutions():
    return pd.read_csv(f"{data_path()}/HDX_Solutions.csv")


@first_command
def countries():
    countries = set()
    for data in ["asylum_applications", "asylum_decisions", "demographics",
"population_totals",
"solutions"]:
        df = evaluate(data).get()
        countries.update(df.ISO3CoO)
        countries.update(df.ISO3CoA)
    countries = sorted(countries)
    countrynames = [Country.get_country_name_from_iso3(countryiso) for countryiso in countries]
    return pd.DataFrame(dict(iso3=countries, country=countrynames))


@command
def convert(df, add_hxltags=True):
    fields = config()["fields"]
    columns = convert_headers(df.columns, fields)
    mapping = hxltags_mapping(fields)
    if add_hxltags:
        hxltags = [{c:mapping.get(c,"") for c in columns}]
    else:
        hxltags = []
    data = hxltags+list(convert_fields_in_iterator(df.to_dict("records"), fields))
    return pd.DataFrame(data, columns=columns)

def country_columns(df):
    fields = config()["fields"]
    coo = "ISO3CoO"
    coa = "ISO3CoA"
    if coo not in df.columns:
        coo = convert_headers([coo], fields)[0]
    if coa not in df.columns:
        coa = convert_headers([coa], fields)[0]
    return coo, coa

@command
def filter_country(df, countryiso, country_of="originating"):
    countries_table = evaluate("countries").get() # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    coo, coa = country_columns(df)
    is_originating = country_of.lower().startswith("o")
    
    fixed_country_column = coo if is_originating else coa
    variable_country_column = coa if is_originating else coo
    df = df.loc[df[fixed_country_column]==countryiso, :]
    df["Country"]=[country_map.get(c, "") for c in df[variable_country_column]]
    return df

@command
def totals_per(df, column="Country"):
    df = df.groupby([column]).sum().reset_index()
    return df.sort_values(by=column)

@command
def pie(df, values_column, names_column="Country"):
    return px.pie(df, values=values_column, names=names_column, width=600, height=400
    ).to_html(full_html=False, include_plotlyjs="cdn")

@command
def bar(df, y_column, x_column="Year"):
    return px.bar(df, x=x_column, y=y_column, width=600, height=400
    ).to_html(full_html=False, include_plotlyjs="cdn")

@first_command
def report_applications(countryiso):
    countries_table = evaluate("countries").get() # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(f"""
<html>
<head>
  <title>Asylum Applications - {country_name}</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Asylum Applications - {country_name}</h1>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Applications by country for refugees originating from {country_name}</h4>
        $asylum_applications/convert-f/filter_country-{countryiso}-originating/totals_per-Country/pie-Number~.of~.Applications$
      </div>
      <div class="col-sm">
        <h4>Applications by country for refugees residing in {country_name}</h4>
        $asylum_applications/convert-f/filter_country-{countryiso}-residing/totals_per-Country/pie-Number~.of~.Applications$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Applications by year for refugees originating from {country_name}</h4>
        $asylum_applications/convert-f/filter_country-{countryiso}-originating/totals_per-Year/bar-Number~.of~.Applications$
      </div>
      <div class="col-sm">
        <h4>Applications by year for refugees residing in {country_name}</h4>
        $asylum_applications/convert-f/filter_country-{countryiso}-residing/totals_per-Year/bar-Number~.of~.Applications$
      </div>
    </div>

    
  </div>
</body>
</html>
    """)

@first_command
def reports():
    countries_table = evaluate("countries").get() # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    rows="".join(
f"""    <tr>
      <th>{row.country}</th>
      <td>{row.iso3}</td>
      <td>
      <a href="/liquer/q/report_applications-{row.iso3}/applications_{row.iso3}.html">applications</a>
      </td>
    </tr>""" for index, row in countries_table.iterrows())

    return f"""
<html>
<head>
  <title>Reports</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Reports by country</h1>
<table class="table">
  <thead>
    <tr>
      <th scope="col">Country</th>
      <th scope="col">ISO3</th>
      <th scope="col">Reports</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>  
</body>
</html>
    """

def add_menuitem(title, subtitle, link):
    menu = get_vars().get("menu", [])
    try:
        item_number = [i for i, item in enumerate(menu) if item["title"] == title][0]
    except:
        menu.append(dict(title=title, items=[]))
        item_number = len(menu) - 1
    menu[item_number]["items"].append(dict(title=subtitle, link=link))
    set_var("menu", menu)

add_menuitem("Reports", "Countries", "countries")
add_menuitem("Reports", "Reports by Country", "reports/reports_by_country.html")
add_menuitem("Asylum Applications", "Asylum applications raw data", "asylum_applications")
add_menuitem("Asylum Applications", "Asylum applications", "asylum_applications/convert")
add_menuitem("Asylum Decisions", "Asylum decisions raw data", "asylum_decisions")
add_menuitem("Asylum Decisions", "Asylum decisions", "asylum_decisions/convert")
add_menuitem("Demographics", "Demographics raw data", "demographics")
add_menuitem("Demographics", "Demographics", "demographics/convert")
add_menuitem("Population Totals", "End Year Population Totals raw data", "population_totals")
add_menuitem("Population Totals", "End Year Population Totals", "population_totals/convert")
add_menuitem("Solutions", "Solutions raw data", "solutions")
add_menuitem("Solutions", "Solutions raw data", "solutions/convert")

add_menuitem(
    "Help", "Repository", "https://github.com/orest-d/hdx-scraper-unhcr-population"
)
add_menuitem("Help", "Commands", "ns-meta/flat_commands_nodoc/to_df")
add_menuitem("Help", "LiQuer Homepage", "https://orest-d.github.io/liquer/")


@app.route("/", methods=["GET", "POST"])
@app.route("/index.html", methods=["GET", "POST"])
def index():
    """Link to a LiQuer main service page"""
    return redirect("/liquer/static/index.html")


# set_cache(FileCache("cache"))
set_cache(MemoryCache())

if __name__ == "__main__":
    webbrowser.open("http://localhost:5000")
    app.run(debug=True, threaded=False)
