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

# import liquer.ext.lq_hxl
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
    "Config data structure containing the field name conversions and hxl tags"
    return yaml.load(open("../config/project_configuration.yml"))


@first_command
def asylum_applications():
    "Return raw asylum application data"
    return pd.read_csv(f"{data_path()}/HDX_AsylumApplications.csv")


@first_command
def asylum_decisions():
    "Return raw asylum decision data"
    return pd.read_csv(f"{data_path()}/HDX_AsylumDecisions.csv")


@first_command
def demographics():
    "Return raw demographic data"
    return pd.read_csv(f"{data_path()}/HDX_Demographics.csv")


@first_command
def population_totals():
    "Return raw end year population totals data"
    return pd.read_csv(f"{data_path()}/HDX_EndYearPopulationTotals.csv")


@first_command
def solutions():
    "Return raw solutions data"
    return pd.read_csv(f"{data_path()}/HDX_Solutions.csv")


@first_command
def countries():
    "Table of countries (iso3 and country name) used in the data"
    countries = set()
    for data in [
        "asylum_applications",
        "asylum_decisions",
        "demographics",
        "population_totals",
        "solutions",
    ]:
        df = evaluate(data).get()
        countries.update(df.ISO3CoO)
        countries.update(df.ISO3CoA)
    countries = sorted(countries)
    countrynames = [
        Country.get_country_name_from_iso3(countryiso) for countryiso in countries
    ]
    return pd.DataFrame(dict(iso3=countries, country=countrynames))


@command
def convert(df, add_hxltags=True):
    "Rename fields and optionally add hxl tags"
    fields = config()["fields"]
    columns = convert_headers(df.columns, fields)
    mapping = hxltags_mapping(fields)
    if add_hxltags:
        hxltags = [{c: mapping.get(c, "") for c in columns}]
    else:
        hxltags = []
    data = hxltags + list(convert_fields_in_iterator(df.to_dict("records"), fields))
    return pd.DataFrame(data, columns=columns)


def country_columns(df):
    """Return two column names - ouuntry of origin and country of asylum.
    Names of the columns depend on whether it is raw data or if the column names have been "converted". 
    """
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
    """Keep only rows for one country - originating or residing.
    A column "Country" with column names of the variable country (residing or originating) is created.
    """
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    coo, coa = country_columns(df)
    is_originating = country_of.lower().startswith("o")

    fixed_country_column = coo if is_originating else coa
    variable_country_column = coa if is_originating else coo
    df = df.loc[df[fixed_country_column] == countryiso, :]
    df["Country"] = [country_map.get(c, "") for c in df[variable_country_column]]
    return df


@command
def totals_per(df, column="Country"):
    "Group by a specific column and sum up each group (all numeric columns)"
    df = df.groupby([column]).sum().reset_index()
    return df.sort_values(by=column)


@command
def last_year(df):
    "Only keep the last year in the data"
    return df.loc[df.Year == df.Year.max(), :]


@command
def pie(df, values_column, names_column="Country"):
    "Create a pie chart (plotly html)"
    assert values_column in df.columns
    assert names_column in df.columns
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'
    return px.pie(
        df, values=values_column, names=names_column, width=600, height=400
    ).to_html(full_html=False, include_plotlyjs="cdn")


@command
def bar(df, y_column, x_column="Year"):
    "Create a bar chart (plotly html)"
    if x_column not in df.columns:
        return f'<div class="alert alert-warning" role="alert">{x_column} not in dataframe</div>'
    if y_column not in df.columns:
        return f'<div class="alert alert-warning" role="alert">{y_column} not in dataframe</div>'
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'
    return px.bar(df, x=x_column, y=y_column, width=600, height=400).to_html(
        full_html=False, include_plotlyjs="cdn"
    )


@command
def decision_bar(df, x_column="Country"):
    "Create a bar chart for decision categories"
    y = ["Recognized", "Complementary Protection", "Otherwise Closed", "Rejected"]
    y = [c for c in y if c in df.columns]
    if x_column not in df.columns:
        return f'<div class="alert alert-warning" role="alert">{x_column} not in dataframe</div>'
    if len(y) == 0:
        return f'<div class="alert alert-warning" role="alert">Decision category columns missing</div>'
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'

    return px.bar(df, x=str(x_column), y=y, width=600, height=400,).to_html(
        full_html=False, include_plotlyjs="cdn"
    )


@command
def demographics_bar(df, x_column="Country", detailed=False):
    "Create a bar chart for demographics categories"
    if detailed:
        y = [
            "Female 0-4",
            "Female 5-11",
            "Female 12-17",
            "Female 18-59",
            "Female 60 or more",
            "Female Unknown",
            "Male 0-4",
            "Male 5-11",
            "Male 12-17",
            "Male 18-59",
            "Male 60 or more",
            "Male Unknown",
        ]
    else:
        y = ["Female Total", "Male Total"]
    y = [c for c in y if c in df.columns]
    if x_column not in df.columns:
        return f'<div class="alert alert-warning" role="alert">{x_column} not in dataframe</div>'
    if len(y) == 0:
        return f'<div class="alert alert-warning" role="alert">Demographics category columns missing</div>'
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'

    "Create a bar chart for decision categories"
    return px.bar(df, x=str(x_column), y=y, width=600, height=400,).to_html(
        full_html=False, include_plotlyjs="cdn"
    )


@command
def population_bar(df, x_column="Country"):
    "Create a bar chart for population totals categories"
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'
    return px.bar(
        df,
        x=str(x_column),
        y=[
            "Refugees",
            "Internally Displaced Persons",
            "Asylum-seekers",
            "Others of Concern to UNHCR",
            "Stateless persons",
            "Venezuelans Displaced Abroad",
        ],
        width=600,
        height=400,
    ).to_html(full_html=False, include_plotlyjs="cdn")


@command
def solutions_bar(df, x_column="Country"):
    "Create a bar chart for solutions categories"
    if len(df) == 0:
        return f'<div class="alert alert-warning" role="alert">No data</div>'
    return px.bar(
        df,
        x=str(x_column),
        y=["Resettlement arrivals", "Naturalisation", "Refugee returns", "IDP returns"],
        width=600,
        height=400,
    ).to_html(full_html=False, include_plotlyjs="cdn")


@first_command
def report_applications(countryiso):
    "Create a report as html for asylum applications for a specific country"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(
        f"""
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
    """
    )


@first_command
def report_decisions(countryiso):
    "Create a report as html for asylum decisions for a specific country"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(
        f"""
<html>
<head>
  <title>Asylum Decisions - {country_name}</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Asylum Decisions - {country_name}</h1>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Decisions by country for refugees originating from {country_name}</h4>
        $asylum_decisions/convert-f/filter_country-{countryiso}-originating/totals_per-Country/decision_bar-Country$
      </div>
      <div class="col-sm">
        <h4>Decisions by country for refugees residing in {country_name}</h4>
        $asylum_decisions/convert-f/filter_country-{countryiso}-residing/totals_per-Country/decision_bar-Country$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Decisions by year for refugees originating from {country_name}</h4>
        $asylum_decisions/convert-f/filter_country-{countryiso}-originating/totals_per-Year/decision_bar-Year$
      </div>
      <div class="col-sm">
        <h4>Decisions by year for refugees residing in {country_name}</h4>
        $asylum_decisions/convert-f/filter_country-{countryiso}-residing/totals_per-Year/decision_bar-Year$
      </div>
    </div>
  </div>
</body>
</html>
    """
    )


@first_command
def report_demographics(countryiso):
    "Create a report as html for demographics for a specific country"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(
        f"""
<html>
<head>
  <title>Demographics - {country_name}</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Demographics - {country_name}</h1>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Demographics by country for refugees originating from {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-originating/totals_per-Country/demographics_bar-Country-f$
      </div>
      <div class="col-sm">
        <h4>Demographics by country for refugees residing in {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-residing/totals_per-Country/demographics_bar-Country-f$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Demographics by year for refugees originating from {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-originating/totals_per-Year/demographics_bar-Year-f$
      </div>
      <div class="col-sm">
        <h4>Demographics by year for refugees residing in {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-residing/totals_per-Year/demographics_bar-Year-f$
      </div>
    </div>
  </div>

  <h2>Detailed Demographics</h2>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Demographics by country for refugees originating from {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-originating/totals_per-Country/demographics_bar-Country-t$
      </div>
      <div class="col-sm">
        <h4>Demographics by country for refugees residing in {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-residing/totals_per-Country/demographics_bar-Country-t$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Demographics by year for refugees originating from {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-originating/totals_per-Year/demographics_bar-Year-t$
      </div>
      <div class="col-sm">
        <h4>Demographics by year for refugees residing in {country_name}</h4>
        $demographics/convert-f/filter_country-{countryiso}-residing/totals_per-Year/demographics_bar-Year-t$
      </div>
    </div>
  </div>

</body>
</html>
    """
    )


@first_command
def report_population_totals(countryiso):
    "Create a report as html for population totals for a specific country"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(
        f"""
<html>
<head>
  <title>Population Totals - {country_name}</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Population Totals - {country_name}</h1>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Last year population totals - refuges originating from {country_name}</h4>
        $population_totals/convert-f/filter_country-{countryiso}-originating/last_year/totals_per-Country/population_bar-Country$
      </div>
      <div class="col-sm">
        <h4>Last year population totals - refugees residing in {country_name}</h4>
        $population_totals/convert-f/filter_country-{countryiso}-residing/last_year/totals_per-Country/population_bar-Country$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Population totals by year for refugees originating from {country_name}</h4>
        $population_totals/convert-f/filter_country-{countryiso}-originating/totals_per-Year/population_bar-Year$
      </div>
      <div class="col-sm">
        <h4>Population totals by year for refugees residing in {country_name}</h4>
        $population_totals/convert-f/filter_country-{countryiso}-residing/totals_per-Year/population_bar-Year$
      </div>
    </div>
  </div>
</body>
</html>
    """
    )


@first_command
def report_solutions(countryiso):
    "Create a report as html for solutions for a specific country"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    country_name = country_map.get(countryiso)
    return evaluate_template(
        f"""
<html>
<head>
  <title>Solutions - {country_name}</title>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">

</head>

<body>
  <h1>Solutions - {country_name}</h1>

  <div class="container">
    <div class="row">
      <div class="col-sm">
        <h4>Solutions by country for refugees originating from {country_name}</h4>
        $solutions/convert-f/filter_country-{countryiso}-originating/totals_per-Country/solutions_bar-Country$
      </div>
      <div class="col-sm">
        <h4>Solutions by country for refugees residing in {country_name}</h4>
        $solutions/convert-f/filter_country-{countryiso}-residing/totals_per-Country/solutions_bar-Country$
      </div>
    </div>
    <div class="row">
      <div class="col-sm">
        <h4>Solutions by year for refugees originating from {country_name}</h4>
        $solutions/convert-f/filter_country-{countryiso}-originating/totals_per-Year/solutions_bar-Year$
      </div>
      <div class="col-sm">
        <h4>Solutions by year for refugees residing in {country_name}</h4>
        $solutions/convert-f/filter_country-{countryiso}-residing/totals_per-Year/solutions_bar-Year$
      </div>
    </div>
  </div>
</body>
</html>
    """
    )


@first_command
def reports():
    "Table of all reports for all countries"
    countries_table = evaluate(
        "countries"
    ).get()  # evaluate rather than call, so that the cache is used
    country_map = dict(zip(countries_table.iso3, countries_table.country))
    rows = "".join(
        f"""    <tr>
      <th>{row.country}</th>
      <td>{row.iso3}</td>
      <td>
      <a href="/liquer/q/report_applications-{row.iso3}/applications_{row.iso3}.html">applications</a>,
      <a href="/liquer/q/report_decisions-{row.iso3}/decisions_{row.iso3}.html">decisions</a>,
      <a href="/liquer/q/report_demographics-{row.iso3}/demographics_{row.iso3}.html">demographics</a>,
      <a href="/liquer/q/report_population_totals-{row.iso3}/population_totals_{row.iso3}.html">population totals</a>,
      <a href="/liquer/q/report_solutions-{row.iso3}/solutions_{row.iso3}.html">solutions</a>
      </td>
    </tr>"""
        for index, row in countries_table.iterrows()
    )

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
    "Convinience function for creating menus"
    menu = get_vars().get("menu", [])
    try:
        item_number = [i for i, item in enumerate(menu) if item["title"] == title][0]
    except:
        menu.append(dict(title=title, items=[]))
        item_number = len(menu) - 1
    menu[item_number]["items"].append(dict(title=subtitle, link=link))
    set_var("menu", menu)


add_menuitem("Reports", "Reports by Country", "reports/reports_by_country.html")
add_menuitem("Reports", "Countries", "countries")
add_menuitem(
    "Asylum Applications", "Asylum applications raw data", "asylum_applications"
)
add_menuitem(
    "Asylum Applications", "Asylum applications", "asylum_applications/convert"
)
add_menuitem("Asylum Decisions", "Asylum decisions raw data", "asylum_decisions")
add_menuitem("Asylum Decisions", "Asylum decisions", "asylum_decisions/convert")
add_menuitem("Demographics", "Demographics raw data", "demographics")
add_menuitem("Demographics", "Demographics", "demographics/convert")
add_menuitem(
    "Population Totals", "End Year Population Totals raw data", "population_totals"
)
add_menuitem(
    "Population Totals", "End Year Population Totals", "population_totals/convert"
)
add_menuitem("Solutions", "Solutions raw data", "solutions")
add_menuitem("Solutions", "Solutions", "solutions/convert")

# add_menuitem(
#    "Help", "Repository", "https://github.com/orest-d/hdx-scraper-unhcr-population"
# )
# add_menuitem("Help", "Commands", "ns-meta/flat_commands_nodoc/to_df")
# add_menuitem("Help", "LiQuer Homepage", "https://orest-d.github.io/liquer/")


@app.route("/", methods=["GET", "POST"])
@app.route("/index.html", methods=["GET", "POST"])
def index():
    """Redirect to a LiQuer web-application page"""
    return redirect("/liquer/static/index.html")


# set_cache(FileCache("cache"))
set_cache(MemoryCache())

if __name__ == "__main__":
    webbrowser.open("http://localhost:5000")
    app.run(debug=True, threaded=False)
