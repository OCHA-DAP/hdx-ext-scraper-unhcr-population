import sys

sys.path.append("..")

import pandas as pd
import logging
import liquer.blueprint as bp
import webbrowser
from flask import Flask, make_response, redirect
from liquer.cache import FileCache, set_cache
from liquer.state import set_var, get_vars
from liquer import *
from fields import convert_headers, convert_fields_in_iterator
import liquer.ext.basic
import liquer.ext.meta
import liquer.ext.lq_pandas
import liquer.ext.lq_hxl
import liquer.ext.lq_python
import liquer.ext.lq_pygments
import yaml

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

@first_command
def config():
    return yaml.load(open("../config/project_configuration.yml"))

@first_command
def asylum_applications():
    return pd.read_csv("../data/HDX_AsylumApplications.csv")


@first_command
def asylum_decisions():
    return pd.read_csv("../data/HDX_AsylumDecisions.csv")


@first_command
def demographics():
    return pd.read_csv("../data/HDX_Demographics.csv")


@first_command
def population_totals():
    return pd.read_csv("../data/HDX_EndYearPopulationTotals.csv")


@first_command
def solutions():
    return pd.read_csv("../data/HDX_Solutions.csv")


@command
def hxltags(df):
    fields = config()["fields"]
    columns = convert_headers(df.columns, fields)
    data = list(convert_fields_in_iterator(df.to_dict("records"), fields))
    return pd.DataFrame(data, columns=columns)


def add_menuitem(title, subtitle, link):
    menu = get_vars().get("menu", [])
    try:
        item_number = [i for i, item in enumerate(menu) if item["title"] == title][0]
    except:
        menu.append(dict(title=title, items=[]))
        item_number = len(menu) - 1
    menu[item_number]["items"].append(dict(title=subtitle, link=link))
    set_var("menu", menu)


add_menuitem(
    "Asylum Applications", "Asylum applications raw data", "asylum_applications"
)
add_menuitem(
    "Asylum Applications", "Asylum applications", "asylum_applications/hxltags"
)
add_menuitem("Asylum Decisions", "Asylum decisions raw data", "asylum_decisions")
add_menuitem("Demographics", "Demographics raw data", "demographics")
add_menuitem(
    "Population Totals", "End Year Population Totals raw data", "population_totals"
)
add_menuitem("Solutions", "Solutions raw data", "solutions")

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

if __name__ == "__main__":
    webbrowser.open("http://localhost:5000")
    app.run(debug=True, threaded=False)
