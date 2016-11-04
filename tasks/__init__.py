import logging

from invoke import task

# Import invoke tasks
from .import_articles import import_articles
# Import Node objects. (Path likely to change.)
from .import_articles import Revision, Wikitext
# Import util functions for simple tasks written here.
from .util import connect_to_graph_db

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


@task
def open_browser(ctx):
    """Open the graph database in the browser."""
    graph = connect_to_graph_db()
    graph.open_browser()
