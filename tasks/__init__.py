import logging

from invoke import task

from .import_articles import import_articles
from .util import connect_to_graph_db

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


@task
def open_browser(ctx):
    """Open the graph database in the browser."""
    graph = connect_to_graph_db()
    graph.open_browser()
