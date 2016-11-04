import logging
import hashlib
from os import environ

from py2neo import Graph
import pywikibot

logger = logging.getLogger(__name__)


def connect_to_graph_db():
    password = environ.get('NEO4J_PASSWORD')
    if not password:
        raise AssertionError('must set NEO4J_PASSWORD environment variable')

    return Graph(password=password)


def assert_uniqueness_constraint(graph, label, property_name):
    """Create a uniqueness constraint if it doesn't already exist."""
    if property_name not in graph.schema.get_uniqueness_constraints(label):
        msg = 'Creating uniqueness constraint {}: {}'
        logger.info(msg.format(label, property_name))
        graph.schema.create_uniqueness_constraint(label, property_name)


def hash_wikitext(text):
    try:
        btext = text.encode('utf-8')
    except AttributeError:
        btext = ''.encode('utf-8')
        logger.info('Unable to encode wikitext to bytes')
    return hashlib.sha224(btext).hexdigest()


def get_wiki_page(title):
    site = pywikibot.Site('en', 'wikipedia')
    page = pywikibot.Page(site, title)
    return page
