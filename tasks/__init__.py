from os import environ
import logging

from invoke import task
from py2neo import Graph, Node, Relationship, ConstraintError
import pywikibot
import unipath

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

@task
def import_articles(ctx, names, title_col='title', clear_all=False,
                    verbose=False):
    """Import Wikipedia articles into a Neo4j graph database.

    Args:
        names:
            Articles to import. Can be an article title or slug, a
            comma-separated list of article titles or slugs, the path to
            a csv of articles.
        title_col:
            If names is a csv file, title col is the name of the
            column to use for article titles. Defaults to 'title'.

    Flags:
        clear_all:
            Should existing data be purged before importing new articles?
            Default is False.
        verbose:
            Describe what's happening to figure out what went wrong.

    This function is intended to be invoked from the command line. The
    smallest import is a single article. The full article name (with
    spaces) can be provided, or the article slug (with underscores).
    Multiple articles can be imported if they are separated with commas.

        $ inv import_articles "Splendid fairywren"
        $ inv import_articles Splendid_fairywren
        $ inv import_articles "Splendid fairywren,Harpsichord"

    Articles can also be imported from a csv. The expected name for the
    column in the data file containing article titles is "title", but
    a different column name can be specified with the --title-col
    option.

        $ inv import_articles birds.csv
        $ inv import_articles birds.csv --title-col=slug

    Articles are imported as Nodes, and each revision is imported as it's own
    Node as well. Two preliminary relationships are created on import. The
    first is the relationship between an article and it's revisions. The second
    is the chronological relationship between revisions.

        (article) -[:CONTAINS]-> (revision)
        (revision) -[:PARENT_OF]-> (revision)
    """
    if verbose:
        logger.setLevel(logging.INFO)
        logging.info('Logging on')

    if unipath.Path(names).exists():
        titles = pandas.read_csv(names)[title_col].tolist()
    else:
        titles = names.split(',')

    graph = connect_to_graph_db()
    if clear_all:
        graph.delete_all()
    assert_uniqueness_constraint(graph, 'Article', 'title')

    for title in titles:
        logger.info('Start processing article: {}'.format(title))
        title = title.replace('_', ' ')  # convert any slugs to titles

        # Create a node for this article.
        article = Node('Article', title=title)

        try:
            graph.create(article)
        except ConstraintError:
            logger.info('Article {} already exists'.format(title))
            continue

        # Create nodes for each of the article's revisions.
        # Also create preliminary relationships between article and revisions.
        parent = None
        for revision in import_revisions(title):
            graph.create(revision)
            graph.create(Relationship(article, 'CONTAINS', revision))

            if parent:
                graph.create(Relationship(parent, 'PARENT_OF', revision))

            parent = revision


@task
def open_browser(ctx):
    """Open the graph database in the browser."""
    graph = connect_to_graph_db()
    graph.open_browser()


def import_revisions(title):
    """Yield Wikipedia article revisions as py2neo Nodes."""
    properties = ['revid', 'text']

    site = pywikibot.Site('en', 'wikipedia')
    page = pywikibot.Page(site, title)

    revisions = page.revisions(reverse=True, content=True)
    for revision in revisions:
        data = {k: revision[k] for k in properties}
        yield Node('Revision', **data)


def connect_to_graph_db():
    password = environ.get('NEO4J_PASSWORD')
    if not password:
        raise AssertionError('must set NEO4J_PASSWORD environment variable')

    return Graph(password=password)


def assert_uniqueness_constraint(graph, label, property_name):
    """Create a uniqueness constraint if it doesn't already exist."""
    if not property_name in graph.schema.get_uniqueness_constraints(label):
        msg = 'Creating uniqueness constraint {}: {}'
        logger.info(msg.format(label, property_name))
        graph.schema.create_uniqueness_constraint(label, property_name)
