from os import environ

from invoke import task
from py2neo import Graph, Node, Relationship
import pywikibot
import unipath

@task(help=dict(names=("Articles to import. Can be an article title or slug, "
                       "a comma-separated list of article titles or slugs, "
                       "the path to a csv of articles.")))
def import_articles(ctx, names, title_col='title'):
    """Import Wikipedia articles into a Neo4j graph database.

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
    if unipath.Path(names).exists():
        titles = pandas.read_csv(names)[title_col].tolist()
    else:
        titles = names.split(',')

    graph = Graph(password=environ.get('NEO4J_PASSWORD'))

    for title in titles:
        title = title.replace('_', ' ')  # convert any slugs to titles

        # Create a node for this article.
        article = Node('Article', title=title)
        graph.create(article)

        # Create nodes for each of the article's revisions.
        # Also create preliminary relationships between article and revisions.
        parent = None
        for revision in import_revisions(title):
            graph.create(revision)
            graph.create(Relationship(article, 'CONTAINS', revision))

            if parent:
                graph.create(Relationship(parent, 'PARENT_OF', revision))

            parent = revision


def import_revisions(title):
    """Yield Wikipedia article revisions as py2neo Nodes."""
    properties = ['revid', 'text']

    site = pywikibot.Site('en', 'wikipedia')
    page = pywikibot.Page(site, title)

    revisions = page.revisions(reverse=True, content=True)
    for revision in revisions:
        data = {k: revision[k] for k in properties}
        yield Node('Revision', **data)
