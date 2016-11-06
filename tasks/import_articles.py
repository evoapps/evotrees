import logging

from invoke import task
from py2neo import Node, Relationship, ConstraintError
import unipath
import pandas

from .models import Revision, Wikitext
from .util import (connect_to_graph_db, assert_uniqueness_constraint,
                   get_wiki_page)

logger = logging.getLogger(__name__)

arg_docs = dict(
    names=(
        "Articles to import. Can be an article title or slug, a "
        "comma-separated list of article titles or slugs, the path "
        "to a csv of articles."
    ),
    title_col=(
        "If names is a csv file, title col is the name of the "
        "column to use for article titles. Defaults to 'title'."
    ),
    clear_all=(
        "Should existing data be purged before importing new articles? "
        "Default is False."
    ),
    verbose="Describe what's happening to figure out what went wrong."
)


@task(help=arg_docs)
def import_articles(ctx, names, title_col='title', clear_all=False,
                    verbose=False):
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

    # Nodes and Relationships

    Importing articles involves creating py2neo nodes for the article, each
    revision that has been made to the article, each unique version of the
    article wikitext (distinct from unique revisions due to reverts), and
    any unique Wikipedians authoring the edits to this article.

    1. (Article) -[CONTAINS]-> (Revision)
    2. (Revision) -[PARENT_OF]-> (Revision)
    3. (Revision) -[CHANGED_TO]-> (Wikitext)
    4. (Wikitext) -[EDITED]-> (Wikitext)
    5. (Wikipedian) -[AUTHORED]-> (Revision)
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
    assert_uniqueness_constraint(graph, 'Revision', 'revid')
    assert_uniqueness_constraint(graph, 'Wikitext', 'hash')

    for title in titles:
        logger.info('Starting to process article: {}'.format(title))

        # Convert any slugs to titles
        title = title.replace('_', ' ')

        # Create a node for this article.
        article = Node('Article', title=title)

        try:
            graph.create(article)
        except ConstraintError:
            logger.info('Article {} already exists'.format(title))
            # TODO: Allow updating existing articles with new revisions.
            # Right now existing articles are just being skipped:
            continue

        # Create a root wikitext node
        parent_wikitext = Wikitext(dict(text='')).to_node()
        graph.create(parent_wikitext)

        # Create nodes for each of the article's revisions.
        # Also create preliminary relationships between article and revisions.
        parent_node = None
        for revision, wikitext in import_revisions_as_nodes(title):
            # All revisions should be unique
            try:
                graph.create(revision)
            except ConstraintError:
                raise DuplicateRevisionError

            # Wikitexts might not be unique
            graph.merge(wikitext)

            graph.create(Relationship(article, 'CONTAINS', revision))
            graph.create(Relationship(revision, 'CHANGED_TO', wikitext))
            graph.create(Relationship(parent_wikitext, 'EDIT', wikitext))

            if parent_node:
                graph.create(Relationship(parent_node, 'PARENT_OF', revision))

            parent_node = revision
            parent_wikitext = wikitext

        logger.info('Finished processing article: {}'.format(title))


def import_revisions_as_nodes(title):
    """Yield Wikipedia article revisions as py2neo Nodes.

    This generator returns tuples of (Revision, Wikitext) Node objects.

    Each revision is split into two nodes: Revisions and Wikitexts. The reason
    is that revisions and wikitext content are not equally unique. For example,
    reverts are unique revisions (based on revid and Wikipedian author) but
    they do not introduce any novel forms of the article wikitext content.

    Right now revisions are downloaded from the live Wikipedia servers.
    TODO: Allow importing revisions from other data sources, e.g., a Wikipedia
          database dump.

    Args:
        title: The name of the article to import. Can be either title case
            with spaces or a slug, with underscores. pywikibot doesn't seem
            to care.
    """
    page = get_wiki_page(title)
    revisions = page.revisions(reverse=True, content=True)
    for revision in revisions:
        revision_node = Revision(revision).to_node()
        wikitext_node = Wikitext(revision).to_node()
        yield (revision_node, wikitext_node)


class DuplicateRevisionError(Exception):
    """An unexpected, duplicated revision was encountered."""
