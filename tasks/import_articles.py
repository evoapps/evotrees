import logging

from invoke import task
from py2neo import Relationship, ConstraintError
import unipath
import pandas

from .models import Article, Revision, Wikitext
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
    4. (Wikitext) -[EDIT]-> (Wikitext)
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
        article = Article(title)

        try:
            graph.create(article)
        except ConstraintError:
            logger.info('Article {} already exists'.format(title))
            # TODO: Allow updating existing articles with new revisions.
            # Right now existing articles are just being skipped:
            continue

        # Create nodes for each of the article's revisions.
        # Also create preliminary relationships between article and revisions.
        first_revision = True
        parent_revision = None
        parent_wikitext = None

        rev_count = 1
        for revision, wikitext in import_revisions_as_nodes(title):
            logging.info('Processing revision #{}: {}'.format(rev_count,
                                                              revision.revid))
            transaction = graph.begin()


            # All revisions should be unique
            try:
                transation.create(revision)
            except ConstraintError:
                raise DuplicateRevisionError

            # Wikitexts might not be unique
            transaction.merge(wikitext)

            # Create article -> revision and revision -> wikitext relationships.
            article.revisions.add(revision)
            revision.texts.add(wikitext)

            transaction.merge(article)
            transaction.merge(revision)

            if first_revision:
                first_revision = False
            else:
                parent_revision.children.add(revision)
                parent_wikitext.edits.add(wikitext)
                transaction.merge(parent_revision)
                transaction.merge(parent_wikitext)

            transaction.commit()
            rev_count += 1
            parent_revision = revision
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
    for revision_data in revisions:
        revision = Revision(revision_data.__dict__)
        wikitext = Wikitext(revision_data.__dict__)
        yield (revision, wikitext)


class DuplicateRevisionError(Exception):
    """An unexpected, duplicated revision was encountered."""
