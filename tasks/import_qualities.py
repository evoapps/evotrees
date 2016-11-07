import logging

from invoke import task, run
from unipath import Path
from py2neo import Node, Subgraph
import pandas

from .util import connect_to_graph_db, connect_to_sqlite_db
from .settings import DATA_DIR, SQLITE_PATH


logger = logging.getLogger(__name__)

arg_docs = dict(
    force="Should files be redownloaded? Default is False.",
    download_only="Download qualities and return without doing anything else.",
    i_have_enough_space="Override the automatic check for available space.",
    keep="Keep intermediate files after downloading and importing.",
)


@task(help=arg_docs)
def import_qualities(ctx, force=False, download_only=False, verbose=False,
                     i_have_enough_space=False, keep=False):
    """Download machine predicted article qualities.

    When running for the first time, this will download an archived tsv
    made available by the Wikimedia Research team containing machine
    predicted article qualities for all articles in the English Wikipedia
    estimated monthly.

        $ inv import_qualities -d

    Warning! When uncompressed, the plaintext data file is 35 GB, which
    isn't much fun to work with. By default, after downloading and
    decompressing, the data is loaded into a sqlite database (26 GB).
    This form makes querying much easier, but that doesn't make it snappy.
    These are set-it-and-forget-it tasks.

    There is an automatic check for at least 100 GB of free space. If this
    check fails in any way, the program will raise a NotEnoughGBAvailable
    exception. To override this automatic check, pass the '-i' flag, short
    for '--i-have-enough-space'.

    After downloading, decompressing, and importing into a sqlite database,
    the intermediate files will be deleted, unless the '--keep' flag is
    specified.

    A typical workflow looks like this:

        $ inv import_articles birds.csv  # import birds article revisions
        $ inv import_qualities           # add quality preds to revisions

    """
    if verbose:
        logger.setLevel(logging.INFO)

    download_qualities(force=force, i_have_enough_space=i_have_enough_space,
                       keep=keep)

    if download_only:
        return

    unlabeled_revids = get_revids_in_graph()
    labels = select_qualities_by_revid(unlabeled_revids)
    update_revision_nodes_with_qualities(labels)


def download_qualities(force=False, i_have_enough_space=False, keep=False):
    url = 'https://ndownloader.figshare.com/files/6059502'
    bz2 = Path(DATA_DIR, 'article_qualities.tsv.bz2')
    tsv = Path(DATA_DIR, 'article_qualities.tsv')

    # Try to prevent people from accidentally downloading too big a file.
    if not i_have_enough_space:
        try:
            gb_available = int(run("df -g {} | awk '/\//{ print $4 }'").stdout)
            if gb_available < 100:
                raise NotEnoughGBAvailable
        except:
            raise NotEnoughGBAvailable
        else:
            logger.info('Rest easy, you have enough space.')
    else:
        logger.info('Skipping space check. Good luck soldier!')

    if SQLITE_PATH.exists() and not force:
        raise DBAlreadyExists

    logger.info('Downloading and decompressing.')
    run('wget {url} > {bz2} && bunzip2 {bz2}'.format(url=url, bz2=bz2))

    logger.info('Importing into sqlite.')
    conn = connect_to_sqlite_db()
    for chunk in pandas.read_table(tsv, chunksize=100000):
        chunk.to_sql('qualities', conn, if_exists='append', index=False)
    conn.close()

    if not keep:
        tsv.remove()


def get_revids_in_graph():
    graph = connect_to_graph_db()
    records = graph.data("MATCH (r:Revision) RETURN r.revid AS revid")
    revids = pandas.DataFrame(records)['revid'].tolist()
    return revids


def select_qualities_by_revid(revids):
    q = "SELECT * FROM qualities WHERE rev_id IN ({})"
    revid_str = ','.join(map(str, revids))
    conn = connect_to_sqlite_db()
    qualities = pandas.read_sql_query(q.format(revid_str), conn)
    conn.close()
    return qualities


def update_revision_nodes_with_qualities(qualities):
    nodes = [Node('Revision', revid=int(rev.rev_id), quality=rev.weighted_sum)
             for rev in qualities.itertuples()]
    subgraph = Subgraph(nodes)
    graph = connect_to_graph_db()
    graph.merge(subgraph)


class NotEnoughGBAvailable(Exception):
    pass


class DBAlreadyExists(Exception):
    pass
