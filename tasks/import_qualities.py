from invoke import task, run
from unipath import Path
from py2neo import Node, Subgraph
import pandas

from .util import connect_to_graph_db, connect_to_sqlite_db
from .settings import DATA_DIR, SQLITE_PATH

arg_docs = dict(
    force="Should files be redownloaded? Default is False.",
    download_only="Download qualities and return without doing anything else."
)

@task
def import_qualities(ctx, force=False, download_only=False, verbose=False):
    """Download machine predicted article qualities.


    """
    download_qualities(force=force)

    if download_only:
        return

    unlabeled_revids = get_revids_in_graph()
    labels = select_qualities_by_revid(unlabeled_revids)
    update_revision_nodes_with_qualities(labels)


def download_qualities(force=False):
    url = 'https://ndownloader.figshare.com/files/6059502'
    bz2 = Path(DATA_DIR, 'article_qualities.tsv.bz2')
    tsv = Path(DATA_DIR, 'article_qualities.tsv')

    if tsv.exists() and not force:
        print('Compressed file already exists, not downloading.')
    else:
        print('Downloading and decompressing ~ 15 minutes.')
        run('wget {url} > {bz2} && bunzip2 {bz2}'.format(url=url, bz2=bz2))

    if SQLITE_PATH.exists() and not force:
        print('Sqlite database already exists, not creating.')
    else:
        print('Converting to sqlite')
        conn = connect_to_sqlite_db()
        for chunk in pandas.read_table(tsv, chunksize=100000):
            chunk.to_sql('qualities', conn, if_exists='append', index=False)
        conn.close()


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
