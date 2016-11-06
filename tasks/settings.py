from unipath import Path

# proj_root/py_pkg/settings.py
PROJ_ROOT = Path(__file__).ancestor(2).absolute()
DATA_DIR = Path(PROJ_ROOT, 'data')
if not DATA_DIR.isdir():
    DATA_DIR.mkdir()

SQLITE_PATH = Path(DATA_DIR, 'article_qualities.sqlite')
