from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom

from .util import hash_wikitext, parse_wikitext


class Article(GraphObject):
    __primarykey__ = 'title'

    title = Property()

    revisions = RelatedTo("Revision", "CONTAINS")

    def __init__(self, title):
        # Convert any slugs to titles
        self.title = title.replace('_', ' ')


class Revision(GraphObject):
    __primarykey__ = 'revid'

    revid = Property()
    quality = Property()

    articles = RelatedFrom("Article", "CHANGED")
    texts = RelatedTo("Wikitext", "CHANGED_TO")
    children = RelatedTo("Revision", "PARENT_OF")

    def __init__(self, revision_data):
        self.revid = int(revision_data.get('revid'))
        self.quality = revision_data.get('quality')


class Wikitext(GraphObject):
    __primarykey__ = 'hash'

    wikitext = Property()
    hash = Property()
    plaintext = Property()

    revisions = RelatedFrom("Revision", "CHANGED_TO")
    edits = RelatedTo("Wikitext", "EDIT")

    def __init__(self, revision_data):
        self.wikitext = revision_data.get('text')
        self.hash = hash_wikitext(self.wikitext)
        self.plaintext = parse_wikitext(self.wikitext)
