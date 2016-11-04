from py2neo import Node

from .util import hash_wikitext, parse_wikitext


class RevisionNode:
    def __init__(self, revision_data):
        self.data = {k: revision_data[k] for k in self.REVISION_PROPERTIES}

    def to_node(self):
        return Node(self.NODE_LABEL, **self.data)


class Revision(RevisionNode):
    NODE_LABEL = 'Revision'
    REVISION_PROPERTIES = ['revid']


class Wikitext(RevisionNode):
    NODE_LABEL = 'Wikitext'
    REVISION_PROPERTIES = ['text']

    def to_node(self):
        # Rename text -> wikitext
        self.data['wikitext'] = self.data.pop('text')

        # Derive additional node attributes
        self.data['hash'] = hash_wikitext(self.data['wikitext'])
        self.data['plaintext'] = parse_wikitext(self.data['wikitext'])

        return super().to_node()
