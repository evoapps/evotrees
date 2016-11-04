import tasks as evotrees

def test_wikitext_node_hash():
    revision_data = {'text': 'hello, world!'}
    node = evotrees.Wikitext(revision_data).to_node()
    expected = evotrees.util.hash_wikitext(revision_data['text'])
    assert node['hash'] == expected

def test_hash_wikitext_accepts_bad_text():
    expected = 'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f'
    assert evotrees.util.hash_wikitext("") == expected
    assert evotrees.util.hash_wikitext(None) == expected
