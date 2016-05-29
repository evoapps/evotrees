import pytest
import evotrees

@pytest.fixture
def simple(request):
    session = evotrees.connect()
    session.run("CREATE (a:Person {name: 'Authur', title: 'King'})")

    def close_session():
        session.close()

    request.add_finalizer(close_session)
    return session


def test_hello_world(simple):
    result = simple.run("MATCH (a:Person {name: 'Authur', title: 'King'})")
    assert len(result) == 1
