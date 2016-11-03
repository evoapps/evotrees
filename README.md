# evotrees

# Usage

```bash
neo4j start
inv import_articles "Splendid fairywren"
# Go to http://localhost:7474/browser/ to view data.
```

# Setup

## Neo4j

Install Neo4j, start it, and change the default password.

```bash
brew install neo4j
neo4j start
# Go to http://localhost:7474/browser/
# Change default Neo4j password.
# Export password as NEO4J_PASSWORD environment variable.
neo4j stop
```

## python

Install the requirements in a python3 virtualenv.

```bash
mkvirtualenv --python=python3 trees -r requirements.txt
```
