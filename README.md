# evotrees

Load Wikipedia article revision histories into a graph database to study changes to Wikipedia article quality over time.

To use this package, you need to have Neo4j installed, and a python virtual environment with the required packages installed. With these requirements, you can import the revision history of any article into a Neo4j graph database.

```bash
neo4j start
inv import_articles "Splendid fairywren"
inv open_browser  # View article revision history in Neo4j
```

You can also import machine predicted article qualities based on revid from the Wikimedia Foundation's Objective Revision Evaluation Service (ORES). The following command downloads monthly quality scores for the English Wikipedia and merges the qualities with the revisions in the graph database.

```bash
inv import_qualities
```

# Setup

## Neo4j

Install Neo4j, start it, and change the default password.

```bash
brew install neo4j    # Install Neo4j with Homebrew
neo4j start           # Start the Neo4j server
# Go to http://localhost:7474/browser/ and change default Neo4j password.
# Export password as NEO4J_PASSWORD environment variable.
echo $NEO4J_PASSWORD  # Should be your new password.
```

## python

Install the requirements in a python3 virtualenv.

```bash
mkvirtualenv --python=python3 trees -r requirements.txt
```
