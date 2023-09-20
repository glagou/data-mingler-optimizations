# DataMingler Optimizations

## Prerequisites

- JDK 17 (or higher)
- Maven 3.9 (or higher)
- Python 3.11 (or higher)
- Docker

### Install Python Dependencies

Assuming you're using pip:

```bash
pip install jep
```

### Install Java Dependencies

```bash
mvn install
```

### Setup Environment Variables

Create a `.env` file in the root directory of the project and add the following:

```
JEP_PATH=<path to jep.so OR jep.dll>
```

if you're running into issues with setting up JEP, please refer to the [JEP documentation](https://github.com/ninia/jep/wiki/Getting-Started)

### Running the Neo4j Container

```bash
docker run --name testneo4j -p7474:7474 -p7687:7687 -d -v $HOME/neo4j/data:/data -v $HOME/neo4j/logs:/logs -v $HOME/neo4j/import:/var/lib/neo4j/import -v $HOME/neo4j/plugins:/plugins --env NEO4J_AUTH=neo4j/12345678 neo4j:latest
```

## Running the Application

Having done all of the above, you can now run the application.

### Loading the Data

```bash
mvn compile exec:java -Dexec.mainClass="gr.aueb.data_mingler_optimizations.util.DvmToNeo4jLoader" -Dexec.args="path/to/default.dvm.xml CLEAR"
```

### Executing a Query

```bash
mvn compile exec:java -Dexec.mainClass="gr.aueb.data_mingler_optimizations.QueryEvaluation" -Dexec.args="path/to/query NONE ALL"
```