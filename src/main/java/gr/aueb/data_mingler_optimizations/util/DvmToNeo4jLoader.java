package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.enumerator.DvmLoadType;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import org.neo4j.driver.*;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.File;

/**
 * This class is created in order to load a DVM described as an XML file to a neo4j instance.
 * The {@link DvmToNeo4jLoader} takes two mandatory arguments as input:
 * <ul>
 *     <li>The absolute path to the DVM XML file (including the file name)</li>
 *     <li>The loading type of the DVM, which can be any value of {@link DvmLoadType}</li>
 * <ul>
 */
public class DvmToNeo4jLoader {

    private static final String BOLT_URI = "bolt://localhost:7687";
    private static final AuthToken NEO4J_AUTH_TOKEN = AuthTokens.basic("neo4j", "12345678");

    private static Driver neo4jDriver;

    private static void validateNumberOfArguments(String[] args) {
        if (args.length != 2) throw new InvalidNumberOfCmdArgumentsException();
    }

    private static void initializeDriver() {
        neo4jDriver = GraphDatabase.driver(BOLT_URI, NEO4J_AUTH_TOKEN);
    }

    private static void clearGraphIfNeeded(String[] args) {
        DvmLoadType addition = DvmLoadType.valueOf(args[1].toUpperCase());
        if (addition == DvmLoadType.CLEAR) {
            try (Session session = neo4jDriver.session()) {
                session.run("MATCH (n:attribute) DETACH DELETE n");
            }
        }
    }

    private static void loadNodeToNeo4j(String nodeName, String nodeDescription) {
        try (Session session = neo4jDriver.session()) {
            Result result = session.run("MATCH (n:attribute {name:'" + nodeName + "'}) RETURN n.name AS name");
            if (result.hasNext()) {
                System.out.println("Node:" + nodeName + " already exists");
            } else {
                session.run("CREATE (n:attribute {name: '" + nodeName + "', description:'" + nodeDescription + "'})");
                System.out.println("Node:" + nodeName + " was created");
            }
        }
    }

    private static void loadDvmToNeo4j(String[] args) throws Exception {
        DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = parser.parse(new File(args[0]));
        XPath xpath = XPathFactory.newInstance().newXPath();

        int rows = 1;
        while (!xpath.evaluate("/edges/edge[position()=" + rows + "]", doc).isEmpty()) {

            String nodeA_name = xpath.evaluate("/edges/edge[position()=" + rows + "]/headnode/name", doc).trim();
            String nodeA_description = xpath.evaluate("/edges/edge[position()=" + rows + "]/headnode/description", doc).trim();
            String nodeB_name = xpath.evaluate("/edges/edge[position()=" + rows + "]/tailnode/name", doc).trim();
            String nodeB_description = xpath.evaluate("/edges/edge[position()=" + rows + "]/tailnode/description", doc).trim();
            String datasourceName = xpath.evaluate("/edges/edge[position()=" + rows + "]/datasource", doc).trim();
            String queryString = xpath.evaluate("/edges/edge[position()=" + rows + "]/query", doc).trim();
            String pos1 = xpath.evaluate("/edges/edge[position()=" + rows + "]/key", doc).trim();
            String pos2 = xpath.evaluate("/edges/edge[position()=" + rows + "]/value", doc).trim();

            loadNodeToNeo4j(nodeA_name, nodeA_description);
            loadNodeToNeo4j(nodeB_name, nodeB_description);

            try (Session session = neo4jDriver.session()) {
                session.run("MATCH (a:attribute),(b:attribute) " +
                        "WHERE a.name = '" + nodeA_name + "' AND b.name = '" + nodeB_name + "' " +
                        "CREATE (a)-[:has{datasource:'" + datasourceName + "', query:'" + queryString + "', key:'" +
                        pos1 + "', value:'" + pos2 + "', selected:'false'}]->(b)");
            }

            try (Session session = neo4jDriver.session()) {
                session.run("MATCH (n:attribute {name:'" + nodeA_name + "'})-[:has]->(b) " +
                        "WITH n, count(b) AS cnt " +
                        "WHERE cnt > 1 " +
                        "SET n:primary ");
                session.run("MATCH (n:attribute {name:'" + nodeB_name + "'})-[:has]->(b) " +
                        "WITH n, count(b) AS cnt " +
                        "WHERE cnt > 1 " +
                        "SET n:primary ");
            }
            rows++;
        }
    }

    private static void closeDriver() {
        neo4jDriver.close();
    }


    public static void main(String[] args) throws Exception {
        validateNumberOfArguments(args);
        initializeDriver();
        clearGraphIfNeeded(args);
        loadDvmToNeo4j(args);
        closeDriver();
    }

}
