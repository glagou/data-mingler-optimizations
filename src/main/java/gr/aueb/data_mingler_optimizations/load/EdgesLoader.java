package gr.aueb.data_mingler_optimizations.load;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.neo4j.driver.v1.*;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import gr.aueb.data_mingler_optimizations.enumerator.DatabaseType;
import gr.aueb.data_mingler_optimizations.enumerator.SourceType;
import gr.aueb.data_mingler_optimizations.exception.DatabaseVendorNotSupportedException;
import gr.aueb.data_mingler_optimizations.exception.EdgeNotFoundException;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.UnableToConnectToDatabaseException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

record Edge(String nodeA, String nodeB, String aliasA, String aliasB, SourceType sourceType, String datasourceRow,
        String queryString,
        String[] keyPosStr, String[] valuePosStr) {
};

public class EdgesLoader {

    private static final String PATH_TO_DICT = "C:\\DataMingler\\Implementation\\datasources.xml";

    private static XPath xpath;
    private static Document document;
    private static String datasourceRow;
    private static List<Record> records;
    private static org.neo4j.driver.v1.Driver driver;

    public static void main(String[] args) throws ParserConfigurationException,
            XPathExpressionException, org.xml.sax.SAXException {

        Instant start = Instant.now();

        validateCmdArguments(args);
        initializeDocumentAndXpath();
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"));

        int numOfEdges = (args.length / 4);
        IntStream.range(0, args.length)
                .filter(i -> i % 4 == 0)
                .mapToObj(i -> Arrays.copyOfRange(args, i, i + 4))
                .flatMap(stringGroup -> {
                    String nodeA = stringGroup[0];
                    String nodeB = stringGroup[1];
                    String aliasA = stringGroup[2];
                    String aliasB = stringGroup[3];
                    List<Edge> edges = getEdgesBetweenNodes(nodeA, nodeB, aliasA, aliasB);
                    return edges.stream();
                })
                .forEach(edge -> {
                    switch (edge.sourceType()) {
                        case DB:
                            loadFromDatabase(edge);
                            break;
                        case CSV:
                            loadFromCsv(edge);
                            break;
                        case EXCEL:
                            loadFromExcel(edge);
                            break;
                        case PROCESS:
                            loadFromProcess(edge);
                            break;
                    }
                });

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }

    private static void validateCmdArguments(String[] args) {
        if (args.length < 4 || args.length % 4 != 0) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    private static void initializeDocumentAndXpath() {
        try {

            DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = parser.parse(new File(PATH_TO_DICT));
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new UnableToInitializeDocumentAndXpathException(PATH_TO_DICT);
        }
        xpath = XPathFactory.newInstance().newXPath();
    }

    private static List<Edge> getEdgesBetweenNodes(String nodeA, String nodeB, String aliasA, String aliasB) {
        List<Edge> records = new ArrayList<>();
        try (Session session = driver.session()) {
            StatementResult result = session.run("MATCH (a:attribute{name:'" + nodeA
                    + "'})-[r:has]->(b:attribute{name:'" + nodeB
                    + "'}) RETURN r.datasource as datasource, r.query as query, r.key as key, r.value as value");
            while (result.hasNext()) {
                Record record = result.next();
                String datasource = record.get("datasource").asString();
                Map<String, String> resultMap = searchEdgeSourceType(datasource, nodeA, nodeB);
                SourceType sourceType = SourceType.valueOf(resultMap.get("sourceType"));
                int xmlRow = Integer.parseInt(resultMap.get("row"));
                String datasourceRow = "/datasources/datasource[position()=" + xmlRow + "]";
                String queryString = record.get("query").asString();
                String[] keyPosStr = record.get("key").asString().split(",", -1);
                String[] valuePosStr = record.get("value").asString().split(",", -1);
                Edge edgeRecord = new Edge(nodeA, nodeB, aliasA, aliasB, sourceType, datasourceRow,
                        queryString, keyPosStr,
                        valuePosStr);
                records.add(edgeRecord);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage()); // TODO: handle exception
        }
        return records;
    }

    private static Map<String, String> searchEdgeSourceType(String datasource, String nodeA, String nodeB) {
        int rows = 1;
        while (xpath.evaluate("/datasources/datasource[position()=" + rows + "]", document).trim() != "") {
            if (xpath.evaluate("/datasources/datasource[position()=" + rows + "]/name", document).trim()
                    .equals(datasource)) {
                String sourceType = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/@type", document)
                        .trim();
                return new HashMap<String, String>(Map.of("sourceType", sourceType, "row", String.valueOf(rows)));
            }
            rows++;
        }
        throw new EdgeNotFoundException(datasource, nodeA, nodeB);
    }

    private static void loadFromDatabase(Edge edge) {
        String dbSystemString = xpath.evaluate(edge.datasourceRow().concat("/system"), document).trim();
        String connString = xpath.evaluate(edge.datasourceRow().concat("/connection"), document).trim();
        String username = xpath.evaluate(edge.datasourceRow().concat("/username"), document).trim();
        String password = xpath.evaluate(edge.datasourceRow().concat("/password"), document).trim();
        String database = xpath.evaluate(edge.datasourceRow().concat("/database"), document).trim();
        try {
            Connection connection;

            DatabaseType dbSystem = DatabaseType.valueOf(dbSystemString.toUpperCase());
            switch (dbSystem) {
                case MSACCESS:
                    connection = DriverManager.getConnection("jdbc:ucanaccess://" + connString);
                    break;
                case ORACLE:
                    throw new DatabaseVendorNotSupportedException(dbSystem);
                    break;
                case MYSQL:
                    throw new DatabaseVendorNotSupportedException(dbSystem);
                    break;
                case SQLSERVER:
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                    String initialConnectionString = "jdbc:sqlserver://" + connString + ";databaseName = " + database;
                    if (username.isEmpty() && password.isEmpty())
                        connection = DriverManager.getConnection(initialConnectionString + ";integratedSecurity=true;");
                    else
                        connection = DriverManager.getConnection(
                                initialConnectionString + ";username=" + username + ";password=" + password);
                    break;
                case POSTGRES:
                    throw new DatabaseVendorNotSupportedException(dbSystem);
                    break;
                case DB2:
                    throw new DatabaseVendorNotSupportedException(dbSystem);
                    break;
            }

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(edge.queryString());

            while (resultSet.next()) {
                // TODO: Streams maybe?
                String key = "";
                for (int i = 0; i < edge.keyPosStr().length; i++) {
                    if (i != 0)
                        key += ":";
                    key += resultSet.getString(edge.keyPosStr()[i]);
                }
                String value = "";
                for (int i = 0; i < edge.valuePosStr().length; i++) {
                    if (i != 0)
                        value += ":";
                    value += resultSet.getString(edge.valuePosStr()[i]);
                }
                GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB() + ":" + key, value);
                GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB(), key);
            }

            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException | ClassNotFoundException e) {
            throw new UnableToConnectToDatabaseException();
        }

    }

    private static void loadFromCsv(Edge edge) {
        String fileName = xpath.evaluate(edge.datasourceRow().concat("/filename"), document).trim();
        String path = xpath.evaluate(edge.datasourceRow().concat("/path"), document).trim();
        boolean headings = xpath.evaluate(edge.datasourceRow().concat("/headings"), document).trim().equals("yes");
        String delimiter = xpath.evaluate(edge.datasourceRow().concat("/delimiter"), document).trim();

        FileInputStream is = new FileInputStream(path + fileName);
        Reader csvFile = new InputStreamReader(is);
        BufferedReader buf = new BufferedReader(csvFile);
        String inputLine;
        int rowCounter = 0;
        while ((inputLine = buf.readLine()) != null) {
            rowCounter++;
            if (rowCounter == 1 && headings)
                continue;
            String[] columns = inputLine.split(delimiter, -1);

            // TODO: Streams maybe?
            String key = "";
            for (int i = 0; i < edge.keyPosStr().length; i++) {
                if (i != 0)
                    key += ":";
                key += columns[Integer.parseInt(edge.keyPosStr()[i]) - 1]; // we assume csv columns are numbered 1,2,...
                                                                           // in the graph, that's why the minus 1
            }
            String value = "";
            for (int i = 0; i < edge.valuePosStr().length; i++) {
                if (i != 0)
                    value += ":";
                value += columns[Integer.parseInt(edge.valuePosStr()[i]) - 1]; // we assume csv columns are numbered
                                                                               // 1,2,... in the graph, that's why the
                                                                               // minus 1
            }

            GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB() + ":" + key, value);
            GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB(), key);
        }
    }

    public static void loadFromExcel(Edge edge) {
        String fileName = xpath.evaluate(edge.datasourceRow().concat("/filename"), document).trim();
        String path = xpath.evaluate(edge.datasourceRow().concat("/path"), document).trim();
        String sheetName = xpath.evaluate(edge.datasourceRow().concat("/sheet"), document).trim();
        boolean headings = xpath.evaluate(edge.datasourceRow().concat("/headings"), document).trim().equals("yes");

        Workbook xlWBook;
        Sheet xlSheet;
        Row xlRow;
        Cell xlCell;
        DataFormatter formatter = new DataFormatter();
        FileInputStream xlFile = new FileInputStream(path + fileName);
        xlWBook = new XSSFWorkbook(xlFile);
        xlSheet = xlWBook.getSheet(sheetName);
        int noOfRows = xlSheet.getPhysicalNumberOfRows();

        // TODO: Streams maybe?
        for (int r = 0; r < noOfRows; r++) {
            if (r == 0 && headings)
                continue;
            xlRow = xlSheet.getRow(r);

            String key = "";
            for (int i = 0; i < edge.keyPosStr().length; i++) {
                xlCell = xlRow.getCell(Integer.parseInt(edge.keyPosStr()[i]) - 1);
                if (i != 0)
                    key += ":";
                key += formatter.formatCellValue(xlCell); // we assume columns are numbered 1,2,... in the graph, that's
                                                          // why the minus 1
            }
            String value = "";
            for (int i = 0; i < edge.valuePosStr().length; i++) {
                xlCell = xlRow.getCell(Integer.parseInt(edge.valuePosStr()[i]) - 1);
                if (i != 0)
                    value += ":";
                value += formatter.formatCellValue(xlCell); // we assume columns are numbered 1,2,... in the graph,
                                                            // that's why the minus 1
            }

            GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB() + ":" + key, value);
            GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB(), key);
        }
    }

    public static void loadFromProcess(Edge edge) {

        String path = xpath.evaluate(edge.datasourceRow().concat("/path"), document).trim();
        String fileName = xpath.evaluate(edge.datasourceRow().concat("/filename"), document).trim();
        String engPath = xpath.evaluate(edge.datasourceRow().concat("/enginePath"), document).trim();
        String engine = xpath.evaluate(edge.datasourceRow().concat("/engine"), document).trim();
        String delimiter = xpath.evaluate(edge.datasourceRow().concat("/delimiter"), document).trim();

        try {
            Process p = Runtime.getRuntime().exec(engPath + engine + " " + path + fileName);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            // BufferedReader stdError = new BufferedReader(new
            // InputStreamReader(p.getErrorStream()));

            String inputLine = null;
            while ((inputLine = stdInput.readLine()) != null) {
                String[] columns = inputLine.split(delimiter, -1);

                String key = "";
                for (int i = 0; i < edge.keyPosStr().length; i++) {
                    if (i != 0)
                        key += ":";
                    key += columns[Integer.parseInt(edge.valuePosStr()[i]) - 1];
                }
                String value = "";
                for (int i = 0; i < edge.valuePosStr().length; i++) {
                    if (i != 0)
                        value += ":";
                    value += columns[Integer.parseInt(edge.valuePosStr()[i]) - 1];
                }

                GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB() + ":" + key, value);
                GraphUtils.addValueToCollection(edge.aliasA() + "-" + edge.aliasB(), key);
            }

            p.waitFor();

            int returnValue = p.exitValue();
            if (returnValue != 0) {
                throw new RuntimeException("loadEdge failed during the evaluation of edge: " + edge.nodeA() + "->"
                        + edge.nodeB() + " with return error code:" + returnValue); // TODO: better exception
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
