package gr.aueb.data_mingler_optimizations.load;

import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class EdgesLoader {

    private static final String BOLT_URI = "bolt://localhost:7687";
    private static final AuthToken NEO4J_AUTH_TOKEN = AuthTokens.basic("neo4j", "12345678");
    private static final String PATH_TO_DATASOURCES_XML = "C:\\Users\\User\\Desktop\\Development" +
            "\\data-mingler-optimizations\\src\\main\\resources\\xml_files\\datasources.xml";
    private static final DbConnectionPool CONNECTION_POOL = new DbConnectionPool();

    private static Driver neo4jDriver;
    private static XPath xpath;
    private static Document document;

    private static void validateCmdArguments(String[] args) {
        if (args.length < 4 || args.length % 4 != 0) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    private static void initializeDriver() {
        neo4jDriver = GraphDatabase.driver(BOLT_URI, NEO4J_AUTH_TOKEN);
    }

    private static void initializeDocumentAndXpath() {
        try {
            DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = parser.parse(new File(PATH_TO_DATASOURCES_XML));
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new UnableToInitializeDocumentAndXpathException(PATH_TO_DATASOURCES_XML);
        }
        xpath = XPathFactory.newInstance().newXPath();
    }

    private static void loadEdges(String[] args) {
        IntStream.range(0, args.length)
                .filter(i -> i % 4 == 0)
                .forEach(i -> {
                    String nodeA = args[i];
                    String nodeB = args[i + 1];
                    String aliasA = args[i + 2].isEmpty() ? nodeA : args[i + 2];
                    String aliasB = args[i + 3].isEmpty() ? nodeB : args[i + 3];
                    boolean found = false;
                    Result result;
                    try (Session session = neo4jDriver.session()) {
                        try {
                            result = session.run("MATCH (a:attribute{name:'" + nodeA + "'})-[r:has]->(b:attribute{name:'"
                                    + nodeB + "'}) RETURN r.datasource as datasource, r.query as query, r.key as key, " +
                                    "r.value as value");
                            while (result.hasNext()) {
                                found = true;
                                Record record = result.next();
                                String datasource = record.get("datasource").asString();
                                String queryString = record.get("query").asString();
                                String[] keyPosStr = record.get("key").asString().split(",", -1);
                                String[] valuePosStr = record.get("value").asString().split(",", -1);
                                List<Integer> keyPositions = new ArrayList<>();
                                List<Integer> valuePositions = new ArrayList<>();
                                for (String s : keyPosStr) {
                                    keyPositions.add(Integer.parseInt(s));
                                }
                                for (String s : valuePosStr) {
                                    valuePositions.add(Integer.parseInt(s));
                                }
                                System.out.println("Key Positions: " + keyPositions);
                                System.out.println("Value Positions: " + valuePositions);

                                int rows = 1;
                                boolean exists = false;
                                while (!xpath.evaluate("/datasources/datasource[position()=" + rows + "]",
                                        document).trim().isEmpty()) {
                                    if (xpath.evaluate("/datasources/datasource[position()=" + rows + "]/name",
                                            document).trim().equals(datasource)) {
                                        exists = true;
                                        break;
                                    }
                                    rows++;
                                }
                                if (!exists) {
                                    System.out.println("Datasource: " + datasource + " was not found in the dictionary " +
                                            "for the edge between nodes " + nodeA + " and " + nodeB);
                                    System.exit(3);
                                }
                                String datasourceType = xpath.evaluate("/datasources/datasource[position()="
                                        + rows + "]/@type", document).trim();

                                if (datasourceType.equals("db")) {
                                    String dbSystem = xpath.evaluate("/datasources/datasource[position()=" + rows +
                                            "]/system", document).trim();
                                    String connString = xpath.evaluate("/datasources/datasource[position()=" + rows
                                            + "]/connection", document).trim();
                                    String username = xpath.evaluate("/datasources/datasource[position()=" + rows
                                            + "]/username", document).trim();
                                    String password = xpath.evaluate("/datasources/datasource[position()=" + rows
                                            + "]/password", document).trim();
                                    String database = xpath.evaluate("/datasources/datasource[position()=" + rows
                                            + "]/database", document).trim();

                                    try {
                                        Connection connection = null;
                                        switch (dbSystem) {
                                            case "msaccess" ->
                                                    connection = DriverManager.getConnection("jdbc:ucanaccess://" + connString);
                                            case "sqlserver" -> {
                                                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                                                if (username.equals("") && password.equals("")) // windows authentication
                                                    connection = DriverManager.getConnection("jdbc:sqlserver://" + connString + ";databaseName = " + database + ";integratedSecurity=true;");
                                                else // username, password authentication
                                                    connection = DriverManager.getConnection("jdbc:sqlserver://" + connString + ";databaseName = " + database + ";username=" + username + ";password=" + password);
                                            }
                                            default -> {
                                                System.out.println("This database vendor is not supported");
                                                System.exit(4);
                                            }
                                        }

                                        java.sql.Statement statement = connection.createStatement();
                                        ResultSet resultSet = statement.executeQuery(queryString);
                                        while (resultSet.next()) {
                                            String key = "";
                                            for (int j = 0; j < keyPositions.size(); j++) {
                                                if (j != 0) key += ":";
                                                key += resultSet.getString(keyPositions.get(j));
                                            }
                                            String value = "";
                                            for (int j = 0; j < valuePositions.size(); j++) {
                                                if (j != 0) value += ":";
                                                value += resultSet.getString(valuePositions.get(j));
                                            }
                                            GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key,
                                                    value);
                                            GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key);
                                        }
                                        resultSet.close();
                                        statement.close();
                                        connection.close();
                                    } catch (Exception sqlex) {
                                        sqlex.printStackTrace();
                                        System.exit(5);
                                    }


                                }

                                // csv type
                                if (datasourceType.equals("csv")) {
                                    String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
                                    String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
                                    boolean headings = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/headings", document).trim().equals("yes");
                                    String delimiter = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/delimiter", document).trim();

                                    FileInputStream is = new FileInputStream(path + fileName);
                                    Reader csvFile = new InputStreamReader(is);
                                    BufferedReader buf = new BufferedReader(csvFile);
                                    String inputLine;
                                    int rowCounter = 0;
                                    while ((inputLine = buf.readLine()) != null) {
                                        rowCounter++;
                                        if (rowCounter == 1 && headings) continue;
                                        String[] columns = inputLine.split(delimiter, -1);

                                        String key = "";
                                        for (int j = 0; j < keyPositions.size(); j++) {
                                            if (j != 0) key += ":";
                                            key += columns[keyPositions.get(j) - 1];
                                        }
                                        String value = "";
                                        for (int j = 0; j < valuePositions.size(); j++) {
                                            if (j != 0) value += ":";
                                            value += columns[valuePositions.get(j) - 1];
                                        }

                                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key,
                                                value);
                                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key);
                                    }
                                }

                                // excel type - xlsx
                                if (datasourceType.equals("excel")) {
                                    String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
                                    String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
                                    String sheetName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/sheet", document).trim();
                                    boolean headings = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/headings", document).trim().equals("yes");

                                    Workbook xlWBook;
                                    Sheet xlSheet;
                                    Row xlRow;
                                    Cell xlCell;
                                    DataFormatter formatter = new DataFormatter();
                                    FileInputStream xlFile = new FileInputStream(path + fileName);
                                    xlWBook = new XSSFWorkbook(xlFile);
                                    xlSheet = xlWBook.getSheet(sheetName);
                                    int noOfRows = xlSheet.getPhysicalNumberOfRows();

                                    for (int r = 0; r < noOfRows; r++) {
                                        if (r == 0 && headings) continue;
                                        xlRow = xlSheet.getRow(r);

                                        String key = "";
                                        for (int j = 0; j < keyPositions.size(); j++) {
                                            xlCell = xlRow.getCell(keyPositions.get(j) - 1);
                                            if (j != 0) key += ":";
                                            key += formatter.formatCellValue(xlCell);
                                        }
                                        String value = "";
                                        for (int j = 0; j < valuePositions.size(); j++) {
                                            xlCell = xlRow.getCell(valuePositions.get(j) - 1);
                                            if (j != 0) value += ":";
                                            value += formatter.formatCellValue(xlCell);
                                        }

                                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key,
                                                value);
                                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key);
                                    }
                                }
                                if (datasourceType.equals("process")) {
                                    String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
                                    String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
                                    String engPath = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/enginePath", document).trim();
                                    String engine = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/engine", document).trim();
                                    String delimiter = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/delimiter", document).trim();

                                    try {
                                        Process p = Runtime.getRuntime().exec(engPath + engine + " " + path + fileName);

                                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

                                        String inputLine;
                                        while ((inputLine = stdInput.readLine()) != null) {
                                            String[] columns = inputLine.split(delimiter, -1);

                                            String key = "";
                                            for (int j = 0; j < keyPositions.size(); j++) {
                                                if (j != 0) key += ":";
                                                key += columns[keyPositions.get(j) - 1];
                                            }
                                            String value = "";
                                            for (int j = 0; j < valuePositions.size(); j++) {
                                                if (j != 0) value += ":";
                                                value += columns[valuePositions.get(j) - 1];
                                            }

                                            GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key,
                                                    value);
                                            GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key);
                                        }

                                        p.waitFor();

                                        int returnValue = p.exitValue();
                                        if (returnValue != 0) {
                                            System.out.println("loadEdge failed during the evaluation of edge: "
                                                    + nodeA + "->" + nodeB + " with return error code:" + returnValue);
                                            System.exit(4);
                                        }
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }

                                }


                            }
                            if (!found) {
                                System.out.println("There is no edge between " + nodeA + " and " + nodeB);
                                System.exit(2);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (XPathExpressionException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void closeConnectionPool() {
        CONNECTION_POOL.closeConnections();
    }

    public static void main(String[] args) {
        validateCmdArguments(args);
        initializeDocumentAndXpath();
        initializeDriver();
        loadEdges(args);
        closeConnectionPool();
    }

}
