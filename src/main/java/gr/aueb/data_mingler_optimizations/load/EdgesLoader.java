package gr.aueb.data_mingler_optimizations.load;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.DatabaseSystem;
import gr.aueb.data_mingler_optimizations.enumerator.DatasourceType;
import gr.aueb.data_mingler_optimizations.exception.DatasourceNotFoundInDictionaryException;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.dhatim.fastexcel.reader.Cell;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.neo4j.driver.Values.parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import org.dhatim.fastexcel.reader.ReadableWorkbook;
import org.dhatim.fastexcel.reader.Row;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EdgesLoader {

    private static final String BOLT_URI = "bolt://localhost:7687";
    private static final AuthToken NEO4J_AUTH_TOKEN = AuthTokens.basic("neo4j", "12345678");

    private static Driver neo4jDriver;
    private static XPath xpath;
    private static Document document;

    static Connection connection = null;

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
            String dataSourcesFilePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                    .getResource("xml_files/datasources.xml")).getPath();
            DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = parser.parse(new File(dataSourcesFilePath));
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new UnableToInitializeDocumentAndXpathException("datasources.xml");
        }
        xpath = XPathFactory.newInstance().newXPath();
    }

    private static int validateDatasourceExists(String datasource, String nodeA, String nodeB)
            throws XPathExpressionException {

        int rows = 1;
        boolean datasourceExists = false;
        while (!xpath.evaluate("/datasources/datasource[position()=" + rows + "]",
                document).trim().isEmpty()) {
            if (xpath.evaluate("/datasources/datasource[position()=" + rows + "]/name",
                    document).trim().equals(datasource)) {
                datasourceExists = true;
                break;
            }
            rows++;
        }
        if (!datasourceExists) {
            throw new DatasourceNotFoundInDictionaryException(datasource, nodeA, nodeB);
        }
        return rows;
    }

    private static DatasourceType findDatasourceType(int row) throws XPathExpressionException {
        String datasourceType = xpath.evaluate("/datasources/datasource[position()=" + row + "]/@type",
                document).trim();
        return DatasourceType.valueOf(datasourceType.toUpperCase());
    }

    private static DatabaseSystem findDatabaseSystem(int row) throws XPathExpressionException {
        String databaseSystem = xpath.evaluate("/datasources/datasource[position()=" + row + "]/system",
                document).trim();
        return DatabaseSystem.valueOf(databaseSystem.toUpperCase());
    }

    private static void loadEdgesForDatabase(int rows, List<Integer> keyPositions, List<Integer> valuePositions,
                                             String queryString, String aliasA, String aliasB, Connection connection) throws XPathExpressionException {

        queryString = queryString.replace("\"", "'");
        try (
                PreparedStatement statement = connection.prepareStatement(queryString);
                ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                StringBuilder key = new StringBuilder();
                for (int j : keyPositions) {
                    if (!key.isEmpty()) key.append(":");
                    key.append(resultSet.getString(j));
                }

                StringBuilder value = new StringBuilder();
                for (int j : valuePositions) {
                    if (!value.isEmpty()) value.append(":");
                    value.append(resultSet.getString(j));
                }

                GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                        GraphAdditionMethod.AS_LIST);

                GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getSingleNodeValue(Document document, String xpathExpression) throws XPathExpressionException {
        return xpath.evaluate(xpathExpression, document).trim();
    }

    private static void databaseoptimizer(Connection connection, List<String> aliasa, List<String> aliasb) {
        String sqlQuery = "SELECT tt.int_id, td.base_passenger_fare, ts.trip_miles, tt.pickup_datetime " +
                "FROM trip_time tt " +
                "INNER JOIN trip_speed ts ON tt.int_id = ts.int_id " +
                "INNER JOIN trip_details td ON tt.int_id = td.int_id;";

        try (
                PreparedStatement statement = connection.prepareStatement(sqlQuery);
                ResultSet resultSet = statement.executeQuery()) {

            List<Integer> values = new ArrayList<>();
            values.add(2);
            values.add(3);
            values.add(4);

            while (resultSet.next()) {

                for (Integer num : values) {


                    StringBuilder key = new StringBuilder();

                    if (!key.isEmpty()) key.append(":");
                    key.append(resultSet.getString(1));

                    StringBuilder value = new StringBuilder();

                    if (!value.isEmpty()) value.append(":");
                    value.append(resultSet.getString(num));

                    GraphUtils.addValueToCollection(aliasa.get(num - 2) + "-" + aliasb.get(num - 2) + ":" + key, value.toString(),
                            GraphAdditionMethod.AS_LIST);

                    GraphUtils.addValueToCollection(aliasa.get(num - 2) + "-" + aliasb.get(num - 2), key.toString(), GraphAdditionMethod.AS_SET);
                }


            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private static void loadEdgesForCsv(int rows, List<Integer> keyPositions, List<Integer> valuePositions,
                                        String aliasA, String aliasB) throws XPathExpressionException {

        String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename",
                document).trim();
        String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path",
                document).trim();
        boolean headings = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/headings",
                document).trim().equals("yes");
        String delimiter = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/delimiter",
                document).trim();

        try {
            FileInputStream is = new FileInputStream(path + fileName);
            Reader csvFile = new InputStreamReader(is);
            BufferedReader buf = new BufferedReader(csvFile);
            String inputLine;
            int rowCounter = 0;
            while ((inputLine = buf.readLine()) != null) {
                rowCounter++;
                if (rowCounter == 1 && headings) continue;
                String[] columns = inputLine.split(delimiter, -1);

                StringBuilder key = new StringBuilder();
                for (int j = 0; j < keyPositions.size(); j++) {
                    if (j != 0) key.append(":");
                    key.append(columns[keyPositions.get(j) - 1]);
                }
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < valuePositions.size(); j++) {
                    if (j != 0) value.append(":");
                    value.append(columns[valuePositions.get(j) - 1]);
                }
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                        GraphAdditionMethod.AS_LIST);
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void loadEdgesForXML(int rows, List<Integer> keyPositions, List<Integer> valuePositions, String nodeA, String nodeB,
                                       String aliasA, String aliasB) {
        try {
            String basePath = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
            String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
            String fullPath = basePath + fileName;

            File xmlFile = new File(fullPath);

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            XMLStreamReader reader = inputFactory.createXMLStreamReader(new FileInputStream(xmlFile));

            StringBuilder keyBuilder = new StringBuilder();
            StringBuilder valueBuilder = new StringBuilder();
            String index = null;
            String reviewText = null;

            String compositeKeyPrefix = aliasA + "-" + aliasB + ":";
            String aliasKey = aliasA + "-" + aliasB;

            while (reader.hasNext()) {
                int event = reader.next();

                switch (event) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String elementName = reader.getLocalName();
                        if (!elementName.equals(nodeA) & !elementName.equals(nodeB)) {
                            keyBuilder.setLength(0);
                            valueBuilder.setLength(0);
                        } else if (elementName.equals(nodeA)) {
                            index = reader.getElementText();
                        } else {
                            reviewText = reader.getElementText();
                        }
                    }
                    case XMLStreamConstants.END_ELEMENT -> {
                        //if (reader.getLocalName().equals("review")) {
                        // Process the data here
                        for (int j : keyPositions) {
                            if (!keyBuilder.isEmpty()) {
                                keyBuilder.append(":");
                            }
                            keyBuilder.append(index);
                        }

                        for (int j : valuePositions) {
                            if (!valueBuilder.isEmpty()) {
                                valueBuilder.append(":");
                            }
                            valueBuilder.append(reviewText);
                        }

                        String key = keyBuilder.toString();
                        String value = valueBuilder.toString();

                        String compositeKey = compositeKeyPrefix + key;
                        GraphUtils.addValueToCollection(compositeKey, value, GraphAdditionMethod.AS_LIST);
                        GraphUtils.addValueToCollection(aliasKey, key, GraphAdditionMethod.AS_SET);
                    }
                }
            }
            //}

            reader.close();
        } catch (XPathExpressionException | IOException | XMLStreamException e) {
            throw new RuntimeException("Error processing XML file", e);
        }
    }


    private static void loadEdgesForExcel(int rows, List<Integer> keyPositions, List<Integer> valuePositions,
                                          String aliasA, String aliasB) throws XPathExpressionException {

        String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
        String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
        String sheetName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/sheet", document).trim();
        boolean headings = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/headings", document).trim().equals("yes");

        try (InputStream is = new FileInputStream(path + fileName);
             ReadableWorkbook wb = new ReadableWorkbook(is)) {


            wb.getSheets().forEach(sheet -> {
                try (Stream<Row> rows_excel = sheet.openStream()) {

                    int startRow = headings ? 1 : 0;
                    rows_excel.skip(startRow).forEach(r -> {
                        StringBuilder key = new StringBuilder();
                        for (int j : keyPositions) {
                            String str;
                            Cell cell = r.getCell(j - 1);
                            if (cell != null) {
                                str = cell.getValue().toString();
                            } else {
                                str = null;
                            }
                            if (!key.isEmpty()) key.append(":");
                            key.append(str);
                        }

                        StringBuilder value = new StringBuilder();
                        for (int j : valuePositions) {
                            String str;
                            Cell cell = r.getCell(j - 1);
                            if (cell != null) {
                                str = cell.getValue().toString();
                            } else {
                                str = null;
                            }
                            if (!value.isEmpty()) value.append(":");
                            value.append(str);
                        }

                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                                GraphAdditionMethod.AS_LIST);
                        GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
                    });

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static void loadEdgesForProcess(int rows, List<Integer> keyPositions, List<Integer> valuePositions,
                                            String aliasA, String aliasB) throws XPathExpressionException {

        String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path",
                document).trim();
        String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename",
                document).trim();
        String engPath = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/enginePath",
                document).trim();
        String engine = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/engine",
                document).trim();
        String delimiter = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/delimiter",
                document).trim();

        try {
            Process p = Runtime.getRuntime().exec(engPath + engine + " " + path + fileName);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String inputLine;
            while ((inputLine = stdInput.readLine()) != null) {
                String[] columns = inputLine.split(delimiter, -1);

                StringBuilder key = new StringBuilder();
                for (int j = 0; j < keyPositions.size(); j++) {
                    if (j != 0) key.append(":");
                    key.append(columns[keyPositions.get(j) - 1]);
                }
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < valuePositions.size(); j++) {
                    if (j != 0) value.append(":");
                    value.append(columns[valuePositions.get(j) - 1]);
                }

                GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                        GraphAdditionMethod.AS_LIST);
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
            }

            p.waitFor();

            int returnValue = p.exitValue();
            if (returnValue != 0) {
                System.out.println("loadEdge failed during the evaluation of edge: " + aliasA + "->" + aliasB
                        + " with return error code:" + returnValue);
                System.exit(4);
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void loadEdges(String[] args) {
        AtomicInteger k = new AtomicInteger();
        List<String> aliasa = new ArrayList<>();
        List<String> aliasb = new ArrayList<>();

        try (Session session = neo4jDriver.session()) {
            String query = "MATCH (a:attribute{name:$nodeA})-[r:has]->(b:attribute{name:$nodeB}) " +
                    "RETURN r.datasource as datasource, r.query as query, r.key as key, r.value as value";

            IntStream.iterate(0, i -> i < args.length, i -> i + 4)
                    .forEach(i -> {
                        String nodeA = args[i];
                        String nodeB = args[i + 1];
                        String aliasA = args[i + 2].isEmpty() ? nodeA : args[i + 2];
                        String aliasB = args[i + 3].isEmpty() ? nodeB : args[i + 3];

                        try {
                            Result result = session.run(query, parameters("nodeA", nodeA, "nodeB", nodeB));

                            while (result.hasNext()) {
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

                                int rows = validateDatasourceExists(datasource, nodeA, nodeB);
                                DatasourceType datasourceType = findDatasourceType(rows);

                                if (datasourceType == DatasourceType.DB) {

                                    if (connection == null) {
                                        try {
                                            Class.forName("org.postgresql.Driver");
                                        } catch (ClassNotFoundException e) {
                                            System.out.println(e.getMessage());
                                        }
                                        String connString = getSingleNodeValue(document, "/datasources/datasource[position()=" + rows + "]/connection").trim();
                                        String username = getSingleNodeValue(document, "/datasources/datasource[position()=" + rows + "]/username").trim();
                                        String password = getSingleNodeValue(document, "/datasources/datasource[position()=" + rows + "]/password").trim();
                                        String database = getSingleNodeValue(document, "/datasources/datasource[position()=" + rows + "]/database").trim();
                                        String url = "jdbc:postgresql://" + connString + "/" + database;
                                        connection = DriverManager.getConnection(url, username, password);
                                    }
                                    loadEdgesForDatabase(rows, keyPositions, valuePositions, queryString, aliasA, aliasB, connection);
                                    //loadEdgesForDatabase(rows, keyPositions, valuePositions, queryString, aliasA, aliasB, connection);

                                } else if (datasourceType == DatasourceType.CSV) {
                                    loadEdgesForCsv(rows, keyPositions, valuePositions, aliasA, aliasB);
                                } else if (datasourceType == DatasourceType.EXCEL) {
                                    loadEdgesForExcel(rows, keyPositions, valuePositions, aliasA, aliasB);
                                } else if (datasourceType == DatasourceType.XML) {
                                    loadEdgesForXML(rows, keyPositions, valuePositions, nodeA, nodeB, aliasA, aliasB);
                                } else {
                                    loadEdgesForProcess(rows, keyPositions, valuePositions, aliasA, aliasB);
                                }

                                keyPositions.clear();
                                valuePositions.clear();
                            }
                        } catch (XPathExpressionException | SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) {
        validateCmdArguments(args);
        initializeDocumentAndXpath();
        initializeDriver();
        loadEdges(args);
        neo4jDriver.close();
    }

}
