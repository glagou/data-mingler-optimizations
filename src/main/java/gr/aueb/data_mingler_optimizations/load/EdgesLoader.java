package gr.aueb.data_mingler_optimizations.load;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.DatabaseSystem;
import gr.aueb.data_mingler_optimizations.enumerator.DatasourceType;
import gr.aueb.data_mingler_optimizations.exception.DatasourceNotFoundInDictionaryException;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.NoEdgeExistsException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.w3c.dom.Document;
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
import java.util.stream.IntStream;

import static org.neo4j.driver.Values.parameters;

public class EdgesLoader {

    private static final String BOLT_URI = "bolt://localhost:7687";
    private static final AuthToken NEO4J_AUTH_TOKEN = AuthTokens.basic("neo4j", "12345678");

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
                                             String queryString, String aliasA, String aliasB)
            throws XPathExpressionException {

        DatabaseSystem dbSystem = findDatabaseSystem(rows);
        String connString = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/connection",
                document).trim();
        String username = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/username",
                document).trim();
        String password = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/password",
                document).trim();
        String database = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/database",
                document).trim();


        try {
            Connection connection;
            if (dbSystem == DatabaseSystem.MSACCESS) {
                connection = DriverManager.getConnection("jdbc:ucanaccess://" + connString);
            } else {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                if (username.isEmpty() && password.isEmpty())
                    connection = DriverManager.getConnection("jdbc:sqlserver://" + connString +
                            ";databaseName = " + database + ";integratedSecurity=true;");
                else
                    connection = DriverManager.getConnection("jdbc:sqlserver://" + connString +
                            ";databaseName = " + database + ";username=" + username + ";password=" + password);
            }

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryString);
            while (resultSet.next()) {
                StringBuilder key = new StringBuilder();
                for (int j = 0; j < keyPositions.size(); j++) {
                    if (j != 0) key.append(":");
                    key.append(resultSet.getString(keyPositions.get(j)));
                }
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < valuePositions.size(); j++) {
                    if (j != 0) value.append(":");
                    value.append(resultSet.getString(valuePositions.get(j)));
                }
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                        GraphAdditionMethod.AS_LIST);
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException | ClassNotFoundException e) {
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

    private static void loadEdgesForExcel(int rows, List<Integer> keyPositions, List<Integer> valuePositions,
                                          String aliasA, String aliasB) throws XPathExpressionException {

        String fileName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/filename", document).trim();
        String path = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/path", document).trim();
        String sheetName = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/sheet", document).trim();
        boolean headings = xpath.evaluate("/datasources/datasource[position()=" + rows + "]/headings", document).trim().equals("yes");



        try {
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

                StringBuilder key = new StringBuilder();
                for (int j = 0; j < keyPositions.size(); j++) {
                    xlCell = xlRow.getCell(keyPositions.get(j) - 1);
                    if (j != 0) key.append(":");
                    key.append(formatter.formatCellValue(xlCell));
                }
                StringBuilder value = new StringBuilder();
                for (int j = 0; j < valuePositions.size(); j++) {
                    xlCell = xlRow.getCell(valuePositions.get(j) - 1);
                    if (j != 0) value.append(":");
                    value.append(formatter.formatCellValue(xlCell));
                }

                GraphUtils.addValueToCollection(aliasA + "-" + aliasB + ":" + key, value.toString(),
                        GraphAdditionMethod.AS_LIST);
                GraphUtils.addValueToCollection(aliasA + "-" + aliasB, key.toString(), GraphAdditionMethod.AS_SET);
            }
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
        IntStream.iterate(0, i -> i < args.length, i -> i + 4)
                .parallel()
                .forEach(i -> {
                    String nodeA = args[i];
                    String nodeB = args[i + 1];
                    String aliasA = args[i + 2].isEmpty() ? nodeA : args[i + 2];
                    String aliasB = args[i + 3].isEmpty() ? nodeB : args[i + 3];

                    try (Session session = neo4jDriver.session()) {
                        try {
                            Result result = session.run("MATCH (a:attribute{name:$nodeA})-[r:has]->(b:attribute{name:$nodeB}) RETURN r.datasource as datasource, r.query as query, r.key as key, r.value as value",
                                    parameters("nodeA", nodeA, "nodeB", nodeB));

                            if (!result.hasNext()) {
                                throw new NoEdgeExistsException(nodeA, nodeB);
                            }

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
                                System.out.println("Key Positions: " + keyPositions);
                                System.out.println("Value Positions: " + valuePositions);

                                int rows = validateDatasourceExists(datasource, nodeA, nodeB);
                                DatasourceType datasourceType = findDatasourceType(rows);

                                if (datasourceType == DatasourceType.DB) {
                                    loadEdgesForDatabase(rows, keyPositions, valuePositions, queryString, aliasA,
                                            aliasB);
                                } else if (datasourceType == DatasourceType.CSV) {
                                    loadEdgesForCsv(rows, keyPositions, valuePositions, aliasA, aliasB);
                                } else if (datasourceType == DatasourceType.EXCEL) {
                                    loadEdgesForExcel(rows, keyPositions, valuePositions, aliasA, aliasB);
                                } else {
                                    loadEdgesForProcess(rows, keyPositions, valuePositions, aliasA, aliasB);
                                }
                            }
                        } catch (XPathExpressionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }



    public static void main(String[] args) {
        validateCmdArguments(args);
        initializeDocumentAndXpath();
        initializeDriver();
        loadEdges(args);
    }

}
