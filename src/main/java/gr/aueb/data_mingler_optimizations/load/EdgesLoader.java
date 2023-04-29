package gr.aueb.data_mingler_optimizations.load;// This program is the same to loadEdgeNeo4j but loads multiple edges
// These are passed to command line string in the form: nodenameA_1, nodenameB_1, aliasA_1, aliasB_1,...
// So command line args must be divided by four

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.neo4j.driver.v1.*;
import org.w3c.dom.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

// Exit error codes:
// 1: wrong number of args
// 2: no edge(s) exist between nodes
// 3: the datasource name for an edge was not found in the dictionary
// 4: database vendor not supported
// 5: generic database/SQL execution error

public class EdgesLoader {
  public static void main(String[] args) throws ParserConfigurationException,
      XPathExpressionException, org.xml.sax.SAXException, IOException {

	// command arguments should contain:
	// nodeA_name - string
	// nodeB_name - string
	// nodeA_alias - string
	// nodeB_alias - string

	if ((args.length<4) || ((args.length%4)!=0)) { // wrong number of args
		System.out.println("Wrong number of arguments");
		System.exit(1);
	}

    long startTime = System.currentTimeMillis();

    org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j", "1234"));
    Jedis jedis = new Jedis("127.0.0.1", 6379);
    Pipeline jPipeline = jedis.pipelined();

	String dictionary = "C:\\DataMingler\\Implementation\\datasources.xml";
    DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = parser.parse(new File(dictionary));
    XPath xpath = XPathFactory.newInstance().newXPath();

	int numOfEdges = (args.length/4);
	for (int k=0; k<numOfEdges; k++) {
		String nodeA_name = args[4*k+0];
		String nodeB_name = args[4*k+1];
		String nodeA_alias = args[4*k+2];
		String nodeB_alias = args[4*k+3];

		if (nodeA_alias.equals(""))
			nodeA_alias = nodeA_name;
		if (nodeB_alias.equals(""))
			nodeB_alias = nodeB_name;

	    boolean found=false;
	    StatementResult result;

	    try (Session session = driver.session()) {
			found=false;
			result = session.run("MATCH (a:attribute{name:'"+nodeA_name+"'})-[r:has]->(b:attribute{name:'"+nodeB_name+"'}) RETURN r.datasource as datasource, r.query as query, r.key as key, r.value as value");
			// process each edge from nodeA to nodeB to populate the Key-List structure
	        while (result.hasNext()) {
				found=true;
				Record record = result.next();
				String datasource = record.get("datasource").asString();
				String queryString = record.get("query").asString();
				String[] keyPosStr = record.get("key").asString().split(",",-1); // a key can be composite, so key can be "1,2,3"
				String[] valuePosStr = record.get("value").asString().split(",",-1); // a value can be composite, so value can be "4,5"
				List<Integer> keyPositions = new ArrayList<Integer>(); //each column position of the key will be added to a list
				List<Integer> valuePositions = new ArrayList<Integer>(); // each column of the value will be added to a list
				for (int i=0; i<keyPosStr.length; i++)
					keyPositions.add(Integer.parseInt(keyPosStr[i]));
				for (int i=0; i<valuePosStr.length; i++)
					valuePositions.add(Integer.parseInt(valuePosStr[i]));
				System.out.println("Key Positions: "+keyPositions.toString());
				System.out.println("Value Positions: "+valuePositions.toString());

				//int keyPos = Integer.parseInt(record.get("key").asString());
				//int valuePos = Integer.parseInt(record.get("value").asString());
				//int keyPos = record.get("key").asInt();
				//int valuePos = record.get("value").asInt();
				//System.out.println(datasource+","+queryString+","+keyPos+","+valuePos);
				// find the specifications for the datasource
				// first check if the datasource exists
			    int rows=1;
			    boolean exists=false;
			    while (xpath.evaluate("/datasources/datasource[position()="+rows+"]",doc).trim()!="") {
					//System.out.println(xpath.evaluate("/datasources/datasource[position()="+rows+"]/name",doc).trim());
			    	if (xpath.evaluate("/datasources/datasource[position()="+rows+"]/name",doc).trim().equals(datasource)) {
						exists=true;
						break;
					}
					rows++;
				}
			    if (!exists) {
					System.out.println("Datasource: "+datasource+" was not found in the dictionary for the edge between nodes "+nodeA_name+" and "+nodeB_name);
					System.exit(3);
				}
				// now process according the type of the datasource
				String datasourceType = xpath.evaluate("/datasources/datasource[position()="+rows+"]/@type",doc).trim();

				// db type
				if (datasourceType.equals("db")) {	// DB - which one? retrieve system element
					String dbSystem = xpath.evaluate("/datasources/datasource[position()="+rows+"]/system",doc).trim();
					String connString = xpath.evaluate("/datasources/datasource[position()="+rows+"]/connection",doc).trim();
					String username = xpath.evaluate("/datasources/datasource[position()="+rows+"]/username",doc).trim();
					String password = xpath.evaluate("/datasources/datasource[position()="+rows+"]/password",doc).trim();
					String database = xpath.evaluate("/datasources/datasource[position()="+rows+"]/database",doc).trim();

	        		try {
						Connection connection=null;
			            if (dbSystem.equals("msaccess"))
			            	connection = DriverManager.getConnection("jdbc:ucanaccess://"+connString);
			            else if (dbSystem.equals("oracle")) ;
			            else if (dbSystem.equals("mysql")) ;
			            else if (dbSystem.equals("sqlserver")) {
							Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
							if (username.equals("") && password.equals("")) // windows authentication
								connection=DriverManager.getConnection("jdbc:sqlserver://" + connString + ";databaseName = " + database + ";integratedSecurity=true;");
							else // username, password authentication
        						connection=DriverManager.getConnection("jdbc:sqlserver://" + connString + ";databaseName = " + database + ";username=" + username + ";password=" + password);
						}
			            else if (dbSystem.equals("postgres")) ;
			            else if (dbSystem.equals("db2")) ;
			            else {
							System.out.println("This database vendor is not supported");
							System.exit(4);
						}

	            		Statement statement = connection.createStatement();
			            ResultSet resultSet = statement.executeQuery(queryString);
	            		while(resultSet.next()) {
							String key = "";
							for (int i=0; i<keyPositions.size(); i++) {
								if (i!=0) key += ":";
								key += resultSet.getString(keyPositions.get(i)); // 1,2,...
							}
							String value = "";
							for (int i=0; i<valuePositions.size(); i++) {
								if (i!=0) value += ":";
								value += resultSet.getString(valuePositions.get(i)); // 4,5,...
							}
							//System.out.println(key + " -- " + value);
							jPipeline.rpush(nodeA_alias+"-"+nodeB_alias+":"+key,value);
							jPipeline.sadd(nodeA_alias+"-"+nodeB_alias, key);

	        		    }
	        		    jPipeline.sync();

						resultSet.close();
	                    statement.close();
	                    connection.close();
	        		}
	        		catch(Exception sqlex){
	        		    sqlex.printStackTrace();
	        		    System.exit(5);
	        		}


				}

				// csv type
				if (datasourceType.equals("csv")) {
					String fileName = xpath.evaluate("/datasources/datasource[position()="+rows+"]/filename",doc).trim();
					String path = xpath.evaluate("/datasources/datasource[position()="+rows+"]/path",doc).trim();
					boolean headings = xpath.evaluate("/datasources/datasource[position()="+rows+"]/headings",doc).trim().equals("yes");
					String delimiter = xpath.evaluate("/datasources/datasource[position()="+rows+"]/delimiter",doc).trim();

					FileInputStream is = new FileInputStream(path+fileName);
	    			Reader csvFile = new InputStreamReader(is);
	    			BufferedReader buf = new BufferedReader(csvFile);
					String inputLine;
					int rowCounter=0;
					while ((inputLine = buf.readLine()) != null) {
						rowCounter++;
						if (rowCounter==1 && headings) continue;
						String[] columns = inputLine.split(delimiter,-1);

						String key = "";
						for (int i=0; i<keyPositions.size(); i++) {
							if (i!=0) key += ":";
							key += columns[keyPositions.get(i)-1]; // we assume csv columns are numbered 1,2,... in the graph, that's why the minus 1
						}
						String value = "";
						for (int i=0; i<valuePositions.size(); i++) {
							if (i!=0) value += ":";
							value += columns[valuePositions.get(i)-1]; // we assume csv columns are numbered 1,2,... in the graph, that's why the minus 1
						}

						//System.out.println(key + " -- " + value);
						jPipeline.rpush(nodeA_alias+"-"+nodeB_alias+":"+key,value);
						jPipeline.sadd(nodeA_alias+"-"+nodeB_alias, key);
					}
					jPipeline.sync();
				}

				// excel type - xlsx
				if (datasourceType.equals("excel")) {
					String fileName = xpath.evaluate("/datasources/datasource[position()="+rows+"]/filename",doc).trim();
					String path = xpath.evaluate("/datasources/datasource[position()="+rows+"]/path",doc).trim();
					String sheetName = xpath.evaluate("/datasources/datasource[position()="+rows+"]/sheet",doc).trim();
					boolean headings = xpath.evaluate("/datasources/datasource[position()="+rows+"]/headings",doc).trim().equals("yes");

				    Workbook xlWBook;
				    Sheet xlSheet;
				    Row xlRow;
				    Cell xlCell;
				    DataFormatter formatter = new DataFormatter();
		            FileInputStream xlFile = new FileInputStream(path+fileName);
		            xlWBook = new XSSFWorkbook(xlFile);
		            xlSheet = xlWBook.getSheet(sheetName);
	            	int noOfRows = xlSheet.getPhysicalNumberOfRows();

		            for (int r = 0; r < noOfRows; r++) {
						if (r==0 && headings) continue;
	                    xlRow = xlSheet.getRow(r);

	                    //xlCell = xlRow.getCell(keyPos-1); // we assume excel columns are numbered 1,2,... in the graph, that's why the minus 1
						//String key = formatter.formatCellValue(xlCell); //xlCell.getStringCellValue();
	                    //xlCell = xlRow.getCell(valuePos-1); // we assume excel columns are numbered 1,2,... in the graph, that's why the minus 1
						//String value = formatter.formatCellValue(xlCell);

						String key = "";
						for (int i=0; i<keyPositions.size(); i++) {
							xlCell = xlRow.getCell(keyPositions.get(i)-1);
							if (i!=0) key += ":";
							key += formatter.formatCellValue(xlCell); // we assume columns are numbered 1,2,... in the graph, that's why the minus 1
						}
						String value = "";
						for (int i=0; i<valuePositions.size(); i++) {
							xlCell = xlRow.getCell(valuePositions.get(i)-1);
							if (i!=0) value += ":";
							value += formatter.formatCellValue(xlCell); // we assume columns are numbered 1,2,... in the graph, that's why the minus 1
						}

						// System.out.println(key + " -- " + value);
						jPipeline.rpush(nodeA_alias+"-"+nodeB_alias+":"+key,value);
						jPipeline.sadd(nodeA_alias+"-"+nodeB_alias, key);
					}
					jPipeline.sync();
				}

				// process that can be executed via some runtime (e.g. java, python)
				if (datasourceType.equals("process")) {
					String path = xpath.evaluate("/datasources/datasource[position()="+rows+"]/path",doc).trim();
					String fileName = xpath.evaluate("/datasources/datasource[position()="+rows+"]/filename",doc).trim();
					String engPath = xpath.evaluate("/datasources/datasource[position()="+rows+"]/enginePath",doc).trim();
					String engine = xpath.evaluate("/datasources/datasource[position()="+rows+"]/engine",doc).trim();
					String delimiter = xpath.evaluate("/datasources/datasource[position()="+rows+"]/delimiter",doc).trim();

					try {
						Process p = Runtime.getRuntime().exec(engPath+engine+" "+path+fileName);

		        		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
						//BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

						String inputLine = null;
						while ((inputLine = stdInput.readLine()) != null) {
							String[] columns = inputLine.split(delimiter,-1);

							String key = "";
							for (int i=0; i<keyPositions.size(); i++) {
								if (i!=0) key += ":";
								key += columns[keyPositions.get(i)-1]; // we assume csv columns are numbered 1,2,... in the graph, that's why the minus 1
							}
							String value = "";
							for (int i=0; i<valuePositions.size(); i++) {
								if (i!=0) value += ":";
								value += columns[valuePositions.get(i)-1]; // we assume csv columns are numbered 1,2,... in the graph, that's why the minus 1
							}

							//System.out.println(key + " -- " + value);
							jPipeline.rpush(nodeA_alias+"-"+nodeB_alias+":"+key,value);
							jPipeline.sadd(nodeA_alias+"-"+nodeB_alias, key);
						}
						jPipeline.sync();

						//System.out.println("Here is the standard error of the command:\n");
						//s = null;
						//while ((s = stdError.readLine()) != null) {
						//	System.out.println(s);
						//}

						p.waitFor();

		        		int returnValue = p.exitValue();
		        		if (returnValue!=0) {
							System.out.println("loadEdge failed during the evaluation of edge: "+nodeA_name+"->"+nodeB_name+" with return error code:"+returnValue);
							System.exit(4);
						}
					}
					catch (InterruptedException e) {
								e.printStackTrace();
					}

				}


	        } // end of the while loop that processes each edge

	        if (!found) {
				System.out.println("There is no edge between "+nodeA_name+" and "+nodeB_name);
				System.exit(2);
			}
	    }
	} // of k loop

    driver.close();

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println(estimatedTime);

  } // of main
} // of program


