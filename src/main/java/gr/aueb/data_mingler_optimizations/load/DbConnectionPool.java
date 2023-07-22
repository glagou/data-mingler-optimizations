package gr.aueb.data_mingler_optimizations.load;

import gr.aueb.data_mingler_optimizations.enumerator.DatabaseType;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DbConnectionPool {
    private final Map<String, Connection> connectionMap = new HashMap<>();

    public Connection getOrCreateConnection(DatabaseType dbType, String url) throws SQLException {
        String datasourceKey = getConnectionKey(dbType, url);
        Connection connection = connectionMap.get(datasourceKey);
        if (connection == null) {
            connection = createConnection(url);
            connectionMap.put(datasourceKey, connection);
        }
        return connection;
    }

    private String getConnectionKey(DatabaseType dbType, String url) {
        return dbType.name().concat(StringConstant.HYPHEN.getValue()).concat(url);
    }

    private Connection createConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    public void closeConnections() {
        for (Connection connection : connectionMap.values()) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("Error closing connection: " + e.getMessage());
            }
        }
        connectionMap.clear();
    }
}
