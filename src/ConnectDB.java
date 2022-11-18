import java.sql.*;

public class ConnectDB {
    private static final String dbURL = "jdbc:oracle:thin:@//localhost:1521/orclpdb";
    private static final String username = "hr";
    private static final String password = "hr";

    public static Connection connect() throws SQLException {
        Connection connection;
        try {
            connection = DriverManager.getConnection(dbURL,username,password);
            System.out.println("Successfully connected to Oracle database server!");
        } catch (SQLException e) {
            System.out.println("Error occurred: ");
            throw new RuntimeException(e);
        }
        return connection;
    }

}
