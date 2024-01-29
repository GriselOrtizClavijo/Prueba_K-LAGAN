import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public record ConexionBBDD() {

    private static String username = "root";
    private static String password = "admin";

    private static String url = "url";

    public Connection getConnection(){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url,username,password);
            return connection;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
