import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {

        try {
            ActualizarRangos actualizador = new ActualizarRangos();
            actualizador.datosFrontal();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}