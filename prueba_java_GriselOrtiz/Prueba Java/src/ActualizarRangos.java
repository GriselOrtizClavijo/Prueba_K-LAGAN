
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ActualizarRangos {

    //Ejemplo con fecha que cumple el formato
    //private String fechaModificacion = "2024-01-29 13:30:40.123";

    //Ejemplo con fecha que no cumple formato
    private String fechaModificacion = "2024-01-29 13:30";
    private Integer bimestresModificacion = 1;
    private String rangoInicial = "AA-100";
    private String rangoFin = "AA-2500";

    public void setFechaModificacion(String fechaModificacion) {
        this.fechaModificacion = fechaModificacion;
    }

    //Declaración de la variable Conexión a la base de datos
    private ConexionBBDD conexion = new ConexionBBDD();

    public void datosFrontal() throws SQLException {
        Map<String, Object> ctx = new HashMap<>();

        ctx.put("FechaModificacion", fechaModificacion);
        ctx.put("bimestresModificacion", bimestresModificacion);
        ctx.put("rangoInicial", rangoInicial);
        ctx.put("rangoFin", rangoFin);

        if (validarDatos(ctx)) {
            buscarActualizarRegistro(ctx);
        } else {
            info("Datos que no cumplen con los requisitos de validación");
        }

    }

    private boolean validarDatos(Map<String, Object> ctx) {

        if (!esTimestampValido((String) ctx.get("FechaModificacion"))) {
            info("La fecha introducida debe ser un timeStamp válido (yyyy-MM-dd HH:mm:ss.SSS)");
            return false;
        }

        if (!cantidadValores((String) ctx.get("rangoInicial"), (String) ctx.get("rangoFin"))) {
            info("Debe haber al menos 1000 valores entre rango Inicial y Final.");
            return false;
        }

        if (!esPeriodoFuturo((String) ctx.get("FechaModificacion"))) {
            info("Solo se puede actualizar o insertar un periodo futuro.");
            return false;
        }
        info("Datos validados, se puede realizar paso a actualización");
        return true;
    }


    private boolean esTimestampValido(String fechaModificacion) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
            LocalDateTime localDateTime = LocalDateTime.parse(fechaModificacion, formatter);
            Timestamp timestamp = Timestamp.valueOf(localDateTime);

            this.fechaModificacion = timestamp.toString();
            info("Se realiza parseo de fecha: " +  timestamp);
            return true;
        } catch (Exception e) {
            info("Error al intentar parsear el formato de fecha: " + e.getMessage());
            return false;
        }
    }


    private boolean cantidadValores(String rangoInicial, String rangoFin) {
        int inicio = extraerParteNumerica(rangoInicial);
        int fin = extraerParteNumerica(rangoFin);

        if ((fin - inicio) >= 1000) {
            info("Diferencia de valores superior a 1000");
            return true;
        }else {
            info("Diferencia de valores inferior a 1000");
            return false;
        }
    }


    private boolean esPeriodoFuturo(String fechaModificacion) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime localDateTime = LocalDateTime.parse(fechaModificacion, formatter);
        Timestamp timestamp = Timestamp.valueOf(localDateTime);

        this.fechaModificacion = timestamp.toString();
        LocalDateTime currentDate = LocalDateTime.now();

        if (timestamp.after(Timestamp.valueOf(currentDate))) {
            info("La fecha es un periodo futuro");
            return true;
        } else {
            info("La fecha no es un periodo futuro");
            return false;
        }
    }

    public void buscarActualizarRegistro(Map<String, Object> ctx) throws SQLException {

        fechaModificacion = (String) ctx.get("FechaModificacion");
        bimestresModificacion = (Integer) ctx.get("bimestresModificacion");
        rangoInicial = (String) ctx.get("rangoInicial");
        rangoFin = (String) ctx.get("rangoFin");

        String queryFechaActualizacion = "SELECT * FROM RANGOS WHERE FechaModificacion = :FECHA_MODIFICACION";
        Map<String, Object> paramsFechaActualización = Map.of("FECHA_MODIFICACION", fechaModificacion);
        List<Map<String, Object>> resultadoFechaActualizacion = sqlQuery(queryFechaActualizacion, paramsFechaActualización);

        String queryBimestre = "SELECT * FROM RANGOS WHERE bimestresModificacion = :BIMESTRE";
        Map<String, Object> paramsBimestre = Map.of("BIMESTRE", bimestresModificacion);
        List<Map<String, Object>> resultadoBimestre = sqlQuery(queryBimestre, paramsBimestre);

        String queryRangoInicial = "SELECT * FROM RANGOS WHERE rangoInicial = :RANGO_INICIAL";
        Map<String, Object> paramsaRangoInicial = Map.of("RANGO_INICIAL", rangoInicial);
        List<Map<String, Object>> resultadoRangoInicial = sqlQuery(queryRangoInicial, paramsaRangoInicial);

        String queryRangoFin = "SELECT * FROM RANGOS WHERE rangoFin = :RANGO_FINAL";
        Map<String, Object> paramsRangoFin = Map.of("RANGO_FINAL", rangoFin);
        List<Map<String, Object>> resultadoRangoFin = sqlQuery(queryRangoFin, paramsRangoFin);

        LocalDateTime fechaModificacionDateTime = LocalDateTime.parse(fechaModificacion, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));

        if (resultadoFechaActualizacion.isEmpty()
                && resultadoBimestre.isEmpty()
                && resultadoRangoInicial.isEmpty()
                && resultadoRangoFin.isEmpty()
        ) {
            String queryInsercion = "INSERT INTO RANGOS (Anho, Bimestre, Rango_Inicial, Rango_Final, Ultima_actualizacion) VALUES (?, ?, ?, ?, ?)";
            Map<String, Object> paramsInsercion = new HashMap<>();

            paramsInsercion.put("Anho", fechaModificacionDateTime.getYear());
            paramsInsercion.put("Bimestre", bimestresModificacion);
            paramsInsercion.put("Rango_Inicial", rangoInicial);
            paramsInsercion.put("Rango_Final", rangoFin);
            paramsInsercion.put("Ultima_actualizacion", fechaModificacion);

            sqlUpdate("RANGOS", queryInsercion, paramsInsercion);
        } else {
            Map<String, Object> registroExistente = resultadoFechaActualizacion.get(0);
            Integer bimestre = (Integer) registroExistente.get("Bimestre");
            String rangoInicialExistente = (String) registroExistente.get("Rango_Inicial");
            String rangoFinalExistente = (String) registroExistente.get("Rango_Final");

            if (!resultadoFechaActualizacion.equals(fechaModificacion) ||!bimestre.equals(bimestresModificacion) || !rangoInicialExistente.equals(rangoInicial) || !rangoFinalExistente.equals(rangoFin)) {
                String queryInsercion = "INSERT INTO RANGOS (Anho, Bimestre, Rango_Inicial, Rango_Final, Ultima_actualizacion) VALUES (?, ?, ?, ?, ?)";
                Map<String, Object> paramsInsercion = new HashMap<>();

                Map<String, Object> paramsActualizacion = new HashMap<>();
                paramsActualizacion.put("Rango_Inicial", rangoInicial);
                paramsActualizacion.put("Rango_Final", rangoFin);
                paramsActualizacion.put("Ultima_actualizacion", LocalDateTime.now());
                paramsActualizacion.put("Anho", fechaModificacionDateTime.getYear());
                paramsActualizacion.put("Bimestre", bimestresModificacion);

                sqlUpdate("RANGOS", queryInsercion , paramsInsercion);
            }
        }

    }

    private int extraerParteNumerica(String rango) {
        return Integer.parseInt(rango.split("-")[1]);
    }


    //Helpers
    private void info(String mensaje) {
        System.out.println(mensaje);
    }

    private List<Map<String, Object>> sqlQuery(String query, Map<String, Object> params) {

        List<Map<String, Object>> resultados = new ArrayList<>();
        Map<String, Object> resultado = new HashMap<>();

        resultado.put("ID_RANGO", 1);
        resultado.put("Anho", 2022);
        resultado.put("Bimestre", 1);
        resultado.put("Rango_Inicial", "AA-1000");
        resultado.put("Rango_Final", "AA-3000");
        resultado.put("Ultima_actualizacion", Timestamp.valueOf("2022-01-27 10:30:00"));
        resultados.add(resultado);

        return resultados;
    }

    private void sqlUpdate(String bbdd, String query, Map<String, Object> params) throws SQLException {
        if ("RANGOS".equals(bbdd)) {
            if (query.contains("UPDATE")) {
                try (PreparedStatement preparedStatement = conexion.getConnection().prepareStatement(query)) {
                    configurarParametros(preparedStatement, params);
                    preparedStatement.executeUpdate();
                info("Simulando inserción SQL: " + query);
                } catch (SQLException e) {
                    info("No se requiere actualización de datos " + e);
                }
            } else if (query.contains("INSERT")) {
                info("Simulando inserción SQL: " + query);
                try (PreparedStatement preparedStatement = conexion.getConnection().prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                    configurarParametros(preparedStatement, params);
                    preparedStatement.executeUpdate();
                }catch (SQLException e) {
                    info("No se requiere actualización de datos " + e);
                }
            } else {
                info("Consulta SQL no reconocida: " + query);
            }
        } else {
            info("Base de datos: " + bbdd + " no reconocida.");
        }
    }

    private void configurarParametros(PreparedStatement preparedStatement, Map<String, Object> params) throws SQLException {

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if ("ID_RANGO".equals(key) || "Anho".equals(key) || "Bimestre".equals(key)) {
                preparedStatement.setInt(Integer.parseInt(key), (Integer) value);
            } else if ("Rango_Inicial".equals(key) || "Rango_Final".equals(key)) {
                preparedStatement.setString(Integer.parseInt(key), (String) value);
            } else if ("Rango_Final".equals(key) || "Rango_Final".equals(key)) {
                preparedStatement.setString(Integer.parseInt(key), (String) value);
            } else if ("Ultima_actualizacion".equals(key)) {
                preparedStatement.setTimestamp(Integer.parseInt(key), (Timestamp) value);
            } else {
                {
                    info("No se encuentran datos para configurar");
                }
            }
        }
    }

}