package appenders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public interface Appender<T> {

    enum Mode
    {
        READ,
        WRITE,
        APPEND
    }

    void append(PreparedStatement statement, T object);

    void append(Connection connection, String query, T object);

    void append(Connection connection, Collection<? extends T> objects) throws SQLException;

    default String getCreateQuery(String tableName, Map<String, String> columnsToType)
    {
        return "CREATE TABLE IF NOT EXISTS " +
                tableName +
                columnsToType.entrySet().stream()
                        .map(columnData -> columnData.getKey() + " " + columnData.getValue())
                        .collect(Collectors.joining(" , ", " (", " )"));
    }

    default String getInsertQuery(String tableName, Map<String, String> columnsToType) {
        return "INSERT INTO " +
                tableName +
                columnsToType.keySet().stream()
                        .collect(Collectors.joining(",", " (", ") ")) +
                columnsToType.entrySet().stream()
                        .map(columnData -> "?")
                        .collect(Collectors.joining(",", "VALUES (", ") "));
    }

    Mode getMode();
}
