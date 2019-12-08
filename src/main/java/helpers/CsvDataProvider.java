package helpers;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by konstantin on 23.12.2017.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvDataProvider {

    public static List<List<String>> getData(String csvFile, char delimiter) {
        try {
            Reader reader = new FileReader(new File(csvFile));
            BufferedReader buffer = new BufferedReader(reader);
            List<List<String>> rows = new ArrayList<>();
            String line;
            while ((line = buffer.readLine()) != null) {
                rows.add(Arrays.asList(line.split(String.valueOf(delimiter))));
            }
            return rows;
        } catch (IOException e) {
            throw new IllegalArgumentException("No file found by path or file can not be parsed" + csvFile);
        }
    }

    public static void convertToCsv(Connection connection, String tableName, Path outputFile) {
        try {
            List<String> columns = getColumnsFromTable(connection, tableName);
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + tableName);
                 ResultSet resultSet = statement.executeQuery())
            {
                List<String> data = new ArrayList<>();
                String headers = columns.stream().collect(Collectors.joining(","));
                data.add(headers);
                while (resultSet.next())
                {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i <= columns.size(); i++)
                    {
                        row.append(resultSet.getInt(i));
                        if (i != columns.size())
                        {
                            row.append(',');
                        }
                    }
                    data.add(row.toString());
                }
                Files.write(outputFile, data);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }


    public static List<String> getColumnsFromTable(Connection connection, String tableName) throws SQLException {
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null)) {
            List<String> columns = new ArrayList<>();
            while (resultSet.next()) {
                columns.add(resultSet.getString("COLUMN_NAME"));
            }
            return columns.stream().distinct().collect(Collectors.toList());
        }
    }
}
