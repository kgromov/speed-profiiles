package speed_profiles;

import appenders.Appender;
import checker.consumers.SpeedProfilesCriteriaConsumer;
import helpers.CsvDataProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProvider.class.getName());

    private static final String DB_URI_PREFIX = "jdbc:sqlite:file:";
    public static final String ATTACH_QUERY = "ATTACH database '%s' as %s";
    public static final String DETACH_QUERY = "DETACH database %s";

    private static final String QUERY = "select * from " + SpeedProfilesCriteriaConsumer.TABLE_NAME + " order by PATTERN_ID, SEQ_NUM";
    private static final String USAGE_QUERY = "select * from PROFILES_USAGE_BY_LINKS";
    private static final String OUTPUT_FOLDER = "C:\\Projects\\java-examples\\Database\\src\\test\\java\\oracle\\output";

    public static final BiPredicate<Path, BasicFileAttributes> ORIGINAL_PROFILES =
            ((path, attributes) -> attributes.isRegularFile() && path.toString().endsWith(Extension.SQ3.getValue()));

    public static final BiPredicate<Path, BasicFileAttributes> AGGREGATED_PROFILES =
            ((path, attributes) -> attributes.isRegularFile()
                    && path.toString().endsWith(Extension.SQ3.getValue())
                    && path.toString().contains("_AGGREGATED"));

    public enum Extension {
        SQ3(".sq3"),
        CSV(".csv");

        private String value;

        Extension(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static void exportSourceDataToCsv(BiPredicate<Path, BasicFileAttributes> matcher) {
        Path outFolder = Paths.get(OUTPUT_FOLDER);
        try {
            Class.forName("org.sqlite.JDBC");
            Files.find(outFolder, 1, matcher)
                    .forEach(dbPath ->
                    {
                        Path csvPath = outFolder.resolve(dbPath.getFileName().toString().replace(Extension.SQ3.getValue(), Extension.CSV.getValue()));
                        try {
                            Files.deleteIfExists(csvPath);
                            Files.createFile(csvPath);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try (Connection connection = getConnection(dbPath)) {
                            CsvDataProvider.convertToCsv(connection, SpeedProfilesCriteriaConsumer.TABLE_NAME, csvPath);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<Integer, List<SpeedProfile>> extractSpeedProfiles(String market) {
        long start = System.nanoTime();
        Path dbPath = getPath("NTP_SPEED_PROFILES", market, Extension.SQ3);
        try (Connection connection = getConnection(dbPath);
             ResultSet resultSet = connection.createStatement().executeQuery(QUERY)) {
            Map<Integer, List<SpeedProfile>> speedProfilesPerSamplingId = new HashMap<>();
            Map<Integer, SpeedProfile> speedProfiles = new TreeMap<>();
            while (resultSet.next()) {
                int samplingId = resultSet.getInt("SAMPLING_ID");
                int patternId = resultSet.getInt("PATTERN_ID");
                int speed = resultSet.getInt("SPEED_KPH");
                // speed = speed * 100 for more accurate precision
//                int speed = resultSet.getInt("SPEED_KPH") * 100;

                SpeedProfile speedProfile = speedProfiles.computeIfAbsent(patternId, profile -> new SpeedProfile(patternId, samplingId));
                // performance optimization - add new created speedProfile
                if (speedProfile.getSpeedPerTime().isEmpty()) {
                    speedProfilesPerSamplingId.computeIfAbsent(samplingId, profiles -> new ArrayList<>()).add(speedProfile);
                }
                speedProfile.addSpeed(speed);
            }
            return speedProfilesPerSamplingId;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to extractSpeedProfiles; query = %s" + QUERY, e);
        } finally {
            LOGGER.trace(String.format("#extractSpeedProfiles: Time elapsed = %d ms",
                    TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }

    public static Map<Integer, Map<Integer, Integer>> getSpeedProfilesUsage(String market) {
        long start = System.nanoTime();
        Path dbPath = getPath("NTP_SPEED_PROFILES", market, Extension.SQ3);
        try (Connection connection = getConnection(dbPath);
             ResultSet resultSet = connection.createStatement().executeQuery(USAGE_QUERY)) {
            Map<Integer, Map<Integer, Integer>> profilesUsage = new HashMap<>();
            while (resultSet.next()) {
                int samplingId = resultSet.getInt("SAMPLING_ID");
                int patternId = resultSet.getInt("PATTERN_ID");
                int usagesCount = resultSet.getInt("USAGES_COUNT");
                profilesUsage.computeIfAbsent(samplingId, profiles -> new HashMap<>()).put(patternId, usagesCount);
            }
            return profilesUsage;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to extract speedProfiles usage; query = %s" + USAGE_QUERY, e);
        } finally {
            LOGGER.trace(String.format("#extractSpeedProfiles: Time elapsed = %d ms",
                    TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }

    public static <T> void exportToSq3(Path outputFile, Appender<T> appender, Collection<? extends T> objects) {
        try {
            if (appender.getMode() == Appender.Mode.WRITE) {
                Files.deleteIfExists(outputFile);
                Files.createFile(outputFile);
            }
            // register sqlite driver
            Class.forName("org.sqlite.JDBC");
            try (Connection connection = getConnection(outputFile)) {
                appender.append(connection, objects);
            }
        } catch (Exception e) {
            throw new RuntimeException("Export to sqlite failed", e);
        }
    }

    public static Connection getConnection(Path dbPath) throws SQLException {
        return DriverManager.getConnection(DB_URI_PREFIX + dbPath.toString());
    }

    public static Path getPath(String fileName, String market, Extension extension) {
        Path outFolder = Paths.get(OUTPUT_FOLDER);
        return outFolder.resolve(fileName + "_" + market + extension.getValue());
    }
}
