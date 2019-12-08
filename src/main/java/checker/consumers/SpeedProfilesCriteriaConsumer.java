package checker.consumers;

import appenders.Appender;
import appenders.SpeedProfileAppender;
import checker.criterias.ICriteria;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import speed_profiles.DataProvider;
import speed_profiles.SpeedProfile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SpeedProfilesCriteriaConsumer implements ICriteriaConsumer {
    public static final String TABLE_NAME = "NTTP_SPEED_PATTERN";

    private static final String TEXT_TYPE = "TEXT";
    private static final String INTEGER_TYPE = "INTEGER";

    private static final Map<String, String> COLUMN_TYPE_BY_COLUMN_NAME = ImmutableMap.<String, String>builder()
            .put("PATTERN_ID", INTEGER_TYPE)
            .put("SEQ_NUM", INTEGER_TYPE)
            .put("SAMPLING_ID", INTEGER_TYPE)
            .put("START_TIME", TEXT_TYPE)
            .put("END_TIME", TEXT_TYPE)
            .put("SPEED_KPH", INTEGER_TYPE)
            .build();

    private static final Set<String> DAYS_OF_WEEK_PATTERNS = Arrays.stream(DayOfWeek.values())
            .map(day -> day + "_PATTERN_ID").collect(Collectors.toSet());

    private static final String QUERY = "select distinct SAMPLING_ID, PATTERN_ID from NTP_SPEED_PATTERN";
    private static final String FULL_ROW_QUERY = "select PATTERN_ID, SEQ_NUM, SAMPLING_ID, SPEED_KPH, " +
            "to_char(START_TIME, 'hh24:mi') as START_TIME, " +
            "to_char(END_TIME, 'hh24:mi') as END_TIME from NTP_SPEED_PATTERN " +
            "order by PATTERN_ID, SEQ_NUM";
    private static final String USED_PATTERN_ID_BY_LINKS_ = "select LINK_ID, SAMPLING_ID, " +
            "SUNDAY_PATTERN_ID, MONDAY_PATTERN_ID, TUESDAY_PATTERN_ID, WEDNESDAY_PATTERN_ID, THURSDAY_PATTERN_ID, FRIDAY_PATTERN_ID, SATURDAY_PATTERN_ID " +
            "from NTP_LINK_PATTERN";
    private static final String USED_PATTERN_ID_BY_LINKS = "select SAMPLING_ID, SUNDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, SUNDAY_PATTERN_ID\n" +
            "union all\n" +
            "select SAMPLING_ID, MONDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, MONDAY_PATTERN_ID\n" +
            "union all \n" +
            "select SAMPLING_ID, TUESDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, TUESDAY_PATTERN_ID\n" +
            "union all\n" +
            "select SAMPLING_ID, WEDNESDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, WEDNESDAY_PATTERN_ID\n" +
            "union all\n" +
            "select SAMPLING_ID, THURSDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, THURSDAY_PATTERN_ID\n" +
            "union all\n" +
            "select SAMPLING_ID, FRIDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, FRIDAY_PATTERN_ID\n" +
            "union all\n" +
            "select SAMPLING_ID, SATURDAY_PATTERN_ID as PATTERN_ID, count(*) as count from NTP_LINK_PATTERN group by SAMPLING_ID, SATURDAY_PATTERN_ID";




    private static final Comparator<Pair<Integer, Integer>> COMPARATOR = (o1, o2) ->
    {
      int leftDiff = Integer.compare(o1.getLeft(), o2.getLeft());
      int rightDiff = Integer.compare(o1.getRight(), o2.getRight());
      return leftDiff == 0 ? rightDiff : leftDiff;
    };

    private final String market;
    private final String dvn;
    private Map<String, Map<Integer, Set<Integer>>> profileIdsPerRegion = new TreeMap<>();
    private Map<Integer, Set<Integer>> profileIdsPerMarket = new TreeMap<>();
    // probably add SAMPLING_ID as well
    private Set<SpeedProfileRow> speedProfileRows = new TreeSet<>(Comparator.comparingInt(SpeedProfileRow::getPatternId)
            .thenComparing(SpeedProfileRow::getSeqNum));

    private Map<Pair<Integer, Integer>, AtomicLong> speedProfilesUsage = new TreeMap<>(COMPARATOR);
    @Getter
    private Map<Pair<Integer, Integer>, SpeedProfile> profiles = new TreeMap<>(COMPARATOR);

    public SpeedProfilesCriteriaConsumer(String market, String dvn) {
        this.market = market;
        this.dvn = dvn;
    }

    @Override
    public void processDbUser(String dbUser, String dbServerURL) {

    }

    @Override
    public void processDbUser(Connection connection, String dbUser, String dbServerURL) {
        long start = System.nanoTime();
//        String query = ICriteria.getQuery(FULL_ROW_QUERY, dbUser);
        String query = ICriteria.getQuery(USED_PATTERN_ID_BY_LINKS, dbUser);
        try (ResultSet resultSet = connection.createStatement().executeQuery(query)) {
            resultSet.setFetchSize(DEFAULT_FETCH_SIZE * 10);
//            collectSpeedProfiles(resultSet);
//            collectSpeedProfilesCoverage(resultSet, dbUser);
            collectPatternsUsage(resultSet);
        } catch (SQLException e) {
            LOGGER.error(String.format("Unable to process dbUser = %s, dbServerURL = %s, query = %s. Cause:%n%s",
                    dbUser, dbServerURL, query, e));
        } finally {
            LOGGER.debug(String.format("#processDbUser: %s Time elapsed = %d ms", dbUser,
                    TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }

    // QUERY|FULL_ROW_QUERY
    private void collectSpeedProfilesCoverage(ResultSet resultSet, String dbUser) throws SQLException {
        while (resultSet.next()) {
            int samplingId = resultSet.getInt("SAMPLING_ID");
            int patternId = resultSet.getInt("PATTERN_ID");
            int speed = resultSet.getInt("SPEED_KPH");

            profiles.computeIfAbsent(Pair.of(samplingId, patternId),
                    profile -> new SpeedProfile(patternId, samplingId)).addSpeed(speed);
            profileIdsPerRegion.computeIfAbsent(dbUser, samplings -> new TreeMap<>())
                    .computeIfAbsent(samplingId, patterns -> new TreeSet<>()).add(patternId);
            profileIdsPerMarket.computeIfAbsent(samplingId, patterns -> new TreeSet<>()).add(patternId);
        }
    }

    // FULL_ROW_QUERY
    private void collectSpeedProfiles(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            int samplingId = resultSet.getInt("SAMPLING_ID");
            int patternId = resultSet.getInt("PATTERN_ID");
            int seqNum = resultSet.getInt("SEQ_NUM");
            int speed = resultSet.getInt("SPEED_KPH");
            String startTime = resultSet.getString("START_TIME");
            String endTime = resultSet.getString("END_TIME");
            SpeedProfileRow profileRow = new SpeedProfileRow(patternId, seqNum, samplingId, startTime, endTime, speed);
            speedProfileRows.add(profileRow);
        }
    }

    // USED_PATTERN_ID_BY_LINKS
    private void collectPatternsUsage(ResultSet resultSet) throws SQLException {
//        Map<Pair<Integer, Integer>, Set<Integer>> patternsUsage = new HashMap<>();
        while (resultSet.next()) {
            int samplingId = resultSet.getInt("SAMPLING_ID");
            int patternId = resultSet.getInt("PATTERN_ID");
            int count = resultSet.getInt("count");
            speedProfilesUsage.computeIfAbsent(Pair.of(samplingId, patternId), usages -> new AtomicLong()).addAndGet(count);
           /* int linkId = resultSet.getInt("LINK_ID");
            for (String dayPattern : DAYS_OF_WEEK_PATTERNS) {
                int patternId = resultSet.getInt(dayPattern);
                patternsUsage.computeIfAbsent(Pair.of(samplingId, patternId), value -> new HashSet<>()).add(linkId);
            }*/
        }
       /* patternsUsage.forEach((pattern, links) ->
                speedProfilesUsage.computeIfAbsent(pattern, usages -> new AtomicLong()).addAndGet(links.size()));*/
    }

    public void printSpeedProfiles() {
        LOGGER.info("Per Regions:");
        profileIdsPerRegion.forEach((region, patterns) ->
        {
            StringBuilder builder = new StringBuilder();
            builder.append('|').append(region).append('|');
            patterns.forEach((sampleId, patternIds) ->
                    builder.append(patternIds.size()).append('|')
            );
            System.out.println(builder.toString());
        });
        StringBuilder builder = new StringBuilder("|Total|");
        profileIdsPerMarket.forEach((sampleId, patternIds) -> builder.append(patternIds.size()).append('|'));
        System.out.println("Per Market:\n" + builder.toString());
    }

    public void exportToSq3() {
        Path outputFile = Paths.get("Database", "src", "test", "java", "oracle", "output", "NTP_SPEED_PROFILES_" + market + ".sq3");
        DataProvider.exportToSq3(outputFile, new SpeedProfileAppender(Appender.Mode.WRITE), speedProfileRows);
    }

    public void exportProfilesUsage() {
        try {
            LOGGER.info("Speed profiles usage: " + speedProfilesUsage);
            Path outputFile = Paths.get("Database", "src", "test", "java", "oracle", "output", "NTP_SPEED_PROFILES_" + market + ".sq3");
//            Files.deleteIfExists(outputFile);
//            Files.createFile(outputFile);
            // register sqlite driver
            Class.forName("org.sqlite.JDBC");
            String createQuery = "CREATE TABLE IF NOT EXISTS PROFILES_USAGE_BY_LINKS " +
                    "(SAMPLING_ID INTEGER NOT NULL, PATTERN_ID INTEGER NOT NULL, USAGES_COUNT INTEGER NOT NULL)";
            String insertQuery = "INSERT INTO PROFILES_USAGE_BY_LINKS (SAMPLING_ID, PATTERN_ID, USAGES_COUNT) " +
                    "VALUES (?, ?, ?)";
            try (Connection connection = DataProvider.getConnection(outputFile);
                 Statement createStatement = connection.createStatement())
            {
                createStatement.execute(createQuery);
                PreparedStatement statement = connection.prepareStatement(insertQuery);
                connection.setAutoCommit(false);
                for (Map.Entry<Pair<Integer, Integer>, AtomicLong> entry : speedProfilesUsage.entrySet()) {
                    int columnIndex = 0;
                    statement.setInt(++columnIndex, entry.getKey().getLeft());
                    statement.setInt(++columnIndex, entry.getKey().getRight());
                    statement.setLong(++columnIndex, entry.getValue().get());
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
                statement.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Export to sqlite failed", e);
        }
    }

    @Data
    public static final class SpeedProfileRow {
        private final int patternId;
        private final int seqNum;
        private final int samplingId;
        private final String startTime;
        private final String endTime;
        private final int speed;
    }
}
