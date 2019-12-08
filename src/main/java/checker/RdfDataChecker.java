package checker;

import checker.consumers.SpeedProfilesCriteriaConsumer;
import checker.readers.TxtUsersReader;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RdfDataChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(RdfDataChecker.class);
    private static final String USERS_QUERY = "select username from dba_users WHERE REGEXP_LIKE(username, '^CDCA_.*_%s$')";
    private static final String NO_SAMPLE_USERS = String.format("%s and not REGEXP_LIKE(username, '%s')", USERS_QUERY, TxtUsersReader.getSampleRegions());
    // when it's required to determine specific regions
    private static final String SPECIFIC_REGIONS = " and REGEXP_LIKE(username, '%s')";
    private static final String DB_DEFAULT_USER = "AKELA";
    private static final String DB_PASSWORD = "password";
    private static final String DB_SERVER_URL = "jdbc:oracle:thin:@akela-%s-%s-0%d.civof2bffmif.us-east-1.rds.amazonaws.com:1521:orcl";
    // TODO: put to build_config.properties or split by different files
    private static final Map<String, String> MARKET_TO_DVN = ImmutableMap.<String, String>builder()
            .put("eu", "191T1")
            .put("nar", "191T1")
            .put("mrm", "191T1")
            .build();

    private static void processTraversingUsers() {
        MARKET_TO_DVN.forEach((market, dvn) ->
        {
            LOGGER.info(String.format("################## market = %s, dvn = %s ##################", market, dvn));
            Set<Integer> processedServers = IntStream.rangeClosed(1, 8).boxed().collect(Collectors.toSet());
            TxtUsersReader reader = new TxtUsersReader(market, dvn);
            Set<String> allUsers = reader.getCdcUsers();
//                Set<String> allUsers = reader.getSampleCdcUserWithDVN(dvn);
            Set<String> iterateUsers = new HashSet<>(allUsers);
            SpeedProfilesCriteriaConsumer consumer = new SpeedProfilesCriteriaConsumer(market, dvn);
            allUsers.stream().filter(iterateUsers::contains).forEach(cdcUser ->
            {
                for (int i : processedServers) {
                    String dbServerUrl = String.format(DB_SERVER_URL, market, dvn, i)
                            // exceptional case
                            .replaceAll("_", "-");
                    Set<String> dbServerUsers = new HashSet<>();
                    try (Connection connection = DriverManager.getConnection(dbServerUrl, cdcUser, DB_PASSWORD);
                         ResultSet userNames = connection.createStatement().executeQuery(String.format(USERS_QUERY, dvn))) { // + NO_SAMPLE_USERS_PREDICATE
                        LOGGER.debug("Start processing dbServer = " + dbServerUrl);
                        while (userNames.next()) {
                            dbServerUsers.add(userNames.getString(1));
                        }
                        dbServerUsers = reader.withoutSampleUsers(dbServerUsers);
                        LOGGER.debug("Collected CDCA users: " + dbServerUsers);
                        // users
                        dbServerUsers.forEach(userName ->
                        {
                            consumer.processDbUser(connection, userName, dbServerUrl);
                            synchronized (iterateUsers) {
                                iterateUsers.remove(userName);
                            }
                        });
                    } catch (Exception e) {
//                            System.err.println(String.format("No SourceDbUser = %s, dbServer = %s", cdcUser, dbServerUrl));
                    }
                    if (!dbServerUsers.isEmpty()) {
                        processedServers.remove(i);
                        LOGGER.debug("Finish processing dbServer = " + dbServerUrl);
                        break;
                    }
                }
            });
            LOGGER.debug("Remaining users: " + iterateUsers);
            consumer.printSpeedProfiles();
            consumer.exportToSq3();
            consumer.exportProfilesUsage();
        });
    }

    private static void processWithDefaultUser() {
        MARKET_TO_DVN.forEach((market, dvn) ->
        {
            LOGGER.info(String.format("################## market = %s, dvn = %s ##################", market, dvn));
            TxtUsersReader reader = new TxtUsersReader(market, dvn);
            SpeedProfilesCriteriaConsumer consumer = new SpeedProfilesCriteriaConsumer(market, dvn);
            for (int i = 1; i <= 8; i++) {
                String dbServerUrl = String.format(DB_SERVER_URL, market, dvn, i)
                        // exceptional case
                        .replaceAll("_", "-");
                Set<String> dbServerUsers = new HashSet<>();
                LOGGER.debug("Start processing dbServer = " + dbServerUrl);
                try (Connection connection = DriverManager.getConnection(dbServerUrl, DB_DEFAULT_USER, DB_PASSWORD);
                     ResultSet cdcUsers = connection.createStatement().executeQuery(String.format(USERS_QUERY, dvn))) { // + NO_SAMPLE_USERS_PREDICATE
                    LOGGER.debug("Connect to dbServer = " + dbServerUrl);
                    while (cdcUsers.next()) {
                        dbServerUsers.add(cdcUsers.getString(1));
                    }
                    dbServerUsers = reader.withoutSampleUsers(dbServerUsers);
                    LOGGER.debug("Collected CDCA users: " + dbServerUsers);
                    // users
                    dbServerUsers.forEach(userName -> consumer.processDbUser(connection, userName, dbServerUrl));
                } catch (Exception e) {
                    LOGGER.error(String.format("No SourceDbUser = %s, dbServer = %s. Cause: = %s", DB_DEFAULT_USER, dbServerUrl, e.toString()));
                }
                LOGGER.debug("Finish processing dbServer = " + dbServerUrl);
            }
            consumer.printSpeedProfiles();
            consumer.exportToSq3();
            consumer.exportProfilesUsage();
        });
    }


    public static void main(String[] args) {
        long start = System.nanoTime();
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
//            DriverManager.setLoginTimeout(60);
            processTraversingUsers();
//          processWithDefaultUser();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            LOGGER.info(String.format("Time elapsed = %d s", TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }
}
