package speed_profiles;

import appenders.AggregatedSpeedProfileAppender;
import appenders.Appender;
import appenders.SpeedProfileAppender;
import checker.consumers.SpeedProfilesCriteriaConsumer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speed_profiles.merge.HalfAverageMergeStrategy;
import speed_profiles.merge.MergeStrategy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class Aggregation {
    private static final Logger LOGGER = LoggerFactory.getLogger(Aggregation.class);
//        public static final Map<Integer, Integer> AGGREGATION_DEPTH = ImmutableMap.of(1, 4, 2, 1);
    public static final Map<Integer, Integer> AGGREGATION_DEPTH = ImmutableMap.of(1, 8, 2, 2, 4, 1);
    private static final Set<String> MARKETS = Sets.newHashSet("eu", "nar", "mrm");
   /* private static final Comparator<SpeedProfile> SPEED_PROFILE_COMPARATOR = Comparator.comparingInt(SpeedProfile::getAverageSpeed)
            .thenComparing(SpeedProfile::getAverageDaySpeed)
            .thenComparing(SpeedProfile::getAverageNightSpeed)
            .thenComparing(SpeedProfile::getMinSpeed)
            .thenComparing(SpeedProfile::getMaxSpeed);*/

    private static final Comparator<SpeedProfile> SPEED_PROFILE_COMPARATOR = Comparator.comparingInt(SpeedProfile::getAverageDaySpeed)
            .thenComparing(SpeedProfile::getAverageNightSpeed)
            .thenComparing(SpeedProfile::getAverageSpeed)
            .thenComparing(SpeedProfile::getMinSpeed)
            .thenComparing(SpeedProfile::getMaxSpeed);

    private static final IntFunction<Integer> CONSTANT_GROUP_COUNT = iterations -> 4;
    private static final IntFunction<Integer> INCREMENT_GROUP_COUNT = iterations -> 2 * (iterations + 1);

    private MergeStrategy strategy;
    private IntFunction<Integer> groupCountFunction;

    public Aggregation(MergeStrategy strategy, IntFunction<Integer> groupCountFunction) {
        this.strategy = strategy;
        this.groupCountFunction = groupCountFunction;
    }

    public List<? extends SpeedProfile> aggregateProfiles(List<? extends SpeedProfile> speedProfiles, int iterations) {
        List<SpeedProfile> aggregatedSpeedProfiles = new ArrayList<>();
        int speedProfilesAmount = speedProfiles.size();
        int groupCount = groupCountFunction.apply(iterations);
        for (int i = 0; i < speedProfilesAmount / groupCount; i++) {
            int startIndex = i * groupCount;
            int endIndex = (i + 1) * groupCount;
            aggregatedSpeedProfiles.addAll(getMergedGroup(speedProfiles.subList(startIndex, endIndex)));
        }
        int leftOver = speedProfilesAmount % groupCount;
        if (leftOver != 0) {
            int startIndex = speedProfilesAmount - leftOver - 1;
            aggregatedSpeedProfiles.addAll(getMergedGroup(speedProfiles.subList(startIndex, speedProfilesAmount)));
        }
        // as merge could lead to the same speed profile
        aggregatedSpeedProfiles = aggregatedSpeedProfiles.stream()
                .sorted(SPEED_PROFILE_COMPARATOR)
                .distinct()
                .collect(Collectors.toList());
        ++iterations;
        LOGGER.debug(String.format("Iteration = %d, groupCount = %d, source speedProfiles size = %d, target speedProfiles size = %d",
                iterations, groupCount, speedProfiles.size(), aggregatedSpeedProfiles.size()));
        return aggregatedSpeedProfiles.size() <= 200 || aggregatedSpeedProfiles.size() == speedProfilesAmount
                ? aggregatedSpeedProfiles
                : aggregateProfiles(aggregatedSpeedProfiles, iterations);
    }

    private List<? extends SpeedProfile> getMergedGroup(List<? extends SpeedProfile> subSpeedProfiles) {
        List<SpeedProfile> aggregatedSpeedProfiles = new ArrayList<>();
        Set<Integer> mergedIndexes = new HashSet<>(subSpeedProfiles.size());
        for (int i = 0; i < subSpeedProfiles.size(); i++) {
            if (mergedIndexes.contains(i)) {
                continue;
            }
            SpeedProfile speedProfile1 = subSpeedProfiles.get(i);
            for (int j = i + 1; j < subSpeedProfiles.size() && !mergedIndexes.contains(i); j++) {
                if (!mergedIndexes.contains(j)) {
                    SpeedProfile speedProfile2 = subSpeedProfiles.get(j);
                    Optional<? extends SpeedProfile> aggregatedSpeedProfile =
                            strategy.getAggregatedProfile(speedProfile1, speedProfile2);
                    if (aggregatedSpeedProfile.isPresent()) {
                        aggregatedSpeedProfiles.add(aggregatedSpeedProfile.get());
                        mergedIndexes.add(i);
                        mergedIndexes.add(j);
                    }
                }
            }
            if (!mergedIndexes.contains(i)) {
                aggregatedSpeedProfiles.add(speedProfile1);
            }
        }
        return aggregatedSpeedProfiles;
    }

    private List<? extends SpeedProfile> getTopUsedSpeedProfiles(List<? extends SpeedProfile> speedProfiles,
                                                                 Map<Integer, Integer> speedProfilesUsage) {
        return speedProfiles.stream()
                .map(s -> Pair.of(s, s.getAggregatedPatternIDs().stream()
                        .mapToInt(p -> speedProfilesUsage.getOrDefault(p, 0)).sum()))
                .sorted(Comparator.comparingInt(p -> -p.getRight()))
                .peek(p -> LOGGER.trace(String.format("Aggregated profile: patternId = %d, usages = %d", p.getLeft().getPatternId(), p.getRight())))
                .map(Pair::getLeft)
                .limit(200)
                .collect(Collectors.toList());
    }

    private List<? extends SpeedProfile> getTopUsedSpeedProfiles(List<? extends SpeedProfile> speedProfiles) {
        return speedProfiles.stream()
                .sorted(Comparator.comparingInt(p -> -p.getUsages()))
                .peek(p -> LOGGER.trace(String.format("Aggregated profile: patternId = %d, usages = %d", p.getPatternId(), p.getUsages())))
                .limit(200)
                .collect(Collectors.toList());
    }

    private void exportAggregatedSpeedProfiles(String market, List<? extends SpeedProfile> speedProfiles, int samplingId, String fileName) {
        List<SpeedProfilesCriteriaConsumer.SpeedProfileRow> speedProfileRows = new ArrayList<>();
        speedProfiles.stream()
                .peek(p ->
                {
                    if (speedProfiles.size() < 1000) {
                        LOGGER.trace(String.format("PatterId = %d aggregates: %s (%d)",
                                p.getPatternId(), p.getAggregatedPatternIDs(), p.getAggregatedPatternIDs().size()));
                    }
                })
                .map(SpeedProfile::toDbRows)
                .forEach(speedProfileRows::addAll);

        Path outputFile = DataProvider.getPath(fileName + samplingId, market, DataProvider.Extension.SQ3);
        Appender<SpeedProfilesCriteriaConsumer.SpeedProfileRow> appender = new SpeedProfileAppender(Appender.Mode.WRITE);
        DataProvider.exportToSq3(outputFile, appender, speedProfileRows);

        Appender<SpeedProfile> aggregationAppender = new AggregatedSpeedProfileAppender(Appender.Mode.APPEND);
        DataProvider.exportToSq3(outputFile, aggregationAppender, speedProfiles);
    }

    // ========================== Summary methods ==========================
    private void printSummary(String market, int samplingId, SpeedProfile profile)
    {
        String deviationQuery = "with t as " +
                "( " +
                "SELECT m.PATTERN_ID, m.SEQ_NUM, m.START_TIME, m.END_TIME, m.SPEED_KPH,  o.SPEED_KPH, (o.SPEED_KPH - m.SPEED_KPH) as diff " +
                "FROM main.NTTP_SPEED_PATTERN m " +
                "JOIN calibrated.NTTP_SPEED_PATTERN o on o.PATTERN_ID = m.PATTERN_ID and o.SEQ_NUM = m.SEQ_NUM " +
                "WHERE %s"+
                ") " +
                "SELECT min(diff) as min, max(diff) as max, avg(abs(diff)) as avg from t";

        String dayDeviationCondition = "o.SEQ_NUM BETWEEN %d and %d";
        String nightDeviationCondition = "o.SEQ_NUM NOT BETWEEN %d and %d";

        int startDaySeqNum = profile.getStartDayIndex() + 1;
        int endDaySeqNum = profile.getEndDayIndex() + 1;
        String dayDeviationQuery = String.format(deviationQuery, String.format(dayDeviationCondition, startDaySeqNum, endDaySeqNum));
        String nightDeviationQuery = String.format(deviationQuery, String.format(nightDeviationCondition, startDaySeqNum, endDaySeqNum));
        LOGGER.debug("dayDeviationQuery = " + dayDeviationQuery);
        LOGGER.debug("nightDeviationQuery = " + nightDeviationQuery);

        String usagesQuery = "SELECT sum(o.USAGES_COUNT) as original_usages, sum(a.TOTAL_USAGES) as merged_usages\n" +
                "FROM PROFILES_USAGE_BY_LINKS o\n" +
                "join NTTP_SPEED_PATTERN_AGGREGATION a on a.PATTERN_ID  = o.PATTERN_ID \n";
//                "GROUP by a.PATTERN_ID";

        Path originalProfilesDbPath = DataProvider.getPath("NTP_SPEED_PROFILES" , market, DataProvider.Extension.SQ3);
        Path calibratedProfilesPath = DataProvider.getPath("NTP_SPEED_PROFILES_ORIGINAL_s" + samplingId, market, DataProvider.Extension.SQ3);
        Path aggregatedProfilesPath = DataProvider.getPath("NTP_SPEED_PROFILES_AGGREGATED_s" + samplingId, market, DataProvider.Extension.SQ3);
        try (Connection connection = DataProvider.getConnection(aggregatedProfilesPath);
             Statement statement = connection.createStatement()) {
            statement.execute(String.format(DataProvider.ATTACH_QUERY, originalProfilesDbPath.toString(), "original"));
            statement.execute(String.format(DataProvider.ATTACH_QUERY, calibratedProfilesPath.toString(), "calibrated"));

            ResultSet deviation = connection.createStatement().executeQuery(dayDeviationQuery);
            while (deviation.next())
            {
                int min = deviation.getInt("min");
                int max = deviation.getInt("max");
                double avg = deviation.getDouble("avg");
                LOGGER.info(String.format("Day: [Min = %d, Max = %d, Avg = %f]", min, max, avg));
            }
            deviation.close();

            deviation = connection.createStatement().executeQuery(nightDeviationQuery);
            while (deviation.next())
            {
                int min = deviation.getInt("min");
                int max = deviation.getInt("max");
                double avg = deviation.getDouble("avg");
                LOGGER.info(String.format("Night: [Min = %d, Max = %d, Avg = %f]", min, max, avg));
            }
            deviation.close();

            ResultSet usages = connection.createStatement().executeQuery(usagesQuery);
            while (usages.next())
            {
                int originalUsages = usages.getInt("original_usages");
                int mergedUsages = usages.getInt("merged_usages");
                LOGGER.info(String.format("originalUsages = %d, mergedUsages = %d", originalUsages, mergedUsages));
            }
            usages.close();

            statement.execute(String.format(DataProvider.DETACH_QUERY, "original"));
            statement.execute(String.format(DataProvider.DETACH_QUERY, "calibrated"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ========================== Verification methods (better to be unit tests) ==========================
    private void checkUsages(List<? extends SpeedProfile> speedProfiles, Map<Integer, Integer> speedProfilesUsage)
    {
        AtomicInteger totalUsagesExpected = new AtomicInteger();
        AtomicInteger totalUsagesActual = new AtomicInteger();
        speedProfiles.forEach(profile ->
        {
            int patternId = profile.getPatternId();
            int expectedUsagesCount = profile.getAggregatedPatternIDs().stream()
                    .mapToInt(p -> speedProfilesUsage.getOrDefault(p, 0)).sum();
            int actualUsagesCount = profile.getUsages();
            totalUsagesExpected.addAndGet(expectedUsagesCount);
            totalUsagesActual.addAndGet(actualUsagesCount);
            if (expectedUsagesCount != actualUsagesCount)
            {
                LOGGER.trace(String.format("Profile:[patternId = %d] has invalid counted usages: expected = %d, actual = %d",
                        patternId, expectedUsagesCount, actualUsagesCount));
            }
        });
        LOGGER.warn(String.format("expected = %s, actual = %s", totalUsagesExpected, totalUsagesActual));
    }

    private void checkAggregation(List<? extends SpeedProfile> aggregatedProfiles, List<SpeedProfile> originalProfiles) {
        NavigableMap<Integer, Set<SpeedProfile>> profilesByAvgDaySpeed = new TreeMap<>();
        aggregatedProfiles.forEach(profile -> profilesByAvgDaySpeed.computeIfAbsent(profile.getAverageDaySpeed(),
                profiles -> new HashSet<>()).add(profile));
        Set<SpeedProfile> unmergeableProfiles = new TreeSet<>(Comparator.comparingInt(SpeedProfile::getAverageDaySpeed).thenComparing(SpeedProfile::getPatternId));
        // map original to aggregated ones
        originalProfiles.forEach(originalProfile ->
        {

            Optional<? extends SpeedProfile> mergedWith = Optional.empty();
//            for (Set<SpeedProfile> profiles : profilesByAvgDaySpeed.subMap(avgSpeed - threshold - 1, avgSpeed + threshold).values())
            for (Set<SpeedProfile> profiles : profilesByAvgDaySpeed.values())
            {
                /*if (originalProfile.getPatternId() == 15732)
                {
                    LOGGER.info("AvgDaySpeed = " + profiles.iterator().next().getAverageDaySpeed());
                    profiles.forEach(p -> LOGGER.info(p.getPatternId() + ": " + p.getSpeedPerTime()));
                }*/
                mergedWith = getMergeCandidate(profiles, originalProfile);
                if (mergedWith.isPresent())
                {
                    LOGGER.trace(String.format("Original profile %d is merged with aggregated profile %d",
                            originalProfile.getPatternId(), mergedWith.get().getPatternId()));
                    break;
                }
            }
            if (!mergedWith.isPresent())
            {
                unmergeableProfiles.add(originalProfile);

            }
            /*
            int avgSpeed = originalProfile.getAverageDaySpeed();
            int threshold = MergeStrategy.DEFAULT_THRESHOLD;
            if (profilesByAvgDaySpeed.subMap(avgSpeed - threshold - 1, avgSpeed + threshold).values().stream()
                    .noneMatch(profiles -> isAnyApplicable(profiles, profile).isPresent()))
            {
                unmergeableProfiles.add(profile);
            }*/
        });
        LOGGER.warn(String.format("Not merged profiles = %d, threshold = %d" , unmergeableProfiles.size(), strategy.getThreshold()));
        unmergeableProfiles.forEach(p -> LOGGER.trace(String.format("%s: with avgDaySpeed = %d were not merged with aggregated ones", p, p.getAverageDaySpeed())));
        collectDiffPerNotMergedProfile(unmergeableProfiles, profilesByAvgDaySpeed);
    }

    private Optional<? extends SpeedProfile> getMergeCandidate(Collection<? extends SpeedProfile> profiles, SpeedProfile originalProfile) {
        return profiles.stream()
                .map(p -> strategy.getAggregatedProfile(originalProfile, p))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    // profilesByAvgDaySpeed
    private void collectDiffPerNotMergedProfile(Set<SpeedProfile> unmergeableProfiles, NavigableMap<Integer, Set<SpeedProfile>> profilesByAvgDaySpeed)
    {
        int minAvgSpeed = profilesByAvgDaySpeed.firstKey();
        int maxAvgSpeed = profilesByAvgDaySpeed.lastKey();
        unmergeableProfiles.forEach(profile ->
        {
            List<Integer> speedsOriginal = profile.getSpeedPerTime();
            int avgDaySpeed = profile.getAverageDaySpeed();
            if (avgDaySpeed < minAvgSpeed)
            {
                avgDaySpeed = minAvgSpeed;
            }
            else if (avgDaySpeed > maxAvgSpeed)
            {
                avgDaySpeed = maxAvgSpeed;
            }
            else if (!profilesByAvgDaySpeed.containsKey(avgDaySpeed))
            {
                for (int i = 1; i < strategy.getThreshold(); i++)
                {
                    int negDeltaCandidates = profilesByAvgDaySpeed.getOrDefault(avgDaySpeed - i, Collections.emptySet()).size();
                    int posDeltaCandidates = profilesByAvgDaySpeed.getOrDefault(avgDaySpeed + i, Collections.emptySet()).size();
                    if (negDeltaCandidates > 0 || posDeltaCandidates > 0)
                    {
                        avgDaySpeed = negDeltaCandidates > posDeltaCandidates ? avgDaySpeed - i : avgDaySpeed + i;
                        break;
                    }
                }
            }
            IntSummaryStatistics statistics = new IntSummaryStatistics();
            profilesByAvgDaySpeed.get(avgDaySpeed).forEach(p ->
            {
                List<Integer> speedsToCompare = p.getSpeedPerTime();
                for(int i = 0; i < speedsOriginal.size(); i ++)
                {
                    int delta = speedsOriginal.get(0) - speedsToCompare.get(i);
                    statistics.accept(delta);
                }
            });
            LOGGER.trace(String.format("Profile [patternId = %d, avgDaySpeed = %d] has the following closest deltas %s by calibrated avgDaySpeed = %d",
                    profile.getPatternId(), profile.getAverageDaySpeed(), statistics, avgDaySpeed));
        });
    }

    public static void main(String[] args) {
        String market = "eu";
        Map<Integer, List<SpeedProfile>> speedProfilesPerSamplingId = DataProvider.extractSpeedProfiles(market);
        Map<Integer, Map<Integer, Integer>> speedProfilesUsagePerSampleId = DataProvider.getSpeedProfilesUsage(market);

        for (int i = 4; i <=4; i++)
        {
            int threshold = MergeStrategy.DEFAULT_THRESHOLD * i;
            speedProfilesPerSamplingId.forEach((samplingId, speedProfiles) ->
            {
                MergeStrategy strategy = new HalfAverageMergeStrategy();
//                MergeStrategy strategy = new HalfAverageRangeMergeStrategy();
                strategy.setThreshold(threshold);
                LOGGER.info(String.format("########################### SAMPLING_ID = %d, THRESHOLD = %d, Strategy = %s ###########################",
                        samplingId, threshold, strategy.getClass().getSimpleName()));
                Map<Integer, Integer> speedProfilesUsage = speedProfilesUsagePerSampleId.get(samplingId);
                Aggregation aggregation = new Aggregation(strategy, INCREMENT_GROUP_COUNT);
//              Aggregation aggregation = new Aggregation(strategy, CONSTANT_GROUP_COUNT);
                List<SpeedProfile> profilesToAggregate = speedProfiles.stream()
                        .filter(profile -> profile.getMinSpeed() != profile.getMaxSpeed())
                        .map(SpeedProfile::getCalibratedProfile)
                        .peek(p -> p.addUsages(speedProfilesUsage.getOrDefault(p.getPatternId(), 0)))
                        .sorted(SPEED_PROFILE_COMPARATOR)
                        .collect(Collectors.toList());

                Set<Pair<Integer, Integer>> dayRanges = profilesToAggregate.stream()
                        .map(p -> Pair.of(p.getStartDayIndex(), p.getEndDayIndex()))
                        .collect(Collectors.toSet());
                LOGGER.info("profilesToAggregate days range: " + dayRanges);

                List<? extends SpeedProfile> aggregatedSpeedProfiles = aggregation.aggregateProfiles(profilesToAggregate, 0);
                LOGGER.info(String.format("profilesToAggregate size = %d, aggregatedSpeedProfiles size = %d",
                        profilesToAggregate.size(), aggregatedSpeedProfiles.size()));

                dayRanges = aggregatedSpeedProfiles.stream()
                        .map(p -> Pair.of(p.getStartDayIndex(), p.getEndDayIndex()))
                        .collect(Collectors.toSet());
                LOGGER.info("aggregatedSpeedProfiles days range: " + dayRanges);

                // verification - all
                aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                strategy.setThreshold((int)Math.round(threshold * 1.5));
                aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                strategy.setThreshold(threshold * 2);
                aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                aggregation.checkUsages(aggregatedSpeedProfiles, speedProfilesUsage);
                // verification - top 200
                if (aggregatedSpeedProfiles.size() > 200)
                {
                    aggregatedSpeedProfiles = aggregation.getTopUsedSpeedProfiles(aggregatedSpeedProfiles);
                    strategy.setThreshold(threshold);
                    aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                    strategy.setThreshold((int)Math.round(threshold * 1.5));
                    aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                    strategy.setThreshold(threshold * 2);
                    aggregation.checkAggregation(aggregatedSpeedProfiles, profilesToAggregate);
                    aggregation.checkUsages(aggregatedSpeedProfiles, speedProfilesUsage);
                }
                // export
                aggregation.exportAggregatedSpeedProfiles(market, aggregatedSpeedProfiles, samplingId, "NTP_SPEED_PROFILES_AGGREGATED_s");
                aggregation.exportAggregatedSpeedProfiles(market, profilesToAggregate, samplingId, "NTP_SPEED_PROFILES_ORIGINAL_s");
//            DataProvider.exportSourceDataToCsv(DataProvider.AGGREGATED_PROFILES);
                // summary
                aggregation.printSummary(market, samplingId, profilesToAggregate.get(0));
            });
        }
    }
}
