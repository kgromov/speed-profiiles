package speed_profiles;

import checker.consumers.SpeedProfilesCriteriaConsumer.SpeedProfileRow;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@EqualsAndHashCode(exclude = {"statistics", "nightTimeStatistics", "dayTimeStatistics", "usages"})
@ToString(exclude = {"statistics", "nightTimeStatistics", "dayTimeStatistics"})
public class SpeedProfile {
    private static final int MINUTE_IN_DAY = 24 * 60;
    private static final Map<Integer, Integer> SAMPLE_IDS_TO_PERIODS = ImmutableMap.of(1, 15, 2, 60, 4, 120);
    private static final Map<Pair<Integer, Integer>, Integer> DEPTH_TO_CALIBRATED_SAMPLE_ID = ImmutableMap.of(
            Pair.of(1 ,4), 2,
            Pair.of(1, 8), 4,
            Pair.of(2, 2), 4
    );

    private int patternId;
    private final int samplingId;
    private final List<Integer> speedPerTime;
    private IntSummaryStatistics statistics;
    private IntSummaryStatistics nightTimeStatistics;
    private IntSummaryStatistics dayTimeStatistics;
    private int usages;

    public SpeedProfile(int patternId, int samplingId) {
        this.patternId = patternId;
        this.samplingId = samplingId;
        // project specific
        int periods = getPeriods();
        this.speedPerTime = new ArrayList<>(periods);
        this.statistics = new IntSummaryStatistics();
        this.dayTimeStatistics = new IntSummaryStatistics();
        this.nightTimeStatistics = new IntSummaryStatistics();
    }

    public SpeedProfile(int patternId, int samplingId, List<Integer> speedPerTime) {
        this.patternId = patternId;
        this.samplingId = samplingId;
        this.speedPerTime = speedPerTime;
//        this.statistics = speedPerTime.stream().mapToInt(i -> i).summaryStatistics();
       collectSpeedStatistics(speedPerTime);
    }

    private void collectSpeedStatistics(List<Integer> speedPerTime)
    {
        int startDayIndex = getStartDayIndex();
        int endDayIndex = getEndDayIndex();
        this.statistics = new IntSummaryStatistics();
        this.dayTimeStatistics = new IntSummaryStatistics();
        this.nightTimeStatistics = new IntSummaryStatistics();

        for (int i = 0; i < speedPerTime.size(); i++)
        {
            int speed = speedPerTime.get(i);
            if (i < startDayIndex || i > endDayIndex)
            {
                nightTimeStatistics.accept(speed);
            } else
            {
                dayTimeStatistics.accept(speed);
            }
            statistics.accept(speed);
        }
    }

    public void addSpeed(int speed) {
        if (speedPerTime.size() < getStartDayIndex() || speedPerTime.size() > getEndDayIndex()) {
            nightTimeStatistics.accept(speed);
        } else {
            dayTimeStatistics.accept(speed);
        }
        speedPerTime.add(speed);
        statistics.accept(speed);
    }

    public SpeedProfile addUsages(int usages)
    {
        this.usages +=usages;
        return this;
    }

    public int getUsages() {
        return usages;
    }

    public int getMinSpeed() {
        return statistics.getMin();
    }

    public int getMaxSpeed() {
        return statistics.getMax();
    }

    public int getAverageSpeed() {
        return (int) statistics.getAverage();
    }

    public int getAverageNightSpeed() {
        return (int) nightTimeStatistics.getAverage();
    }

    public int getAverageDaySpeed() {
        return (int) dayTimeStatistics.getAverage();
    }

    public int getMinAggregatedSpeedAt(int index)
    {
        return speedPerTime.get(index);
    }

    public int getMaxAggregatedSpeedAt(int index)
    {
        return getMinAggregatedSpeedAt(index);
    }

    public int getPatternId() {
        return patternId;
    }

    // usage: only for aggregated profiles
    public void setPatternId(int patternId) {
        this.patternId = patternId;
    }

    public int getSamplingId() {
        return samplingId;
    }

    public List<Integer> getSpeedPerTime() {
        return speedPerTime;
    }

    protected int getPeriods()
    {
        return MINUTE_IN_DAY / SAMPLE_IDS_TO_PERIODS.get(samplingId);
    }

    public int getStartDayIndex()
    {
        int periods =  getPeriods();
        return (int) (periods / 24.0 * 2);
    }

    public int getEndDayIndex()
    {
        int periods = getPeriods();
        return (int) (periods / 24.0 * 21);
    }

    public boolean isDayTime(int timeIndex)
    {
        return timeIndex >= getStartDayIndex() && timeIndex <= getEndDayIndex();
    }

    public boolean isMergeable(SpeedProfile otherProfile, int threshold) {
        return this.samplingId == otherProfile.samplingId
//                && Math.abs(this.getAverageSpeed() - otherProfile.getAverageSpeed()) <= threshold
                && Math.abs(this.getAverageDaySpeed() - otherProfile.getAverageDaySpeed()) <= threshold
                /*&& Math.abs(this.getFirstSpeed() - otherProfile.getFirstSpeed()) <= threshold * 2
                && Math.abs(this.getLastSpeed() - otherProfile.getLastSpeed()) <= threshold * 2*/;
    }

    public Set<Integer> getAggregatedPatternIDs() {
        return Collections.singleton(patternId);
    }

    public SpeedProfile getCalibratedProfile() {
        int depth = Aggregation.AGGREGATION_DEPTH.getOrDefault(getSamplingId(), 1);
        if (depth == 1) {
            return this;
        }
        int periods = speedPerTime.size() / depth;
        List<Integer> speeds = new ArrayList<>(periods);
        for (int i = 0; i < periods; i++) {
            int finalI = i;
            int resSpeed = (int) Math.round(IntStream.range(0, depth).boxed()
                    .mapToInt(k -> speedPerTime.get(finalI * depth + k))
                    .summaryStatistics().getAverage());
            speeds.add(resSpeed);
        }
//        return new SpeedProfile(patternId, samplingId * depth, speeds);
//        return new SpeedProfile(patternId, 2 * samplingId, speeds);
        return new SpeedProfile(patternId, DEPTH_TO_CALIBRATED_SAMPLE_ID.get(Pair.of(samplingId, depth)), speeds);
    }


 /*   public SpeedProfile getCalibratedProfile(int depth) {
        int periods = speedPerTime.size() / depth;
        List<Integer> speeds = new ArrayList<>(periods);
        for (int i = 0; i < periods; i++) {
            int finalI = i;
            int resSpeed = (int) Math.round(IntStream.range(0, depth).boxed()
                    .mapToInt(k -> speedPerTime.get(finalI * depth + k))
                    .summaryStatistics().getAverage());
            speeds.add(resSpeed);
        }
        return new SpeedProfile(patternId, 2 * samplingId, speeds);
    }*/

    public List<SpeedProfileRow> toDbRows() {
        int seqNums = speedPerTime.size();
        long deltaInMinutes = MINUTE_IN_DAY / seqNums;
        List<LocalTime> timePeriods = new ArrayList<>(seqNums);
        LocalTime midnight = LocalTime.MIDNIGHT;
        timePeriods.add(midnight);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
        for (int i = 1; i < seqNums; i++) {
            LocalTime startTime = i == 1 ? midnight : timePeriods.get(i - 1);
            timePeriods.add(startTime.plusMinutes(deltaInMinutes));
        }
        return IntStream.range(0, seqNums).boxed()
                .map(i -> new SpeedProfileRow(
                        patternId,
                        i + 1,
                        samplingId,
                        timePeriods.get(i).format(formatter),
                        timePeriods.get((i + 1) % seqNums).format(formatter),
                        speedPerTime.get(i)
                ))
                .collect(Collectors.toList());
    }



}
