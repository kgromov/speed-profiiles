package speed_profiles;

import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

@ToString(callSuper = true, exclude = {"aggregatedPatternIDs"})
public class AggregatedSpeedProfile extends SpeedProfile {
    private Set<Integer> aggregatedPatternIDs = new TreeSet<>();
    private List<Pair<Integer, Integer>> aggregatedSpeedRanges;

    public AggregatedSpeedProfile(int patternId, int samplingId) {
        super(patternId, samplingId);
        this.aggregatedSpeedRanges = new ArrayList<>(getPeriods());
    }

    public AggregatedSpeedProfile(int patternId, int samplingId, List<Integer> speedPerTime) {
        super(patternId, samplingId, speedPerTime);
        this.aggregatedSpeedRanges = new ArrayList<>(speedPerTime.size());
    }

    public AggregatedSpeedProfile addPatternId(int patternId)
    {
        aggregatedPatternIDs.add(patternId);
        return this;
    }

    public AggregatedSpeedProfile addPatternId(Collection<Integer> patternIDs)
    {
        aggregatedPatternIDs.addAll(patternIDs);
        return this;
    }

    public void addRange(Pair<Integer, Integer> range)
    {
        aggregatedSpeedRanges.add(range);
    }

    @Override
    public int getMinAggregatedSpeedAt(int index) {
        return aggregatedSpeedRanges.get(index).getLeft();
    }

    @Override
    public int getMaxAggregatedSpeedAt(int index) {
        return aggregatedSpeedRanges.get(index).getRight();
    }

    @Override
    public Set<Integer> getAggregatedPatternIDs() {
        return aggregatedPatternIDs;
    }
}
