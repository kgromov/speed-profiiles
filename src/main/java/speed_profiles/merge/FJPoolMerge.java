package speed_profiles.merge;


import speed_profiles.SpeedProfile;

import java.util.Optional;
import java.util.concurrent.RecursiveTask;

public class FJPoolMerge extends RecursiveTask<SpeedProfile> implements MergeStrategy {
    @Override
    protected SpeedProfile compute() {
        return null;
    }

    @Override
    public Optional<? extends SpeedProfile> getAggregatedProfile(SpeedProfile profile1, SpeedProfile profile2) {
        return Optional.empty();
    }

    @Override
    public void setThreshold(int value) {

    }

    @Override
    public int getThreshold() {
        return DEFAULT_THRESHOLD;
    }
}
