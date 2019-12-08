package speed_profiles.merge;

import speed_profiles.SpeedProfile;
import java.util.Optional;

public interface MergeStrategy {
    //    int DEFAULT_THRESHOLD = 500;
    int DEFAULT_THRESHOLD = 5;  // 19 - 20 gives ~200 for 2 hours; 24 - 25 gives ~200 for 1 hour;
    float DIV = 2;

    Optional<? extends SpeedProfile> getAggregatedProfile(SpeedProfile profile1, SpeedProfile profile2);

    static int roundToThreshold(int value1, int value2) {
        int div = Math.max(Math.round(value1 / DEFAULT_THRESHOLD), value2 / DEFAULT_THRESHOLD);
        return div * DEFAULT_THRESHOLD;
    }

    void setThreshold(int value);

    int getThreshold();
}
