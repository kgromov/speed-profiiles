package speed_profiles.merge;


import speed_profiles.AggregatedSpeedProfile;
import speed_profiles.SpeedProfile;

import java.util.List;
import java.util.Optional;

public class HalfAverageMergeStrategy implements MergeStrategy {
    private int threshold = MergeStrategy.DEFAULT_THRESHOLD;

    @Override
    public Optional<? extends SpeedProfile> getAggregatedProfile(SpeedProfile profile1, SpeedProfile profile2)
    {
        if (!profile1.isMergeable(profile2, threshold))
        {
            return Optional.empty();
        }
        List<Integer> speedPerTime1 = profile1.getSpeedPerTime();
        List<Integer> speedPerTime2 = profile2.getSpeedPerTime();

        int aggregatedPatternId = profile1.getUsages() >=profile2.getUsages() ? profile1.getPatternId() : profile2.getPatternId();
        AggregatedSpeedProfile aggregateProfile = new AggregatedSpeedProfile(aggregatedPatternId, profile1.getSamplingId());
        for (int i = 0 ; i < speedPerTime1.size(); i++)
        {
            int speed1 = speedPerTime1.get(i);
            int speed2 = speedPerTime2.get(i);
//            int threshold = profile1.isDayTime(i) ? DEFAULT_THRESHOLD : DEFAULT_THRESHOLD * 2;
            if (profile1.isDayTime(i) && Math.abs(speed1 - speed2) >  threshold)
//            if ( Math.abs(speed1 - speed2) >  threshold)
            {
                return Optional.empty();
            }
//            int aggregatedSpeed = Math.round((speed1 + speed2) / DIV);
//            int aggregatedSpeed = MergeStrategy.roundToThreshold(speed1, speed2);
            int aggregatedSpeed = speed1 == speed2 ? speed1 : getAverageSpeedByUsages(profile1, profile2, i);
            aggregateProfile.addSpeed(aggregatedSpeed);
        }
        aggregateProfile.addPatternId(profile1.getAggregatedPatternIDs())
                .addPatternId(profile2.getAggregatedPatternIDs())
                .addUsages(profile1.getUsages() + profile2.getUsages());
        return Optional.of(aggregateProfile);
    }

    @Override
    public void setThreshold(int value) {
        this.threshold = value;
    }

    @Override
    public int getThreshold() {
        return threshold;
    }

    private int getAverageSpeedByUsages(SpeedProfile profile1, SpeedProfile profile2, int timeIndex)
    {
        int usages1 = profile1.getUsages();
        int usages2 = profile2.getUsages();
        double sumUsages = (double) usages1 + usages2;
        double weight1 = usages1 / sumUsages;
        double weight2 = usages2 / sumUsages;
        return (int) Math.round(weight1 * profile1.getSpeedPerTime().get(timeIndex) + weight2 * profile2.getSpeedPerTime().get(timeIndex));
    }
}
