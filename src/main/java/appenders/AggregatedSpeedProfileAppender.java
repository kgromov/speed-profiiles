package appenders;

import com.google.common.collect.ImmutableMap;
import speed_profiles.SpeedProfile;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class AggregatedSpeedProfileAppender implements Appender<SpeedProfile> {
    private static final String TABLE_NAME = "NTTP_SPEED_PATTERN_AGGREGATION";
    private static final String TEXT_TYPE = "TEXT";
    private static final String INTEGER_TYPE = "INTEGER";

    private Mode mode;

    public AggregatedSpeedProfileAppender(Mode mode) {
        this.mode = mode;
    }

    private static final Map<String, String> COLUMN_TYPE_BY_COLUMN_NAME = ImmutableMap.<String, String>builder()
            .put("PATTERN_ID", INTEGER_TYPE)
            .put("MERGED_PATTERN_IDS", TEXT_TYPE)
            .put("MERGED_PATTERN_IDS_COUNT", INTEGER_TYPE)
            .put("TOTAL_USAGES", INTEGER_TYPE)
            .build();

    @Override
    public void append(PreparedStatement statement, SpeedProfile object) {

    }

    @Override
    public void append(Connection connection, String query, SpeedProfile object) {

    }

    @Override
    public void append(Connection connection, Collection<? extends SpeedProfile> speedProfiles) throws SQLException {
        connection.createStatement().execute(getCreateQuery(TABLE_NAME, COLUMN_TYPE_BY_COLUMN_NAME));
        String query = getInsertQuery(TABLE_NAME, COLUMN_TYPE_BY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            connection.setAutoCommit(false);
            for (SpeedProfile profile : speedProfiles) {
                int columnIndex = 0;
                statement.setInt(++columnIndex, profile.getPatternId());
                statement.setString(++columnIndex, profile.getAggregatedPatternIDs().toString());
                statement.setInt(++columnIndex, profile.getAggregatedPatternIDs().size());
                statement.setInt(++columnIndex, profile.getUsages());
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    @Override
    public Mode getMode() {
        return mode;
    }
}
