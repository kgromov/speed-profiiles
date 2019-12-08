package appenders;

import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import checker.consumers.SpeedProfilesCriteriaConsumer.SpeedProfileRow;

public class SpeedProfileAppender implements Appender<SpeedProfileRow> {
    public static final String TABLE_NAME = "NTTP_SPEED_PATTERN";
    private static final String TEXT_TYPE = "TEXT";
    private static final String INTEGER_TYPE = "INTEGER";

    private Mode mode;

    public SpeedProfileAppender(Mode mode) {
        this.mode = mode;
    }

    private static final Map<String, String> COLUMN_TYPE_BY_COLUMN_NAME = ImmutableMap.<String, String>builder()
            .put("PATTERN_ID", INTEGER_TYPE)
            .put("SEQ_NUM", INTEGER_TYPE)
            .put("SAMPLING_ID", INTEGER_TYPE)
            .put("START_TIME", TEXT_TYPE)
            .put("END_TIME", TEXT_TYPE)
            .put("SPEED_KPH", INTEGER_TYPE)
            .build();

    @Override
    public void append(PreparedStatement statement, SpeedProfileRow object) {

    }

    @Override
    public void append(Connection connection, String query, SpeedProfileRow object) {

    }

    @Override
    public void append(Connection connection, Collection<? extends SpeedProfileRow> speedProfileRows) throws SQLException {
        connection.createStatement().execute(getCreateQuery(TABLE_NAME, COLUMN_TYPE_BY_COLUMN_NAME));
        String query = getInsertQuery(TABLE_NAME, COLUMN_TYPE_BY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            connection.setAutoCommit(false);
            for (SpeedProfileRow row : speedProfileRows) {
                int columnIndex = 0;
                statement.setInt(++columnIndex, row.getPatternId());
                statement.setInt(++columnIndex, row.getSeqNum());
                statement.setInt(++columnIndex, row.getSamplingId());
                statement.setString(++columnIndex, row.getStartTime());
                statement.setString(++columnIndex, row.getEndTime());
                statement.setInt(++columnIndex, row.getSpeed());
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    @Override
    public Mode getMode() {
        return  mode;
    }
}
