package appenders;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SpeedProfiesUsageAppender implements Appender<Map<Pair<Integer, Integer>, AtomicLong>> {
    @Override
    public void append(PreparedStatement statement, Map<Pair<Integer, Integer>, AtomicLong> object) {

    }

    @Override
    public void append(Connection connection, String query, Map<Pair<Integer, Integer>, AtomicLong> object) {

    }

    @Override
    public void append(Connection connection, Collection<? extends Map<Pair<Integer, Integer>, AtomicLong>> objects) throws SQLException {

    }

    @Override
    public Mode getMode() {
        return null;
    }
}
