package checker.criterias;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ICriteria {

    default String getIdentity(ResultSet resultSet) throws SQLException
    {
        return resultSet.getObject(1).toString();
    }

    default String getQuery(String schema)
    {
        String suffix = schema + ".";
        return getQuery().replaceAll("\\W*(from)\\W*", " from " + suffix)
                .replaceAll("\\W*(join)\\W*", " join " + suffix);
    }

    static String getQuery(String query, String schema)
    {
        ICriteria criteria = () -> query;
        return criteria.getQuery(schema);
    }

    String getQuery();
}
