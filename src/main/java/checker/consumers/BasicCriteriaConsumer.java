package checker.consumers;


import checker.criterias.ICriteria;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BasicCriteriaConsumer implements ICriteriaConsumer {
    private ICriteria[] criterias;

    public BasicCriteriaConsumer(ICriteria... criterias) {
        this.criterias = criterias;
    }

    @Override
    public void processDbUser(String dbUser, String dbServerURL) {

    }

    @Override
    public void processDbUser(Connection connection, String dbUser, String dbServerURL) {
        for (ICriteria criteria : criterias)
        {
            try (ResultSet resultSet = connection.createStatement().executeQuery(ICriteria.getQuery(criteria.getQuery(), dbUser))) {
                if (resultSet.next()) {
                    LOGGER.info(String.format("SourceDbUser = %s, dbServer = %s", dbUser, dbServerURL));
                    LOGGER.info( criteria.getIdentity(resultSet));
                }
            } catch (SQLException e) {
                LOGGER.error(String.format("Unable to process dbUser = %s, dbServerURL = %s. Cause:%n%s", dbUser, dbServerURL, e));
            }
        }
    }
}
