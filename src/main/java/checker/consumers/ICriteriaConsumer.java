package checker.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public interface ICriteriaConsumer {
    Logger LOGGER = LoggerFactory.getLogger(ICriteriaConsumer.class.getName());
    int DEFAULT_FETCH_SIZE = 1000;

    @Deprecated
    void processDbUser(String dbUser, String dbServerURL);

    void processDbUser(Connection connection, String dbUser, String dbServerURL);
}
