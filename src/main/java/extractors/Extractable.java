package extractors;

import java.sql.ResultSet;

public interface Extractable<T> {

    T extract(ResultSet resultSet);
}
