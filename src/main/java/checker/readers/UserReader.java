package checker.readers;

import java.util.Set;

public interface UserReader {
    String CDC_USER_PATTERN = "CDCA_%s_%s";

    static String convertRegionToDbUser(String region, String dvn)
    {
        return String.format(CDC_USER_PATTERN, region, dvn);
    }

    static String convertDbUserToRegion(String dbUser, String dvn)
    {
        return dbUser.replaceFirst("CDCA_", "").replaceFirst("_" + dvn, "");
    }

    Set<String> getCdcUsers();
}
