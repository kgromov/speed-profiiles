package checker.readers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TxtUsersReader implements UserReader{
    private static final String RESOURCES_FILE = "C:\\Projects\\java-examples\\Database\\src\\test\\java\\oracle\\checker\\resources\\%s_users.txt";
    private static final Set<String> SAMPLE_USERS = readRegions("sample");

    private String market;
    private String dvn;
    private Set<String> cdcUsers;

    public TxtUsersReader(String market, String dvn) {
        this.market = market;
        this.dvn = dvn;
        this.cdcUsers = readRegions(market).stream()
                .map(region -> String.format(CDC_USER_PATTERN, region, dvn))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getCdcUsers() {
            return cdcUsers;
    }

    public static String getSampleRegions() {
        return SAMPLE_USERS.stream().collect(Collectors.joining(")|(", "'(", "')"));
    }

    public static Set<String> getSampleCdcUserWithDVN(String customDVN) {
        return SAMPLE_USERS.stream()
                .map(region -> String.format(CDC_USER_PATTERN, region, customDVN))
                .collect(Collectors.toSet());
    }

    public Set<String> withoutSampleUsers(Set<String> cdcUsers) {
        Set<String> pureUsers = new HashSet<>(cdcUsers);
        pureUsers.removeAll(getSampleCdcUserWithDVN(dvn));
        return pureUsers;
    }

    private static Set<String> readRegions(String market) {
        try {
            return Files.readAllLines(Paths.get(String.format(RESOURCES_FILE, market))).stream()
                    .filter(line -> !line.startsWith("//"))
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
