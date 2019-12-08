package checker.readers;

import java.util.Set;

// TODO: implement Akela_splitting parsing
public class XslUserReader implements UserReader {

    private String market;
    private String dvn;
    private Set<String> cdcUsers;

    public XslUserReader(String market, String dvn) {
        this.market = market;
        this.dvn = dvn;
    }

    @Override
    public Set<String> getCdcUsers() {
        return cdcUsers;
    }
}
