package so.ifsc.Models;

import java.net.InetAddress;
import java.util.*;

public class Client {
    private final String id;
    private final Set<String> topics;

    public Client(InetAddress address) {
        this.id = address.getHostAddress();
        this.topics = new HashSet<>();
    }

    public String getId() {
        return id;
    }

    public Set<String> getTopics() {
        return topics;
    }
}
