package so.ifsc.Managers;



import so.ifsc.Threads.ClientService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {
//    armazena os tópicos com um map: topico - lista de clients
    public static final Map<String, List<ClientService>> topics = new ConcurrentHashMap<>();
}
