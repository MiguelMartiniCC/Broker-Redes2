package so.ifsc.Managers;



import com.google.gson.Gson;
import so.ifsc.Models.BufferedMessage;
import so.ifsc.Models.Message;
import so.ifsc.Threads.ClientService;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TopicManager {
    // Tópico -> Set de IDs de Clientes inscritos (mesmo offline)
    private static final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();

    // Tópico -> Lista de mensagens em buffer
    private static final Map<String, List<BufferedMessage>> messageBuffers = new ConcurrentHashMap<>();

    // ID do Cliente -> Conexão ativa (ClientService)
    private static final Map<String, ClientService> activeConnections = new ConcurrentHashMap<>();

    // Retorna todos os tópicos existentes para o comando LIST_ALL_TOPICS
    public static Set<String> getAllTopics() {
        return subscriptions.keySet();
    }

    // Registra a inscrição do cliente no tópico
    public static void subscribe(String topic, String clientId) {
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(clientId);
    }
//resgata todos opcios do cliente
    public static Set<String> getTopicsFromClient(String clientId) {
        Set<String> topics = new HashSet<>();

        for (Map.Entry<String, Set<String>> entry : subscriptions.entrySet()) {
            if (entry.getValue().contains(clientId)) {
                topics.add(entry.getKey());
            }
        }

        return topics;
    }


    // Cliente sai de um topico
    public static void unsubscribe(String topic, String clientId) {
        Set<String> subscribers = subscriptions.get(topic);

        if (subscribers != null) {
            subscribers.remove(clientId);

            if (subscribers.isEmpty()) {
                subscriptions.remove(topic);
            }
        }

        List<BufferedMessage> buffer = messageBuffers.get(topic);

        if (buffer != null) {
            for (BufferedMessage msg : buffer) {
                msg.getPendingClients().remove(clientId);
            }
        }
    }

    // Envia a mensagem para os inscritos ou armazena no buffer
    public static void broadcast(String topic, Message msg) {
        Set<String> subscribers = subscriptions.getOrDefault(topic, ConcurrentHashMap.newKeySet());

        if (subscribers.isEmpty()) return;

        BufferedMessage bufferedMsg = new BufferedMessage(msg, subscribers);
        messageBuffers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(bufferedMsg);

        byte[] data = new Gson().toJson(msg).getBytes(StandardCharsets.UTF_8);

        for (String clientId : subscribers) {
            ClientService activeClient = activeConnections.get(clientId);
            if (activeClient != null) {
                activeClient.sendMessage(data); // Agora mapeado corretamente!
                acknowledgeMessage(topic, bufferedMsg, clientId);
            }
        }
    }

    private static void acknowledgeMessage(String topic, BufferedMessage bufferedMsg, String clientId) {
        if (bufferedMsg.acknowledge(clientId)) {
            List<BufferedMessage> buffer = messageBuffers.get(topic);
            if (buffer != null) {
                buffer.remove(bufferedMsg);
            }
        }
    }

    // Quando o cliente conecta, ele entra no mapa de ativos e puxa o que está pendente
    public static void connectClient(String clientId, ClientService service) {
        activeConnections.put(clientId, service);

        // Varre os buffers enviando o que for dele
//        for (Map.Entry<String, List<BufferedMessage>> entry : messageBuffers.entrySet()) {
//            String topic = entry.getKey();
//            List<BufferedMessage> buffer = entry.getValue();
//
//            for (BufferedMessage bMsg : buffer) {
//                if (bMsg.getPendingClients().contains(clientId)) {
//                    byte[] data = new Gson().toJson(bMsg.getMessage()).getBytes(StandardCharsets.UTF_8);
//                    service.sendMessage(data);
//                    acknowledgeMessage(topic, bMsg, clientId);
//                }
//            }
//        }
    }
    public static void sendBufferToClient(String clientId, ClientService service) {
        for (Map.Entry<String, List<BufferedMessage>> entry : messageBuffers.entrySet()) {
            String topic = entry.getKey();
            List<BufferedMessage> buffer = entry.getValue();

            for (BufferedMessage bMsg : buffer) {
                if (bMsg.getPendingClients().contains(clientId)) {
                    byte[] data = new Gson().toJson(bMsg.getMessage()).getBytes(StandardCharsets.UTF_8);
                    service.sendMessage(data);
                    acknowledgeMessage(topic, bMsg, clientId);
                }
            }
        }
    }

    public static void disconnectClient(String clientId) {
        activeConnections.remove(clientId);
    }
}
