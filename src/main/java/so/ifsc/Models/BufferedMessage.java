package so.ifsc.Models;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BufferedMessage {
    private final Message message;
    // Armazena os IDs dos clientes que ainda NÃO baixaram a mensagem
    private final Set<String> pendingClients;

    public BufferedMessage(Message message, Set<String> subscribers) {
        this.message = message;
        // Criamos uma cópia concorrente dos IDs dos inscritos no momento do envio
        this.pendingClients = ConcurrentHashMap.newKeySet();
        this.pendingClients.addAll(subscribers);
    }

    public Message getMessage() { return message; }
    public Set<String> getPendingClients() { return pendingClients; }

    // Remove o cliente da lista de pendências. Retorna true se não houver mais ninguém pendente.
    public boolean acknowledge(String clientId) {
        pendingClients.remove(clientId);
        return pendingClients.isEmpty();
    }
}
