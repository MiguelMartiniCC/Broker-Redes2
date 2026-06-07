package so.ifsc.Threads;

import com.google.gson.Gson;
import so.ifsc.Managers.TopicManager;
import so.ifsc.Models.Client;
import so.ifsc.Models.Message;


import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import so.ifsc.View.LogView;

public class ClientService {
    protected final Map<String, List<byte[]>> pendingMessages = new ConcurrentHashMap<>();

    protected final Socket socket;
    protected final Client client;
    protected final BlockingQueue<byte[]> filaTxMsg;
    protected OutputStream out;
    protected InputStream in;

    public ClientService(Socket socket) throws IOException {
        this.socket = socket;
        this.client = new Client(socket.getInetAddress());
        this.filaTxMsg = new LinkedBlockingQueue<>();
        this.out = socket.getOutputStream();
        this.in = socket.getInputStream();
    }

    public void init() {
//        abre duas threads, uma para enviar e outra p/receber mensagens
        new Thread(new Rx(in, this)).start();
        new Thread(new Tx(out, filaTxMsg, this)).start();

        TopicManager.connectClient(client.getId(), this);
    }

    public void processMessage(Message msg) {
        switch (msg.type.toUpperCase()) {
            //todos os tópicos
            case "LIST_ALL_TOPICS" -> {
                // Puxa direto do novo TopicManager
                List<String> allTopics = new ArrayList<>(TopicManager.getAllTopics());

                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(allTopics);
                sendMessage(new Gson().toJson(response).getBytes(StandardCharsets.UTF_8));
            }

            //tópicos do cliente
            case "LIST_MY_TOPICS" -> {
                List<String> myTopics = new ArrayList<>(client.getTopics());

                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(myTopics);
                sendMessage(new Gson().toJson(response).getBytes(StandardCharsets.UTF_8));
            }

            //Subs em topiocs
            case "SUBSCRIBE" -> {
                if (!client.getTopics().contains(msg.topic)) {
                    // Altera no TopicManager global e na lista local do cliente
                    TopicManager.subscribe(msg.topic, client.getId());
                    client.getTopics().add(msg.topic);
                }
            }
            //Pub nos topicos
            case "PUBLISH" -> {
                Message response = new Message();
                response.type = "MESSAGE";
                response.topic = msg.topic;
                response.payload = msg.payload;
                response.client = socket.getInetAddress().getHostAddress();
                response.date = msg.date;
                response.time = msg.time;

                // Delega totalmente o envio e o buffer para o gerenciador
                TopicManager.broadcast(msg.topic, response);
            }
            default -> LogView.log("Comando desconhecido: " + msg.type);
        }
    }

    protected void close(){
        try {
            // Avisa o broker que este cliente não está mais online
            TopicManager.disconnectClient(client.getId());
            socket.close();
        } catch (IOException e) {
           LogView.log("Erro ao fechar o socket: " + e.getMessage());
        }
        LogView.log("Client " + client.getId() + " desconectado");
    }


//utils
    public void sendMessage(byte[] msg) {
        filaTxMsg.add(msg);
    }

    private boolean isConnected(){
        return !socket.isClosed() && socket.isConnected();
    }
}
