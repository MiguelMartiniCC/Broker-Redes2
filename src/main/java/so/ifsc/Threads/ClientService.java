package so.ifsc.Threads;

import com.google.gson.Gson;
import so.ifsc.Managers.TopicManager;
import so.ifsc.Models.Client;
import so.ifsc.Models.Message;


import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

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

        List<byte[]> pending = pendingMessages.remove(client.getId());
        if (pending != null) {
            for (byte[] data : pending) {
                sendMessage(data);
            }
        }
    }

    public void processMessage(Message msg) {
        switch (msg.type.toUpperCase()) {
            //todos os tópicos
            case "LIST_ALL_TOPICS" -> {
                List<String> allTopics = new ArrayList<>(TopicManager.topics.keySet());

                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(allTopics);
                sendMessage(new Gson().toJson(response).getBytes());
                System.out.println("Tópicos: " + allTopics);
            }

            //tópicos do cliente
            case "LIST_MY_TOPICS" -> {
                List<String> myTopics = new ArrayList<>(client.getTopics());

                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(myTopics);
                sendMessage(new Gson().toJson(response).getBytes());
                System.out.println("Tópicos inscritos: " + myTopics);
            }

            //Subs em topiocs
            case "SUBSCRIBE" -> {

                if (!client.getTopics().contains(msg.topic)) {
                    TopicManager.topics.computeIfAbsent(msg.topic, k -> new CopyOnWriteArrayList<>()).add(this); //cria topcio se n existir
                    client.getTopics().add(msg.topic); //adiciona nos topicos do client
                }
            }
            //Pub nos topicos
            case "PUBLISH" -> {
                //mensaggem para o topico
                Message response = new Message();
                response.type = "MESSAGE";
                response.topic = msg.topic;
                response.payload = msg.payload;
                response.client = socket.getInetAddress().getHostAddress();
                response.date = msg.date;
                response.time = msg.time;

                String json = new Gson().toJson(response);
                byte[] data = json.getBytes();

                //manda a mensagem para cada um dos subs do topico
                List<ClientService> subscribers = TopicManager.topics.get(msg.topic);
                if (subscribers != null) {
                    for (ClientService subscriber : subscribers) {
                        if (subscriber.isConnected()) {
                            subscriber.sendMessage(data);
                        } else {
                            pendingMessages
                                    .computeIfAbsent(subscriber.client.getId(), k -> new ArrayList<>())
                                    .add(data);
                        }
                    }
                }
            }
            default -> System.out.println("comando desconhecido: " + msg.type);
        }
    }

    protected void close(){
        try {
            socket.close();
        } catch (IOException e) {
            System.out.println("Erro ao fechar o socket: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Client " + client.getId() + " desconectado");
    }


//utils
    public void sendMessage(byte[] msg) {
        filaTxMsg.add(msg);
    }

    private boolean isConnected(){
        return !socket.isClosed() && socket.isConnected();
    }
}
