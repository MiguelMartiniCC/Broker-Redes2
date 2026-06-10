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
import java.util.concurrent.LinkedBlockingQueue;
import so.ifsc.View.LogView;

public class ClientService {
    protected final Map<String, List<byte[]>> pendingMessages = new ConcurrentHashMap<>();

    protected final Socket socket;
    protected final Client client;
    protected final BlockingQueue<byte[]> filaTxMsg;
    protected OutputStream out;
    protected InputStream in;

    // --- VARIÁVEIS DE CONTROLE DA AUTENTICAÇÃO MANUAL ---
    private String nonceEnviado;
    private boolean autenticado = false;

    public ClientService(Socket socket) throws IOException {
        this.socket = socket;
        this.client = new Client();
        this.filaTxMsg = new LinkedBlockingQueue<>();
        this.out = socket.getOutputStream();
        this.in = socket.getInputStream();
    }

    public void init() {
        // Removido o bloco antigo de SSLSocket handshake!
        // Abre diretamente as threads de processamento criadas por você
        new Thread(new Rx(in, this)).start();
        new Thread(new Tx(out, filaTxMsg, this)).start();
    }

    public void processMessage(Message msg) {
        String comando = msg.type.toUpperCase();

        // GUARDA DE SEGURANÇA: Bloqueia qualquer comando se o cliente não passou na autenticação manual
        if (!autenticado && !comando.equals("CONNECT") && !comando.equals("AUTH_RESPONSE")) {
            LogView.log("Acesso negado: Cliente tentou enviar comandos sem autenticação.");
            close();
            return;
        }

        switch (comando) {
            case "CONNECT" -> {
                client.setClientId(msg.clientId);

                // 1. Gera um número aleatório único (O Desafio)
                this.nonceEnviado = String.valueOf(new java.util.Random().nextInt(1000000));

                // 2. Cria a mensagem de desafio
                Message desafio = new Message();
                desafio.type = "CHALLENGE";
                desafio.payload = this.nonceEnviado;

                // Envia o desafio para o cliente decifrar
                sendMessage(new Gson().toJson(desafio).getBytes(StandardCharsets.UTF_8));
                LogView.log("Desafio [" + this.nonceEnviado + "] enviado para o cliente: " + client.getClientId());
            }

            case "AUTH_RESPONSE" -> {
                String certificadoHexDoCliente = msg.payload; // O cliente vai enviar o conteúdo do .crt em Hex ou Base64

                try {
                    // 1. Converte a String Hex recebida de volta para bytes
                    byte[] certBytes = hexStringToByteArray(certificadoHexDoCliente);

                    // 2. Transforma os bytes no objeto de Certificado X509 do Java
                    java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
                    java.security.cert.X509Certificate certCliente =
                            (java.security.cert.X509Certificate) cf.generateCertificate(new java.io.ByteArrayInputStream(certBytes));

                    // 3. Carrega a Chave Pública do próprio Servidor (da CA que está nos resources)
                    java.io.InputStream caStream = getClass().getResourceAsStream("/servidor_ca.crt");
                    if (caStream == null) {
                        throw new java.io.FileNotFoundException("Certificado master 'servidor_ca.crt' não encontrado em resources!");
                    }
                    java.security.cert.X509Certificate certServidorCA =
                            (java.security.cert.X509Certificate) cf.generateCertificate(caStream);
                    caStream.close();

                    // 4. A VERIFICAÇÃO EXIGIDA: Valida se este certificado foi assinado pela chave pública do servidor
                    // Se o certificado NÃO foi assinado pelo servidor, este método joga uma Exception na hora!
                    certCliente.verify(certServidorCA.getPublicKey());

                    // 5. Opcional: Verifica se o CN do certificado bate com o clientId digitado
                    String principalName = certCliente.getSubjectX500Principal().getName();
                    if (!principalName.contains("CN=" + client.getClientId())) {
                        throw new java.security.GeneralSecurityException("O nome no certificado não corresponde ao ID do cliente.");
                    }

                    // SE CHEGOU AQUI, PASSOU NA VERIFICAÇÃO CRIPTOGRÁFICA DE ASSINATURA!
                    this.autenticado = true;
                    TopicManager.connectClient(client.getClientId(), this);

                    Message respostaSucesso = new Message();
                    respostaSucesso.type = "AUTH_SUCCESS";
                    sendMessage(new Gson().toJson(respostaSucesso).getBytes(StandardCharsets.UTF_8));

                    LogView.log("Cliente '" + client.getClientId() + "' AUTENTICADO! Assinatura do servidor validada com sucesso.");

                } catch (Exception e) {
                    LogView.log("Falha crítica de autenticação (Assinatura inválida): " + e.getMessage());
                    enviarFalhaEClose();
                }
            }

            case "LIST_ALL_TOPICS" -> {
                List<String> allTopics = new ArrayList<>(TopicManager.getAllTopics());
                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(allTopics);
                sendMessage(new Gson().toJson(response).getBytes(StandardCharsets.UTF_8));
            }

            case "LIST_MY_TOPICS" -> {
                List<String> myTopics = new ArrayList<>(TopicManager.getTopicsFromClient(client.getClientId()));
                Message response = new Message();
                response.type = "TOPICS_LIST";
                response.payload = new Gson().toJson(myTopics);
                sendMessage(new Gson().toJson(response).getBytes(StandardCharsets.UTF_8));
            }

            case "SUBSCRIBE" -> {
                TopicManager.subscribe(msg.topic, client.getClientId());
            }

            case "PUBLISH" -> {
                Message response = new Message();
                response.type = "MESSAGE";
                response.topic = msg.topic;
                response.payload = msg.payload;
                response.clientId = client.getClientId();
                response.date = msg.date;
                response.time = msg.time;
                TopicManager.broadcast(msg.topic, response);
            }

            case "GET_BUFFER" -> {
                TopicManager.sendBufferToClient(client.getClientId(), this);
            }

            case "EXIT" -> {
                close();
            }

            case "UNSUBSCRIBE" -> {
                TopicManager.unsubscribe(msg.topic, client.getClientId());
                LogView.log("Cliente " + client.getClientId() + " saiu do tópico " + msg.topic);
            }

            default -> LogView.log("Comando desconhecido: " + msg.type);
        }
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    // --- ALGORITMO CRIPTOGRÁFICO DE APLICAÇÃO ---
    private String calcularHashEsperado(String nonce, String clientId) throws Exception {
        // Tenta buscar no resource o arquivo com o nome exato do ID do cliente (Ex: Miguel.crt)
        java.io.InputStream is = getClass().getResourceAsStream("/" + clientId + ".crt");
        if (is == null) {
            throw new java.io.FileNotFoundException("Arquivo de referência '" + clientId + ".crt' não encontrado em resources!");
        }

        String conteudoCertificado = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        is.close();

        // Combina o desafio aleatório com o texto do certificado guardado
        String combinacao = nonce + conteudoCertificado.trim();

        // Calcula o SHA-256 no braço
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(combinacao.getBytes(StandardCharsets.UTF_8));

        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private void enviarFalhaEClose() {
        Message falha = new Message();
        falha.type = "AUTH_FAILED";
        sendMessage(new Gson().toJson(falha).getBytes(StandardCharsets.UTF_8));
        close();
    }

    protected void close(){
        try {
            TopicManager.disconnectClient(client.getClientId());
            socket.close();
        } catch (IOException e) {
            LogView.log("Erro ao fechar o socket: " + e.getMessage());
        }
        LogView.log("Cliente " + client.getClientId() + " desconectado");
    }

    public void sendMessage(byte[] msg) {
        filaTxMsg.add(msg);
    }

    private boolean isConnected(){
        return !socket.isClosed() && socket.isConnected();
    }
}