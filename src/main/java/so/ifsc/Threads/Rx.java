package so.ifsc.Threads;

import com.google.gson.Gson;
import so.ifsc.Models.Message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class Rx implements Runnable{

    private final InputStream in;
    private final ClientService service;

    public Rx(InputStream in, ClientService service) {
        this.in = in;
        this.service = service;
    }

    public void run() {
        try {
            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                byte[] data = new byte[bytesRead];
                System.arraycopy(buffer, 0, data, 0, bytesRead);
                String message = new String(data, StandardCharsets.UTF_8);

                try {
                    Message msg = new Gson().fromJson(message, Message.class);

                    System.out.println("Cliente ip : " + service.client.getId());
                    System.out.println("Tipo    : " + msg.type);
                    System.out.println("Topico   : " + msg.topic);
                    System.out.println("Mensagem : " + msg.payload);
                    System.out.println("Data    : " + msg.date);
                    System.out.println("Horario    : " + msg.time +"\n");

                    service.processMessage(msg);
                }
                catch (Exception e) {
                    System.out.println("Falha ao tentar fazer parse na mensagem: " + message);
                }
            }
        } catch (IOException e) {
            System.out.println("Erro ao ler (Rx) client " + service.client.getId() + ": " + e.getMessage());
            e.fillInStackTrace();
        } finally {
            service.close();
        }
    }
}
