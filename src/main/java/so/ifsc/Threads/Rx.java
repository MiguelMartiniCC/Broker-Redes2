package so.ifsc.Threads;

import com.google.gson.Gson;
import so.ifsc.Models.Message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import so.ifsc.View.LogView;

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
                    
                    LogView.log("===");
                    LogView.log("Cliente ip : " + service.client.getId());
                    LogView.log("Tipo    : " + msg.type);
                    LogView.log("Topico   : " + msg.topic);
                    LogView.log("Mensagem : " + msg.payload);
                    LogView.log("Data    : " + msg.date);
                    LogView.log("Horario    : " + msg.time);
                    LogView.log("===");

                    service.processMessage(msg);
                }
                catch (Exception e) {
                    LogView.log("Falha ao tentar fazer parse na mensagem: " + message);
                }
            }
        } catch (IOException e) {
            LogView.log("Erro ao ler (Rx) client " + service.client.getId() + ": " + e.getMessage());
            e.fillInStackTrace();
        } finally {
            service.close();
        }
    }
}
