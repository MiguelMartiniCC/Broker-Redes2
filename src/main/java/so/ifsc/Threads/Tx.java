package so.ifsc.Threads;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;

import so.ifsc.View.LogView;
public class Tx implements Runnable{
    private final OutputStream out;
    private final BlockingQueue<byte[]> sendingQueue;
    private final ClientService service;

    public Tx(OutputStream out, BlockingQueue<byte[]> sendingQueue, ClientService service) {
        this.out = out;
        this.sendingQueue = sendingQueue;
        this.service = service;
    }

    public void run() {
        try {
            while (true) {
//                segura a thread ate ter msgs
                byte[] msg = sendingQueue.take();
                out.write(msg);
                out.write('\n');
                out.flush();
            }
        } catch (InterruptedException | IOException  e) {
            LogView.log("Tx interrompido para o cliente: "+ service.client.getClientId()+ ": " + e.getMessage());
        } finally {
            service.close();
        }
    }
}
