package so.ifsc;

import so.ifsc.Threads.ClientService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int SERVER_PORT = 5000;
//    Pool de threads (cria sobre demanda)
    private static final ExecutorService pool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        try (
//                abre servidor TCP
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            System.out.println("Broker escutando na porta: " + SERVER_PORT);

//            aceita conexão de clientes
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Novo cliente: " + clientSocket.getInetAddress());
                //add socket na pool de trheds
                try {
//                    Controlador do cliente
                    ClientService ControladorClient = new ClientService(clientSocket);
                    pool.execute(ControladorClient::init);
                } catch (IOException e){
                    System.out.println("Erro ao criar o controlador do Client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            e.fillInStackTrace();
        } finally {
            pool.shutdown();
        }
    }
}