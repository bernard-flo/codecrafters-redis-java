import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class ThreadPoolRedis {

    private final CommandLineHandler commandLineHandler = new CommandLineHandler();

    public void run() throws Exception {
        ServerSocket serverSocket = new ServerSocket(6379);
        while (true) {
            Socket clientSocket = serverSocket.accept();
            CompletableFuture.runAsync(F.wrapEx(() -> handleClientSocket(clientSocket)));
        }
    }

    private void handleClientSocket(Socket socket) throws Exception {
        try {
            new InputStreamRespReader(socket.getInputStream(), F.bind(this::onCommandLine, socket))
                    .read();
        } finally {
            socket.close();
        }
    }

    private void onCommandLine(Socket socket, List<String> commandLine) throws Exception {
        commandLineHandler.handleCommandLine(new SocketResponder(socket), commandLine);
    }

    static class SocketResponder extends Responder {

        private final OutputStream outputStream;

        public SocketResponder(Socket socket) throws Exception {
            this.outputStream = socket.getOutputStream();
        }

        @Override
        public void respond(String line) {
            try {
                outputStream.write(line.getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
