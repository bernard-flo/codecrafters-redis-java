import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class Main {


  private static void handlePing(RedisConnection connection) throws IOException {
    connection.writeSimpleString("PONG");
  }

  private static void handleEcho(RedisConnection connection, String arg) throws IOException {
    connection.writeBulkString(arg);
  }

  private static void handleClientSocket(Socket socket) {
    try {
      Scanner scanner = new Scanner(socket.getInputStream());
      RedisConnection connection = new RedisConnection(socket.getOutputStream());

      while (scanner.hasNext()) {
        String line = scanner.nextLine();
        System.out.println("line: " + line);
        if (line.startsWith("ping")) {
          handlePing(connection);
        } else if (line.startsWith("ECHO")) {
          scanner.nextLine();
          String arg = scanner.nextLine();
          handleEcho(connection, arg);
        } else if (line.startsWith("DOCS")) {
          connection.writeSimpleString("");
        }
      }

      socket.close();
      System.out.println("Done");
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        CompletableFuture.runAsync(() -> handleClientSocket(clientSocket));
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static class RedisConnection {
    private static final String CRLF = "\r\n";

    private final OutputStream outputStream;
    public RedisConnection(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    public void write(String line) throws IOException {
      outputStream.write(line.getBytes());
    }

    public void writeSimpleString(String simpleString) throws IOException {
      write("+" + simpleString + CRLF);
    }

    public void writeBulkString(String bulkString) throws IOException {
      write("$" + bulkString.length() + CRLF + bulkString + CRLF);
    }

  }

}
