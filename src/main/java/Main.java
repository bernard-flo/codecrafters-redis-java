import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Main {


  private static void handlePing(RedisConnection connection, String line) throws IOException {
    connection.writeSimpleString("PONG");
  }

  private static void handleClientSocket(Socket socket) throws IOException {
    Scanner scanner = new Scanner(socket.getInputStream());
    RedisConnection connection = new RedisConnection(socket.getOutputStream());

    while (scanner.hasNext()) {
      String line = scanner.nextLine();
      System.out.println("line: " + line);
      if (line.startsWith("ping")) {
        handlePing(connection, line);
      } else if (line.startsWith("DOCS")) {
        connection.writeSimpleString("");
      }
    }

    socket.close();
    System.out.println("Done");
  }

  public static void main(String[] args){
    boolean debug;
    if (args.length >= 1) {
      debug = true;
    } else {
      debug = false;
    }

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        // Wait for connection from client.
        clientSocket = serverSocket.accept();
        handleClientSocket(clientSocket);
        if (debug == false) {
          break;
        }
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static class RedisConnection {
    private final OutputStream outputStream;
    public RedisConnection(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    public void writeSimpleString(String simpleString) throws IOException {
      String line = "+" + simpleString + "\r\n";
      outputStream.write(line.getBytes());
    }
  }

}
