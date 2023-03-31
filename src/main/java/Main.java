import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class Main {

  private static ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

  private static void handleCommand(RedisConnection connection, List<String> commandLine) {
    connection.writeSimpleString("");
  }

  private static void handlePing(RedisConnection connection, List<String> commandLine) {
    connection.writeSimpleString("PONG");
  }

  private static void handleEcho(RedisConnection connection, List<String> commandLine) {
    String arg = commandLine.get(1);
    connection.writeBulkString(arg);
  }

  private static void handleSet(RedisConnection connection, List<String> commandLine) {
    String key = commandLine.get(1);
    String value = commandLine.get(2);
    map.put(key, value);
    connection.writeSimpleString("OK");
  }

  private static void handleGet(RedisConnection connection, List<String> commandLine) {
    String key = commandLine.get(1);
    String value = map.get(key);
    connection.writeBulkString(value);
  }

  private static void handleClientSocket(Socket socket) {
    Map<String, BiConsumer<RedisConnection, List<String>>> commandHandler = new HashMap<>();
    commandHandler.put("command", Main::handleCommand);
    commandHandler.put("ping", Main::handlePing);
    commandHandler.put("echo", Main::handleEcho);
    commandHandler.put("set", Main::handleSet);
    commandHandler.put("get", Main::handleGet);

    try {

      RedisConnection connection = new RedisConnection(socket);

      while (connection.hasNext()) {
        List<String> commandLine = connection.readArray();
        System.out.println("commandLine: " + commandLine);
        String command = commandLine.get(0).toLowerCase();
        commandHandler.get(command).accept(connection, commandLine);
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

    private final Scanner scanner;
    private final OutputStream outputStream;
    public RedisConnection(Socket socket) throws IOException {
      this.scanner = new Scanner(socket.getInputStream());
      this.outputStream = socket.getOutputStream();
    }

    public boolean hasNext() {
      return scanner.hasNext();
    }

    public List<String> readArray() {
      String firstLine = scanner.nextLine();
      int size = Integer.parseInt(firstLine.substring(1));
      List<String> result = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        scanner.nextLine();
        result.add(scanner.nextLine());
      }
      return result;
    }

    public void write(String line) {
      try {
        outputStream.write(line.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void writeSimpleString(String simpleString) {
      write("+" + simpleString + CRLF);
    }

    public void writeBulkString(String bulkString) {
      write("$" + bulkString.length() + CRLF + bulkString + CRLF);
    }

  }

}
