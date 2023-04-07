import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class ThreadPoolRedis {

  private static ExpiringMap map = new ExpiringMap();

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
    Long expiryMs = null;
    if (commandLine.size() == 5 && commandLine.get(3).equalsIgnoreCase("px")) {
      expiryMs = Long.parseLong(commandLine.get(4));
    }
    map.put(key, value, expiryMs);
    connection.writeSimpleString("OK");
  }

  private static void handleGet(RedisConnection connection, List<String> commandLine) {
    String key = commandLine.get(1);
    Optional<String> valueOptional = map.get(key);
    if (valueOptional.isPresent()) {
      connection.writeBulkString(valueOptional.get());
    } else {
      connection.writeNullBulkString();
    }
  }

  private static void handleClientSocket(Socket socket) {
    Map<String, BiConsumer<RedisConnection, List<String>>> commandHandler = new HashMap<>();
    commandHandler.put("command", ThreadPoolRedis::handleCommand);
    commandHandler.put("ping", ThreadPoolRedis::handlePing);
    commandHandler.put("echo", ThreadPoolRedis::handleEcho);
    commandHandler.put("set", ThreadPoolRedis::handleSet);
    commandHandler.put("get", ThreadPoolRedis::handleGet);

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

    public void writeNullBulkString() {
      write("$-1" + CRLF);
    }

  }

}
