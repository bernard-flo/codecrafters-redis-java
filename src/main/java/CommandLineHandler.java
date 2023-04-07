import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

public class CommandLineHandler {

    private final Map<String, BiConsumer<Responder, List<String>>> commandHandlerMap = new HashMap<>();

    private final ExpiringMap map = new ExpiringMap();

    public CommandLineHandler() {
        commandHandlerMap.put("command", this::handleCommand);
        commandHandlerMap.put("ping", this::handlePing);
        commandHandlerMap.put("echo", this::handleEcho);
        commandHandlerMap.put("set", this::handleSet);
        commandHandlerMap.put("get", this::handleGet);
    }

    public void handleCommandLine(Responder responder, List<String> commandLine) {
        String command = commandLine.get(0).toLowerCase();
        commandHandlerMap.get(command).accept(responder, commandLine);
    }

    private void handleCommand(Responder responder, List<String> commandLine) {
        responder.respondSimpleString("");
    }

    private void handlePing(Responder responder, List<String> commandLine) {
        responder.respondSimpleString("PONG");
    }

    private void handleEcho(Responder responder, List<String> commandLine) {
        String arg = commandLine.get(1);
        responder.respondBulkString(arg);
    }

    private void handleSet(Responder responder, List<String> commandLine) {
        String key = commandLine.get(1);
        String value = commandLine.get(2);
        Long expiryMs = null;
        if (commandLine.size() == 5 && commandLine.get(3).equalsIgnoreCase("px")) {
            expiryMs = Long.parseLong(commandLine.get(4));
        }
        map.put(key, value, expiryMs);
        responder.respondSimpleString("OK");
    }

    private void handleGet(Responder responder, List<String> commandLine) {
        String key = commandLine.get(1);
        Optional<String> valueOptional = map.get(key);
        if (valueOptional.isPresent()) {
            responder.respondBulkString(valueOptional.get());
        } else {
            responder.respondNullBulkString();
        }
    }

}
