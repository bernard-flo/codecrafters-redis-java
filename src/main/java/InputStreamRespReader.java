import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.Consumer;

public class InputStreamRespReader {

    private final Scanner scanner;
    private final Consumer<List<String>> onCommandLine;

    public InputStreamRespReader(InputStream inputStream, Consumer<List<String>> onCommandLine) {
        this.scanner = new Scanner(inputStream);
        this.onCommandLine = onCommandLine;
    }

    public void read() {
        while (scanner.hasNext()) {
            List<String> commandLine = readArray();
            onCommandLine.accept(commandLine);
        }
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

}
