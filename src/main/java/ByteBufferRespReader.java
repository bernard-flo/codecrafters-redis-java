import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ByteBufferRespReader {

    private final ByteBufferLineReader byteBufferLineReader = new ByteBufferLineReader(this::onLine);
    private final ArrayReader arrayReader = new ArrayReader(this::onArray);

    private final Consumer<List<String>> onCommandLine;

    public ByteBufferRespReader(Consumer<List<String>> onCommandLine) {
        this.onCommandLine = onCommandLine;
    }

    public void read(ByteBuffer byteBuffer) {
        byteBufferLineReader.read(byteBuffer);
    }

    private void onLine(String line) {
        System.out.println(line);

        arrayReader.onLine(line);
    }

    private void onArray(List<String> array) {
        onCommandLine.accept(array);
        arrayReader.reset();
    }

    private static class ArrayReader {

        private final List<String> array = new ArrayList<>();
        private final Consumer<List<String>> onArray;

        private final ArrayHeaderReader arrayHeaderReader = new ArrayHeaderReader(this::onArrayHeader);
        private final StringReader stringReader = new StringReader(this::onString);

        private RespReader currentReader = arrayHeaderReader;

        private int size;

        public ArrayReader(Consumer<List<String>> onArray) {
            this.onArray = onArray;
        }

        public void onLine(String line) {
            currentReader.onLine(line);
        }

        public void reset() {
            array.clear();
            size = 0;
            currentReader = arrayHeaderReader;
        }

        private void onArrayHeader(int size) {
            this.size = size;
            currentReader = stringReader;
        }

        private void onString(String string) {
            array.add(string);
            if (array.size() == size) {
                onArray.accept(array);
            }
            stringReader.reset();
        }

    }

    private static class ArrayHeaderReader implements RespReader {

        private final Consumer<Integer> onArrayHeader;

        public ArrayHeaderReader(Consumer<Integer> onArrayHeader) {
            this.onArrayHeader = onArrayHeader;
        }

        public void onLine(String line) {
            int size = Integer.parseInt(line.substring(1));
            onArrayHeader.accept(size);
        }

    }

    private static class StringReader implements RespReader {

        private final Consumer<String> onString;

        private boolean seenHeader;

        public StringReader(Consumer<String> onString) {
            this.onString = onString;
        }

        public void onLine(String line) {
            if (seenHeader) {
                onString.accept(line);
            } else {
                seenHeader = true;
            }
        }

        public void reset() {
            seenHeader = false;
        }

    }

    private interface RespReader {
        void onLine(String line);
    }

}
