import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class ByteBufferLineReader {

    private static final byte CR = "\r".getBytes()[0];
    private static final byte LF = "\n".getBytes()[0];

    private final StringBuffer stringBuffer = new StringBuffer();

    private boolean seenCr = false;

    private final Consumer<String> onLine;

    public ByteBufferLineReader(Consumer<String> onLine) {
        this.onLine = onLine;
    }

    public void read(ByteBuffer byteBuffer) {
        int begin = byteBuffer.position();

        while (byteBuffer.hasRemaining()) {
            byte b = byteBuffer.get();

            if (b == CR) {
                seenCr = true;
            } else if (b == LF) {
                if (seenCr) {
                    int position = byteBuffer.position();
                    int end = position - 2;
                    String line = byteBufferToString(byteBuffer, begin, end);
                    begin = position;
                    onLine.accept(line);
                }
                seenCr = false;
            } else {
                seenCr = false;
            }
        }
    }

    private static String byteBufferToString(ByteBuffer byteBuffer, int begin, int end) {
        ByteBuffer slice = byteBuffer.duplicate();
        slice.position(begin);
        slice.limit(end);
        return StandardCharsets.UTF_8.decode(slice).toString();
    }

}
