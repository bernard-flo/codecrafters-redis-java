import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public enum Utils {
    ;

    public static String byteBufferToString(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        return StandardCharsets.UTF_8.decode(byteBuffer).toString();
    }

}
