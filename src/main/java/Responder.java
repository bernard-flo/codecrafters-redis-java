import java.io.IOException;

public abstract class Responder {

    private static final String CRLF = "\r\n";

    public abstract void respond(String line);

    public void respondSimpleString(String simpleString) {
        respond("+" + simpleString + CRLF);
    }

    public void respondBulkString(String bulkString) {
        respond("$" + bulkString.length() + CRLF + bulkString + CRLF);
    }

    public void respondNullBulkString() {
        respond("$-1" + CRLF);
    }

}
