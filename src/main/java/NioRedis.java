import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

public class NioRedis {

    private final Selector selector = Selector.open();
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(256);

    private final WeakHashMap<SocketChannel, ByteBufferRespReader> byteBufferRespReaderMap = new WeakHashMap<>();

    private final CommandLineHandler commandLineHandler = new CommandLineHandler();

    public NioRedis() throws Exception {
    }

    public void run() throws Exception {
        openServerSocketChannel();

        while (true) {
            selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            selectionKeys.forEach(F.wrapEx(this::handleSelectedKey));
            selectionKeys.clear();
        }
    }

    private void handleSelectedKey(SelectionKey selectionKey) throws Exception {
        if (selectionKey.isAcceptable()) {
            handleAcceptable((ServerSocketChannel) selectionKey.channel());
        } else if (selectionKey.isReadable()) {
            handleReadable((SocketChannel) selectionKey.channel());
        }
    }

    private void handleAcceptable(ServerSocketChannel serverSocketChannel) throws Exception {
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);

        byteBufferRespReaderMap.put(socketChannel, new ByteBufferRespReader(F.bind(this::onCommandLine, socketChannel)));
    }

    private void handleReadable(SocketChannel socketChannel) throws Exception {
        int ret = socketChannel.read(byteBuffer);
        if (ret < 0) {
            socketChannel.close();
            return;
        }

        ByteBufferRespReader byteBufferRespReader = byteBufferRespReaderMap.get(socketChannel);

        byteBuffer.flip();
        byteBufferRespReader.read(byteBuffer);
        byteBuffer.clear();
    }

    private void onCommandLine(SocketChannel socketChannel, List<String> commandLine) {
        commandLineHandler.handleCommandLine(new NioResponder(socketChannel), commandLine);
    }

    private void openServerSocketChannel() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", 6379));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    static class NioResponder extends Responder {

        private final SocketChannel socketChannel;

        public NioResponder(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void respond(String line) {
            try {
                socketChannel.write(ByteBuffer.wrap(line.getBytes()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
