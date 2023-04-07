import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;

public class AsyncNioRedis {

    private final WeakHashMap<AsynchronousSocketChannel, ByteBuffer> byteBufferMap = new WeakHashMap<>();
    private final WeakHashMap<AsynchronousSocketChannel, ByteBufferRespReader> byteBufferRespReaderMap = new WeakHashMap<>();

    private final CommandLineHandler commandLineHandler = new CommandLineHandler();

    private final AsynchronousServerSocketChannel serverSocketChannel = AsynchronousServerSocketChannel.open()
            .bind(new InetSocketAddress("0.0.0.0", 6379));

    public AsyncNioRedis() throws Exception {
    }

    public void run() throws Exception {
        accept();

        synchronized (this) {
            wait();
        }
    }

    private void accept() {
        acceptAsync(serverSocketChannel)
                .thenAccept(this::accepted)
                .exceptionally(F.returnNull(Throwable::printStackTrace));
    }

    private void accepted(AsynchronousSocketChannel socketChannel) {
        System.out.println("accepted");
        accept();
        byteBufferMap.put(socketChannel, ByteBuffer.allocateDirect(256));
        byteBufferRespReaderMap.put(socketChannel, new ByteBufferRespReader(F.bind(this::onCommandLine, socketChannel)));
        readClient(socketChannel);
    }

    private void readClient(AsynchronousSocketChannel socketChannel) {
        ByteBuffer byteBuffer = byteBufferMap.get(socketChannel);
        byteBuffer.clear();
        readAsync(socketChannel, byteBuffer)
                .thenAccept(F.wrapEx(ret -> onRead(socketChannel, ret)))
                .exceptionally(F.returnNull((throwable) -> {
                    throwable.printStackTrace();
                    closeClient(socketChannel);
                }));
    }

    private void onRead(AsynchronousSocketChannel socketChannel, int ret) throws Exception {
        if (ret <= 0) {
            closeClient(socketChannel);
            return;
        }

        ByteBuffer byteBuffer = byteBufferMap.get(socketChannel);
        ByteBufferRespReader byteBufferRespReader = byteBufferRespReaderMap.get(socketChannel);

        byteBuffer.flip();
        byteBufferRespReader.read(byteBuffer);

        readClient(socketChannel);
    }

    private void onCommandLine(AsynchronousSocketChannel socketChannel, List<String> commandLine) {
        commandLineHandler.handleCommandLine(new AsyncNioResponder(socketChannel), commandLine);
    }

    private void closeClient(AsynchronousSocketChannel socketChannel) throws Exception {
        System.out.println("closeClient");
        socketChannel.close();
    }

    static class AsyncNioResponder extends Responder {

        private final AsynchronousSocketChannel socketChannel;

        public AsyncNioResponder(AsynchronousSocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void respond(String line) {
            writeAsync(socketChannel, ByteBuffer.wrap(line.getBytes()))
                    .exceptionally(F.returnNull(Throwable::printStackTrace));
        }

    }

    static CompletableFuture<AsynchronousSocketChannel> acceptAsync(AsynchronousServerSocketChannel serverSocketChannel) {
        CompletableFuture<AsynchronousSocketChannel> completableFuture = new CompletableFuture<>();
        serverSocketChannel.accept(completableFuture, getCompletionHandler());
        return completableFuture;
    }

    static CompletableFuture<Integer> readAsync(AsynchronousSocketChannel socketChannel, ByteBuffer byteBuffer) {
        CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        socketChannel.read(byteBuffer, completableFuture, getCompletionHandler());
        return completableFuture;
    }

    static CompletableFuture<Integer> writeAsync(AsynchronousSocketChannel socketChannel, ByteBuffer byteBuffer) {
        CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
        socketChannel.write(byteBuffer, completableFuture, getCompletionHandler());
        return completableFuture;
    }

    @SuppressWarnings("unchecked")
    static <T> CompletionHandler<T, CompletableFuture<T>> getCompletionHandler() {
        return (CompletionHandler<T, CompletableFuture<T>>) (Object) completionHandler;
    }

    static CompletionHandler<Object, CompletableFuture<Object>> completionHandler = new CompletionHandler<Object, CompletableFuture<Object>>() {

        @Override
        public void completed(Object result, CompletableFuture<Object> attachment) {
            attachment.complete(result);
        }

        @Override
        public void failed(Throwable exc, CompletableFuture<Object> attachment) {
            attachment.completeExceptionally(exc);
        }

    };

}
