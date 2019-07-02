package com.task.pipeline.util;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Closeable;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class BufferingStreamSplitter<T> implements Closeable {

    private static final int DEFAULT_BUFFER_SIZE = Runtime.getRuntime().availableProcessors() << 4;
    private static final int DEFAULT_SPLIT_SIZE = 1;
    private static final long DEFAULT_POLL_TIMEOUT = 1000;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @NonNull
    private final Stream<? extends T> upstream;
    @NonNull
    private final BlockingQueue<Node<T>> queue;
    private final int splitSize;
    private final long pollTimeout;

    private BufferingStreamSplitter(Stream<? extends T> upstream, int bufferSize, int splitSize, long pollTimeout) {
        this(upstream, new LinkedBlockingQueue<>(bufferSize), splitSize, pollTimeout);
    }

    public static <T> Stream<? extends T> split(Stream<? extends T> upstream, int bufferSize, int splitSize, long pollTimeout) {
        BufferingStreamSplitter<T> splitter = new BufferingStreamSplitter<>(upstream, bufferSize, splitSize, pollTimeout);
        splitter.startBuffering();
        return splitter.stream();
    }

    public static <T> Stream<? extends T> split(Stream<? extends T> upstream) {
        return split(upstream, DEFAULT_BUFFER_SIZE, DEFAULT_SPLIT_SIZE, DEFAULT_POLL_TIMEOUT);
    }

    private void startBuffering() {
        executor.submit(new StreamBufferer<>(upstream, queue));
        executor.shutdown();
    }

    private Stream<? extends T> stream() {
        Spliterator<T> spliterator = new BlockingQueueSpliterator<>(queue, splitSize, pollTimeout);
        return StreamSupport.stream(spliterator, false)
                .onClose(this::close);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }


    @RequiredArgsConstructor
    private static class StreamBufferer<T> implements Runnable {

        @NonNull
        private final Stream<? extends T> upstream;
        @NonNull
        private final BlockingQueue<Node<T>> queue;

        @Override
        public void run() {
            RuntimeException exception = null;
            try {
                upstream.sequential().forEach(item -> {
                    try {
                        queue.put(Node.of(item));
                    } catch (InterruptedException e) {
                        throw new IllegalStateException("Thread interrupted when putting an upstream item to the queue: " + item, e);
                    }
                });
            } catch (RuntimeException e) {
                exception = e;
            } finally {
                exception = finalize(exception);
                if (exception != null) {
                    throw exception;
                }
            }
        }

        private RuntimeException finalize(RuntimeException exception) {
            try {
                queue.put(Node.marker());
            } catch (InterruptedException e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = new IllegalStateException("Thread interrupted when putting an end marker to the queue", e);
                }
            } finally {
                try {
                    upstream.close();
                } catch (RuntimeException e) {
                    if (exception != null) {
                        exception.addSuppressed(e);
                    } else {
                        exception = new IllegalStateException("Stream close handler exception", e);
                    }
                }
            }
            return exception;
        }
    }


    private static class BlockingQueueSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

        private static final long ESTIMATE_SIZE = Long.MAX_VALUE;
        private static final int CHARACTERISTICS = CONCURRENT | NONNULL;

        private final BlockingQueue<Node<T>> queue;
        private final int splitSize;
        private final long pollTimeout;

        public BlockingQueueSpliterator(BlockingQueue<Node<T>> queue, int splitSize, long pollTimeout) {
            super(ESTIMATE_SIZE, CHARACTERISTICS);
            this.queue = queue;
            this.splitSize = splitSize;
            this.pollTimeout = pollTimeout;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            try {
                Node<T> node = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                if (node == null) {
                    throw new IllegalStateException("Timed out while waiting for the next item");
                }
                if (node == Node.marker()) {
                    queue.put(node);
                    return false;
                }
                action.accept(node.data);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Thread interrupted when retrieving an item from the queue", e);
            }
            return true;
        }

        @Override
        public Spliterator<T> trySplit() {
            HoldingConsumer<T> holder = new HoldingConsumer<>();
            if (tryAdvance(holder)) {
                Object[] split = new Object[splitSize];
                int i = 0;
                do {
                    split[i] = holder.item;
                } while (++i < splitSize && tryAdvance(holder));
                return Spliterators.spliterator(split, 0);
            }
            return null;
        }


        private static final class HoldingConsumer<T> implements Consumer<T> {

            private T item;

            @Override
            public void accept(T item) {
                this.item = item;
            }
        }
    }


    @RequiredArgsConstructor
    private static class Node<T> {

        private static final Node<?> MARKER_INSTANCE = new Node<>(null);

        private final T data;

        @SuppressWarnings("unchecked")
        private static <T> Node<T> marker() {
            return (Node<T>) MARKER_INSTANCE;
        }

        private static <T> Node<T> of(T data) {
            return new Node<>(data);
        }
    }

}
