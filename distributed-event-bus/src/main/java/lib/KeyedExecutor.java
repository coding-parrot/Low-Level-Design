package lib;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

public class KeyedExecutor<KEY> {
    private final Executor[] executorPool;

    public KeyedExecutor(final int poolSize) {
        this.executorPool = new Executor[poolSize];
        for (int i = 0; i < poolSize; i++) {
            executorPool[i] = Executors.newSingleThreadExecutor();
        }
    }

    public CompletionStage<Void> getThreadFor(KEY key, Runnable task) {
        return CompletableFuture.runAsync(task, executorPool[Math.abs(key.hashCode() % executorPool.length)]);
    }

    public <U> CompletionStage<U> getThreadFor(KEY key, Supplier<U> task) {
        return CompletableFuture.supplyAsync(task, executorPool[Math.abs(key.hashCode() % executorPool.length)]);
    }

    public <U> CompletionStage<U> getThreadFor(KEY key, CompletionStage<U> task) {
        return CompletableFuture.supplyAsync(() -> task, executorPool[Math.abs(key.hashCode() % executorPool.length)]).thenCompose(Function.identity());
    }
}
