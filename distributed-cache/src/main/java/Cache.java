import events.*;
import models.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class Cache<KEY, VALUE> {
    private final int maximumSize;
    private final FetchAlgorithm fetchAlgorithm;
    private final Duration expiryTime;
    private final Map<KEY, CompletionStage<Record<KEY, VALUE>>> cache;
    private final ConcurrentSkipListMap<AccessDetails, List<KEY>> priorityQueue;
    private final ConcurrentSkipListMap<Long, List<KEY>> expiryQueue;
    private final DataSource<KEY, VALUE> dataSource;
    private final List<Event<KEY, VALUE>> eventQueue;
    private final ExecutorService[] executorPool;
    private final Timer timer;

    protected Cache(final int maximumSize,
                    final Duration expiryTime,
                    final FetchAlgorithm fetchAlgorithm,
                    final EvictionAlgorithm evictionAlgorithm,
                    final DataSource<KEY, VALUE> dataSource,
                    final Set<KEY> keysToEagerlyLoad,
                    final Timer timer,
                    final int poolSize) {
        this.expiryTime = expiryTime;
        this.maximumSize = maximumSize;
        this.fetchAlgorithm = fetchAlgorithm;
        this.timer = timer;
        this.cache = new ConcurrentHashMap<>();
        this.eventQueue = new CopyOnWriteArrayList<>();
        this.dataSource = dataSource;
        this.executorPool = new ExecutorService[poolSize];
        for (int i = 0; i < poolSize; i++) {
            executorPool[i] = Executors.newSingleThreadExecutor();
        }
        priorityQueue = new ConcurrentSkipListMap<>((first, second) -> {
            final var accessTimeDifference = (int) (first.getLastAccessTime() - second.getLastAccessTime());
            if (evictionAlgorithm.equals(EvictionAlgorithm.LRU)) {
                return accessTimeDifference;
            } else {
                final var accessCountDifference = first.getAccessCount() - second.getAccessCount();
                return accessCountDifference != 0 ? accessCountDifference : accessTimeDifference;
            }
        });
        expiryQueue = new ConcurrentSkipListMap<>();
        final var eagerLoading = keysToEagerlyLoad.stream()
                .map(key -> getThreadFor(key, addToCache(key, loadFromDB(dataSource, key))))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(eagerLoading).join();
    }

    private <U> CompletionStage<U> getThreadFor(KEY key, CompletionStage<U> task) {
        return CompletableFuture.supplyAsync(() -> task, executorPool[Math.abs(key.hashCode() % executorPool.length)]).thenCompose(Function.identity());
    }

    public CompletionStage<VALUE> get(KEY key) {
        return getThreadFor(key, getFromCache(key));
    }

    public CompletionStage<Void> set(KEY key, VALUE value) {
        return getThreadFor(key, setInCache(key, value));
    }

    private CompletionStage<VALUE> getFromCache(KEY key) {
        final CompletionStage<Record<KEY, VALUE>> result;
        if (!cache.containsKey(key)) {
            result = addToCache(key, loadFromDB(dataSource, key));
        } else {
            result = cache.get(key).thenCompose(record -> {
                if (hasExpired(record)) {
                    priorityQueue.get(record.getAccessDetails()).remove(key);
                    expiryQueue.get(record.getInsertionTime()).remove(key);
                    eventQueue.add(new Eviction<>(record, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                    return addToCache(key, loadFromDB(dataSource, key));
                } else {
                    return CompletableFuture.completedFuture(record);
                }
            });
        }
        return result.thenApply(record -> {
            priorityQueue.get(record.getAccessDetails()).remove(key);
            final AccessDetails updatedAccessDetails = record.getAccessDetails().update(timer.getCurrentTime());
            priorityQueue.putIfAbsent(updatedAccessDetails, new CopyOnWriteArrayList<>());
            priorityQueue.get(updatedAccessDetails).add(key);
            record.setAccessDetails(updatedAccessDetails);
            return record.getValue();
        });
    }

    public CompletionStage<Void> setInCache(KEY key, VALUE value) {
        CompletionStage<Void> result = CompletableFuture.completedFuture(null);
        if (cache.containsKey(key)) {
            result = cache.remove(key)
                    .thenAccept(oldRecord -> {
                        priorityQueue.get(oldRecord.getAccessDetails()).remove(key);
                        expiryQueue.get(oldRecord.getInsertionTime()).remove(key);
                        if (hasExpired(oldRecord)) {
                            eventQueue.add(new Eviction<>(oldRecord, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                        } else {
                            eventQueue.add(new Update<>(new Record<>(key, value, timer.getCurrentTime()), oldRecord, timer.getCurrentTime()));
                        }
                    });
        }
        return result.thenCompose(__ -> addToCache(key, CompletableFuture.completedFuture(value))).thenCompose(record -> {
            final CompletionStage<Void> writeOperation = persistRecord(record);
            return fetchAlgorithm == FetchAlgorithm.WRITE_THROUGH ? writeOperation : CompletableFuture.completedFuture(null);
        });
    }

    private CompletionStage<Record<KEY, VALUE>> addToCache(final KEY key, final CompletionStage<VALUE> valueFuture) {
        manageEntries();
        final var recordFuture = valueFuture.thenApply(value -> {
            final Record<KEY, VALUE> record = new Record<>(key, value, timer.getCurrentTime());
            expiryQueue.putIfAbsent(record.getInsertionTime(), new CopyOnWriteArrayList<>());
            expiryQueue.get(record.getInsertionTime()).add(key);
            priorityQueue.putIfAbsent(record.getAccessDetails(), new CopyOnWriteArrayList<>());
            priorityQueue.get(record.getAccessDetails()).add(key);
            return record;
        });
        cache.put(key, recordFuture);
        return recordFuture;
    }

    private synchronized void manageEntries() {
        if (cache.size() >= maximumSize) {
            while (!expiryQueue.isEmpty() && hasExpired(expiryQueue.firstKey())) {
                final List<KEY> keys = expiryQueue.pollFirstEntry().getValue();
                for (final KEY key : keys) {
                    final Record<KEY, VALUE> expiredRecord = cache.remove(key).toCompletableFuture().join();
                    priorityQueue.remove(expiredRecord.getAccessDetails());
                    eventQueue.add(new Eviction<>(expiredRecord, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                }
            }
        }
        if (cache.size() >= maximumSize) {
            List<KEY> keys = priorityQueue.pollFirstEntry().getValue();
            while (keys.isEmpty()) {
                keys = priorityQueue.pollFirstEntry().getValue();
            }
            for (final KEY key : keys) {
                final Record<KEY, VALUE> lowestPriorityRecord = cache.remove(key).toCompletableFuture().join();
                expiryQueue.get(lowestPriorityRecord.getInsertionTime()).remove(lowestPriorityRecord.getKey());
                eventQueue.add(new Eviction<>(lowestPriorityRecord, Eviction.Type.REPLACEMENT, timer.getCurrentTime()));
            }
        }
    }

    private CompletionStage<Void> persistRecord(final Record<KEY, VALUE> record) {
        return dataSource.persist(record.getKey(), record.getValue(), record.getInsertionTime())
                .thenAccept(__ -> eventQueue.add(new Write<>(record, timer.getCurrentTime())));
    }

    private boolean hasExpired(final Record<KEY, VALUE> record) {
        return hasExpired(record.getInsertionTime());
    }

    private boolean hasExpired(final Long time) {
        return Duration.ofNanos(timer.getCurrentTime() - time).compareTo(expiryTime) > 0;
    }

    public List<Event<KEY, VALUE>> getEventQueue() {
        return eventQueue;
    }

    private CompletionStage<VALUE> loadFromDB(final DataSource<KEY, VALUE> dataSource, KEY key) {
        return dataSource.load(key).whenComplete((value, throwable) -> {
            if (throwable == null) {
                eventQueue.add(new Load<>(new Record<>(key, value, timer.getCurrentTime()), timer.getCurrentTime()));
            }
        });
    }
}

