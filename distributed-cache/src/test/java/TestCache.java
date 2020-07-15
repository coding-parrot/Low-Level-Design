import events.Eviction;
import events.Load;
import events.Update;
import events.Write;
import models.EvictionAlgorithm;
import models.FetchAlgorithm;
import models.SettableTimer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TestCache {

    private static final String PROFILE_MUMBAI_ENGINEER = "profile_mumbai_engineer", PROFILE_HYDERABAD_ENGINEER = "profile_hyderabad_engineer";
    private final Map<String, String> dataMap = new ConcurrentHashMap<>();
    private DataSource<String, String> dataSource;
    private final Queue<CompletableFuture<Void>> writeOperations = new LinkedList<>();
    private DataSource<String, String> writeBackDataSource;

    @Before
    public void setUp() {
        dataMap.clear();
        writeOperations.clear();
        dataMap.put(PROFILE_MUMBAI_ENGINEER, "violet");
        dataMap.put(PROFILE_HYDERABAD_ENGINEER, "blue");
        dataSource = new DataSource<>() {
            @Override
            public CompletionStage<String> load(String key) {
                if (dataMap.containsKey(key)) {
                    return CompletableFuture.completedFuture(dataMap.get(key));
                } else {
                    return CompletableFuture.failedStage(new NullPointerException());
                }
            }

            @Override
            public CompletionStage<Void> persist(String key, String value, long timestamp) {
                dataMap.put(key, value);
                return CompletableFuture.completedFuture(null);
            }
        };

        writeBackDataSource = new DataSource<>() {
            @Override
            public CompletionStage<String> load(String key) {
                if (dataMap.containsKey(key)) {
                    return CompletableFuture.completedFuture(dataMap.get(key));
                } else {
                    return CompletableFuture.failedStage(new NullPointerException());
                }
            }

            @Override
            public CompletionStage<Void> persist(String key, String value, long timestamp) {
                final CompletableFuture<Void> hold = new CompletableFuture<>();
                writeOperations.add(hold);
                return hold.thenAccept(__ -> dataMap.put(key, value));
            }
        };
    }

    private void acceptWrite() {
        final CompletableFuture<Void> write = writeOperations.poll();
        if (write != null) {
            write.complete(null);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheConstructionWithoutDataSourceFailure() {
        new CacheBuilder<>().build();
    }

    @Test
    public void testCacheDefaultBehavior() throws ExecutionException, InterruptedException {
        final var cache = new CacheBuilder<String, String>().dataSource(dataSource).build();
        Assert.assertNotNull(cache);
        assert isEqualTo(cache.get(PROFILE_MUMBAI_ENGINEER), "violet");
        assert cache.get("random")
                .exceptionally(throwable -> Boolean.TRUE.toString())
                .thenApply(Boolean::valueOf)
                .toCompletableFuture()
                .get();
        assert isEqualTo(cache.set(PROFILE_MUMBAI_ENGINEER, "brown").thenCompose(__ -> cache.get(PROFILE_MUMBAI_ENGINEER)), "brown");
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Update;
        assert cache.getEventQueue().get(2) instanceof Write;
    }

    @Test
    public void Eviction_LRU() {
        final var maximumSize = 2;
        final var cache = new CacheBuilder<String, String>()
                .maximumSize(maximumSize)
                .evictionAlgorithm(EvictionAlgorithm.LRU)
                .fetchAlgorithm(FetchAlgorithm.WRITE_BACK)
                .dataSource(writeBackDataSource).build();
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        for (int i = 0; i < maximumSize; i++) {
            cache.set("key" + i, "value" + i).toCompletableFuture().join();
        }
        Assert.assertEquals(2, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Eviction;
        final var evictionEvent = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.REPLACEMENT, evictionEvent.getType());
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, evictionEvent.getElement().getKey());
        cache.getEventQueue().clear();
        final var permutation = new ArrayList<Integer>();
        for (int i = 0; i < maximumSize; i++) {
            permutation.add(i);
        }
        Collections.shuffle(permutation);
        for (final int index : permutation) {
            cache.get("key" + index).toCompletableFuture().join();
        }
        for (int i = 0; i < maximumSize; i++) {
            cache.set("random" + permutation.get(i), "random_value").toCompletableFuture().join();
            assert cache.getEventQueue().get(i) instanceof Eviction;
            final var eviction = (Eviction<String, String>) cache.getEventQueue().get(i);
            Assert.assertEquals(Eviction.Type.REPLACEMENT, eviction.getType());
            Assert.assertEquals("key" + permutation.get(i), eviction.getElement().getKey());
        }
    }

    @Test
    public void Eviction_LFU() {
        final var maximumSize = 2;
        final var cache = new CacheBuilder<String, String>()
                .maximumSize(maximumSize)
                .evictionAlgorithm(EvictionAlgorithm.LFU)
                .fetchAlgorithm(FetchAlgorithm.WRITE_BACK)
                .dataSource(writeBackDataSource)
                .build();
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        for (int i = 0; i < maximumSize; i++) {
            cache.set("key" + i, "value" + i).toCompletableFuture().join();
        }
        Assert.assertEquals(2, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Eviction;
        final var evictionEvent = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.REPLACEMENT, evictionEvent.getType());
        Assert.assertEquals("key0", evictionEvent.getElement().getKey());
        for (int i = 0; i < maximumSize; i++) {
            acceptWrite();
        }
        final var permutation = new ArrayList<Integer>();
        for (int i = 0; i < maximumSize; i++) {
            permutation.add(i);
        }
        Collections.shuffle(permutation);
        for (final int index : permutation) {
            for (int i = 0; i <= index; i++) {
                cache.get("key" + index).toCompletableFuture().join();
            }
        }
        cache.getEventQueue().clear();
        for (int i = 0; i < maximumSize; i++) {
            cache.set("random" + i, "random_value").toCompletableFuture().join();
            acceptWrite();
            for (int j = 0; j <= maximumSize; j++) {
                cache.get("random" + i).toCompletableFuture().join();
            }
            Assert.assertEquals(Eviction.class.getName(), cache.getEventQueue().get(i * 2).getClass().getName());
            Assert.assertEquals(Write.class.getName(), cache.getEventQueue().get(i * 2 + 1).getClass().getName());
            final var eviction = (Eviction<String, String>) cache.getEventQueue().get(i * 2);
            System.out.println(cache.getEventQueue().get(i));
            Assert.assertEquals(Eviction.Type.REPLACEMENT, eviction.getType());
            Assert.assertEquals("key" + i, eviction.getElement().getKey());
        }
    }

    @Test
    public void ExpiryOnGet() {
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>().timer(timer).dataSource(dataSource).expiryTime(Duration.ofSeconds(10)).build();
        timer.setTime(startTime);
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        Assert.assertEquals(1, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, cache.getEventQueue().get(0).getElement().getKey());
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(1) instanceof Eviction;
        assert cache.getEventQueue().get(2) instanceof Load;
        final var eviction = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction.getType());
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, eviction.getElement().getKey());
    }

    @Test
    public void ExpiryOnSet() {
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>().timer(timer).dataSource(dataSource).expiryTime(Duration.ofSeconds(10)).build();
        timer.setTime(startTime);
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        Assert.assertEquals(1, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, cache.getEventQueue().get(0).getElement().getKey());
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.set(PROFILE_MUMBAI_ENGINEER, "blue").toCompletableFuture().join();
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(1) instanceof Eviction;
        assert cache.getEventQueue().get(2) instanceof Write;
        final var eviction = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction.getType());
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, eviction.getElement().getKey());
    }

    @Test
    public void ExpiryOnEviction() {
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>().maximumSize(2).timer(timer).dataSource(dataSource).expiryTime(Duration.ofSeconds(10)).build();
        timer.setTime(startTime);
        cache.get(PROFILE_MUMBAI_ENGINEER).toCompletableFuture().join();
        cache.get(PROFILE_HYDERABAD_ENGINEER).toCompletableFuture().join();
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.set("randomKey", "randomValue").toCompletableFuture().join();
        Assert.assertEquals(5, cache.getEventQueue().size());
        assert cache.getEventQueue().get(2) instanceof Eviction;
        assert cache.getEventQueue().get(3) instanceof Eviction;
        assert cache.getEventQueue().get(4) instanceof Write;
        final var eviction1 = (Eviction<String, String>) cache.getEventQueue().get(2);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction1.getType());
        Assert.assertEquals(PROFILE_MUMBAI_ENGINEER, eviction1.getElement().getKey());
        final var eviction2 = (Eviction<String, String>) cache.getEventQueue().get(3);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction2.getType());
        Assert.assertEquals(PROFILE_HYDERABAD_ENGINEER, eviction2.getElement().getKey());
    }

    @Test
    public void FetchingWriteBack() {
        final var cache = new CacheBuilder<String, String>()
                .maximumSize(1)
                .dataSource(writeBackDataSource)
                .fetchAlgorithm(FetchAlgorithm.WRITE_BACK)
                .build();
        cache.set("randomKey", "randomValue").toCompletableFuture().join();
        Assert.assertEquals(0, cache.getEventQueue().size());
        Assert.assertNull(dataMap.get("randomValue"));
        acceptWrite();
    }

    @Test
    public void FetchingWriteThrough() {
        final var cache = new CacheBuilder<String, String>().dataSource(dataSource).fetchAlgorithm(FetchAlgorithm.WRITE_THROUGH).build();
        cache.set("randomKey", "randomValue").toCompletableFuture().join();
        Assert.assertEquals(1, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Write;
        Assert.assertEquals("randomValue", dataMap.get("randomKey"));
    }

    @Test
    public void EagerLoading() {
        final var eagerlyLoad = new HashSet<String>();
        eagerlyLoad.add(PROFILE_MUMBAI_ENGINEER);
        eagerlyLoad.add(PROFILE_HYDERABAD_ENGINEER);
        final var cache = new CacheBuilder<String, String>()
                .loadKeysOnStart(eagerlyLoad)
                .dataSource(dataSource)
                .build();
        Assert.assertEquals(2, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Load;
        cache.getEventQueue().clear();
        dataMap.clear();
        isEqualTo(cache.get(PROFILE_MUMBAI_ENGINEER), "violet");
        isEqualTo(cache.get(PROFILE_HYDERABAD_ENGINEER), "blue");
        Assert.assertEquals(0, cache.getEventQueue().size());
    }

    @Test
    public void RaceConditions() throws ExecutionException, InterruptedException {
        final var cache = new CacheBuilder<String, String>()
                .poolSize(8)
                .dataSource(dataSource).build();
        final var cacheEntries = new HashMap<String, List<String>>();
        final var numberOfEntries = 100;
        final var numberOfValues = 1000;
        final String[] keyList = new String[numberOfEntries];
        final Map<String, Integer> inverseMapping = new HashMap<>();
        for (int entry = 0; entry < numberOfEntries; entry++) {
            final var key = UUID.randomUUID().toString();
            keyList[entry] = key;
            inverseMapping.put(key, entry);
            cacheEntries.put(key, new ArrayList<>());
            final var firstValue = UUID.randomUUID().toString();
            dataMap.put(key, firstValue);
            cacheEntries.get(key).add(firstValue);
            for (int value = 0; value < numberOfValues - 1; value++) {
                cacheEntries.get(key).add(UUID.randomUUID().toString());
            }
        }
        final Random random = new Random();
        final List<CompletionStage<String>> futures = new ArrayList<>();
        final List<String> queries = new ArrayList<>();
        final int[] updates = new int[numberOfEntries];
        for (int i = 0; i < 1000000; i++) {
            final var index = random.nextInt(numberOfEntries);
            final var key = keyList[index];
            if (Math.random() <= 0.05) {
                if (updates[index] - 1 < numberOfEntries) {
                    updates[index]++;
                }
                cache.set(key, cacheEntries.get(key).get(updates[index] + 1));
            } else {
                queries.add(key);
                futures.add(cache.get(key));
            }
        }
        final CompletionStage<List<String>> results = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(__ -> futures.stream()
                        .map(CompletionStage::toCompletableFuture)
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
        final int[] currentIndexes = new int[numberOfEntries];
        final StringBuilder stringBuilder = new StringBuilder();
        results.thenAccept(values -> {
            for (int i = 0; i < values.size(); i++) {
                final var key = queries.get(i);
                final var possibleValuesForKey = cacheEntries.get(key);
                final var currentValue = currentIndexes[inverseMapping.get(key)];
                if (!possibleValuesForKey.get(currentValue).equals(values.get(i))) {
                    int offset = 1;
                    while (currentValue + offset < numberOfValues && !possibleValuesForKey.get(currentValue + offset).equals(values.get(i))) {
                        offset++;
                    }
                    if (currentValue + offset == numberOfValues) {
                        System.out.println(Arrays.stream(stringBuilder.toString().split("\n")).filter(line -> line.contains(key)).collect(Collectors.joining("\n")));
                        System.err.println(key);
                        System.err.println(possibleValuesForKey);
                        System.err.println(possibleValuesForKey.get(currentValue) + " index: " + currentIndexes[inverseMapping.get(key)]);
                        System.err.println(values.get(i));
                        throw new IllegalStateException();
                    }
                    currentIndexes[inverseMapping.get(key)] += offset;
                    stringBuilder.append(key).append(" index: ").append(currentIndexes[inverseMapping.get(key)]).append(" ").append(values.get(i)).append('\n');
                }
            }
        }).toCompletableFuture().join();
    }

    private boolean isEqualTo(CompletionStage<String> future, String value) {
        return future.thenApply(result -> {
            if (result.equals(value)) {
                return true;
            } else {
                throw new AssertionError();
            }
        }).toCompletableFuture().join();
    }
}
