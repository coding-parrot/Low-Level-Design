import models.EvictionAlgorithm;
import models.FetchAlgorithm;
import models.Timer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class CacheBuilder<KEY, VALUE> {
    private int maximumSize;
    private Duration expiryTime;
    private final Set<KEY> onStartLoad;
    private EvictionAlgorithm evictionAlgorithm;
    private FetchAlgorithm fetchAlgorithm;
    private DataSource<KEY, VALUE> dataSource;
    private Timer timer;
    private int poolSize;

    public CacheBuilder() {
        maximumSize = 1000;
        expiryTime = Duration.ofDays(365);
        fetchAlgorithm = FetchAlgorithm.WRITE_THROUGH;
        evictionAlgorithm = EvictionAlgorithm.LRU;
        onStartLoad = new HashSet<>();
        poolSize = 1;
        timer = new Timer();
    }

    public CacheBuilder<KEY, VALUE> maximumSize(final int maximumSize) {
        this.maximumSize = maximumSize;
        return this;
    }

    public CacheBuilder<KEY, VALUE> expiryTime(final Duration expiryTime) {
        this.expiryTime = expiryTime;
        return this;
    }

    public CacheBuilder<KEY, VALUE> loadKeysOnStart(final Set<KEY> keys) {
        this.onStartLoad.addAll(keys);
        return this;
    }

    public CacheBuilder<KEY, VALUE> evictionAlgorithm(final EvictionAlgorithm evictionAlgorithm) {
        this.evictionAlgorithm = evictionAlgorithm;
        return this;
    }

    public CacheBuilder<KEY, VALUE> fetchAlgorithm(final FetchAlgorithm fetchAlgorithm) {
        this.fetchAlgorithm = fetchAlgorithm;
        return this;
    }

    public CacheBuilder<KEY, VALUE> dataSource(final DataSource<KEY, VALUE> dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public CacheBuilder<KEY, VALUE> timer(final Timer timer) {
        this.timer = timer;
        return this;
    }

    public CacheBuilder<KEY, VALUE> poolSize(final int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public Cache<KEY, VALUE> build() {
        if (dataSource == null) {
            throw new IllegalArgumentException("No datasource configured");
        }
        return new Cache<>(maximumSize, expiryTime, fetchAlgorithm, evictionAlgorithm, dataSource, onStartLoad, timer, poolSize);
    }
}
