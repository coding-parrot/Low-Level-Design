import exceptions.RateLimitExceededException;
import models.Request;
import utils.Timer;

import java.util.Map;
import java.util.concurrent.*;

public class TimerWheel {
    private final int timeOutPeriod;
    private final int capacityPerSlot;
    private final TimeUnit timeUnit;
    private final ArrayBlockingQueue<Request>[] slots;
    private final Map<String, Integer> reverseIndex;
    private final Timer timer;
    private final ExecutorService[] threads;

    public TimerWheel(final TimeUnit timeUnit,
                      final int timeOutPeriod,
                      final int capacityPerSlot,
                      final Timer timer) {
        this.timeUnit = timeUnit;
        this.timeOutPeriod = timeOutPeriod;
        this.capacityPerSlot = capacityPerSlot;
        if (this.timeOutPeriod > 1000) {
            throw new IllegalArgumentException();
        }
        this.slots = new ArrayBlockingQueue[this.timeOutPeriod];
        this.threads = new ExecutorService[this.timeOutPeriod];
        this.reverseIndex = new ConcurrentHashMap<>();
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new ArrayBlockingQueue<>(capacityPerSlot);
            threads[i] = Executors.newSingleThreadExecutor();
        }
        this.timer = timer;
        final long timePerSlot = TimeUnit.MILLISECONDS.convert(1, timeUnit);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::flushRequests,
                        timePerSlot - (this.timer.getCurrentTimeInMillis() % timePerSlot),
                        timePerSlot, TimeUnit.MILLISECONDS);
    }

    public Future<?> flushRequests() {
        final int currentSlot = getCurrentSlot();
        return threads[currentSlot].submit(() -> {
            for (final Request request : slots[currentSlot]) {
                if (timer.getCurrentTime(timeUnit) - request.getStartTime() >= timeOutPeriod) {
                    slots[currentSlot].remove(request);
                    reverseIndex.remove(request.getRequestId());
                }
            }
        });
    }

    public Future<?> addRequest(final Request request) {
        final int currentSlot = getCurrentSlot();
        return threads[currentSlot].submit(() -> {
            if (slots[currentSlot].size() >= capacityPerSlot) {
                throw new RateLimitExceededException();
            }
            slots[currentSlot].add(request);
            reverseIndex.put(request.getRequestId(), currentSlot);
        });
    }

    public Future<?> evict(final String requestId) {
        final int currentSlot = reverseIndex.get(requestId);
        return threads[currentSlot].submit(() -> {
            slots[currentSlot].remove(new Request(requestId, 0));
            reverseIndex.remove(requestId);
        });
    }

    private int getCurrentSlot() {
        return (int) timer.getCurrentTime(timeUnit) % slots.length;
    }
}

