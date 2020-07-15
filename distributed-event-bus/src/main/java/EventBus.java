import com.google.inject.Inject;
import com.google.inject.Singleton;
import exceptions.RetryLimitExceededException;
import exceptions.UnsubscribedPollException;
import lib.KeyedExecutor;
import models.Event;
import models.FailureEvent;
import models.Subscription;
import util.Timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

@Singleton
public class EventBus {
    private final Map<String, List<Event>> topics;
    private final Map<String, Map<String, Integer>> eventIndexes;
    private final Map<String, ConcurrentSkipListMap<Long, String>> eventTimestamps;
    private final Map<String, Map<String, Subscription>> pullSubscriptions;
    private final Map<String, Map<String, Subscription>> pushSubscriptions;
    private final KeyedExecutor<String> eventExecutor;
    private final KeyedExecutor<String> broadcastExecutor;
    private EventBus deadLetterQueue;
    private final Timer timer;

    @Inject
    public EventBus(final KeyedExecutor<String> eventExecutor, final KeyedExecutor<String> broadcastExecutor, final Timer timer) {
        this.topics = new ConcurrentHashMap<>();
        this.eventIndexes = new ConcurrentHashMap<>();
        this.eventTimestamps = new ConcurrentHashMap<>();
        this.pullSubscriptions = new ConcurrentHashMap<>();
        this.pushSubscriptions = new ConcurrentHashMap<>();
        this.eventExecutor = eventExecutor;
        this.broadcastExecutor = broadcastExecutor;
        this.timer = timer;
    }

    public void setDeadLetterQueue(final EventBus deadLetterQueue) {
        this.deadLetterQueue = deadLetterQueue;
    }

    public CompletionStage<Void> publish(final String topic, final Event event) {
        return eventExecutor.getThreadFor(topic, publishToBus(topic, event));
    }

    private CompletionStage<Void> publishToBus(final String topic, final Event event) {
        if (eventIndexes.containsKey(topic) && eventIndexes.get(topic).containsKey(event.getId())) {
            return null;
        }
        topics.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        eventIndexes.putIfAbsent(topic, new ConcurrentHashMap<>());
        eventIndexes.get(topic).put(event.getId(), topics.get(topic).size());
        eventTimestamps.putIfAbsent(topic, new ConcurrentSkipListMap<>());
        eventTimestamps.get(topic).put(timer.getCurrentTime(), event.getId());
        topics.get(topic).add(event);
        return notifyPushSubscribers(topic, event);
    }

    private CompletionStage<Void> notifyPushSubscribers(String topic, Event event) {
        if (!pushSubscriptions.containsKey(topic)) {
            return CompletableFuture.completedStage(null);
        }
        final var subscribersForTopic = pushSubscriptions.get(topic);
        final var notifications = subscribersForTopic.values()
                .stream()
                .filter(subscription -> subscription.getPrecondition().test(event))
                .map(subscription -> executeEventHandler(event, subscription))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(notifications);
    }

    private CompletionStage<Void> executeEventHandler(final Event event, Subscription subscription) {
        return broadcastExecutor.getThreadFor(subscription.getTopic() + subscription.getSubscriber(),
                doWithRetry(event, subscription.getEventHandler(),
                        1, subscription.getNumberOfRetries())
                        .exceptionally(throwable -> {
                            if (deadLetterQueue != null) {
                                deadLetterQueue.publish(subscription.getTopic(), new FailureEvent(event, throwable, timer.getCurrentTime()));
                            }
                            return null;
                        }));
    }

    private CompletionStage<Void> doWithRetry(final Event event,
                                              final Function<Event, CompletionStage<Void>> task,
                                              final int coolDownIntervalInMillis,
                                              final int remainingTries) {
        return task.apply(event).handle((__, throwable) -> {
            if (throwable != null) {
                if (remainingTries == 1) {
                    throw new RetryLimitExceededException(throwable);
                }
                try {
                    Thread.sleep(coolDownIntervalInMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return doWithRetry(event, task, Math.max(coolDownIntervalInMillis * 2, 10), remainingTries - 1);
            } else {
                return CompletableFuture.completedFuture((Void) null);
            }
        }).thenCompose(Function.identity());
    }


    public CompletionStage<Event> poll(final String topic, final String subscriber) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> pollBus(topic, subscriber));
    }

    private Event pollBus(final String topic, final String subscriber) {
        var subscription = pullSubscriptions.getOrDefault(topic, new HashMap<>()).get(subscriber);
        if (subscription == null) {
            throw new UnsubscribedPollException();
        }
        for (var index = subscription.getCurrentIndex(); index.intValue() < topics.get(topic).size(); index.increment()) {
            var event = topics.get(topic).get(index.intValue());
            if (subscription.getPrecondition().test(event)) {
                index.increment();
                return event;
            }
        }
        return null;
    }

    public CompletionStage<Void> subscribeToEventsAfter(final String topic, final String subscriber, final long timeStamp) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> moveIndexAtTimestamp(topic, subscriber, timeStamp));
    }

    private void moveIndexAtTimestamp(final String topic, final String subscriber, final long timeStamp) {
        final var closestEventAfter = eventTimestamps.get(topic).higherEntry(timeStamp);
        if (closestEventAfter == null) {
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventIndexes.get(topic).size());
        } else {
            final var eventIndex = eventIndexes.get(topic).get(closestEventAfter.getValue());
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventIndex);
        }
    }

    public CompletionStage<Void> subscribeToEventsAfter(final String topic, final String subscriber, final String eventId) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> moveIndexAfterEvent(topic, subscriber, eventId));
    }

    private void moveIndexAfterEvent(final String topic, final String subscriber, final String eventId) {
        if (eventId == null) {
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(0);
        } else {
            final var eventIndex = eventIndexes.get(topic).get(eventId) + 1;
            pullSubscriptions.get(topic).get(subscriber).setCurrentIndex(eventIndex);
        }
    }

    public CompletionStage<Void> subscribeForPush(final String topic,
                                                  final String subscriber,
                                                  final Predicate<Event> precondition,
                                                  final Function<Event, CompletionStage<Void>> handler,
                                                  final int numberOfRetries) {
        return eventExecutor.getThreadFor(topic + subscriber,
                () -> subscribeForPushEvents(topic, subscriber, precondition, handler, numberOfRetries));
    }

    private void subscribeForPushEvents(final String topic,
                                        final String subscriber,
                                        final Predicate<Event> precondition,
                                        final Function<Event, CompletionStage<Void>> handler,
                                        final int numberOfRetries) {
        addSubscriber(pushSubscriptions, subscriber, precondition, topic, handler, numberOfRetries);
    }

    private void addSubscriber(final Map<String, Map<String, Subscription>> pullSubscriptions,
                               final String subscriber,
                               final Predicate<Event> precondition,
                               final String topic,
                               final Function<Event, CompletionStage<Void>> handler,
                               final int numberOfRetries) {
        pullSubscriptions.putIfAbsent(topic, new ConcurrentHashMap<>());
        final var subscription = new Subscription(topic, subscriber, precondition, handler, numberOfRetries);
        subscription.setCurrentIndex(topics.getOrDefault(topic, new ArrayList<>()).size());
        pullSubscriptions.get(topic).put(subscriber, subscription);
    }

    public CompletionStage<Void> subscribeForPull(final String topic, final String subscriber, final Predicate<Event> precondition) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> subscribeForPullEvents(topic, subscriber, precondition));
    }

    private void subscribeForPullEvents(final String topic, final String subscriber, final Predicate<Event> precondition) {
        addSubscriber(pullSubscriptions, subscriber, precondition, topic, null, 0);
    }

    public CompletionStage<Void> unsubscribe(final String topic, final String subscriber) {
        return eventExecutor.getThreadFor(topic + subscriber, () -> unsubscribeFromTopic(topic, subscriber));
    }

    private void unsubscribeFromTopic(final String topic, final String subscriber) {
        pushSubscriptions.getOrDefault(topic, new HashMap<>()).remove(subscriber);
        pullSubscriptions.getOrDefault(topic, new HashMap<>()).remove(subscriber);
    }
}

