import com.google.gson.Gson;
import exceptions.RetryLimitExceededException;
import exceptions.UnsubscribedPollException;
import lib.KeyedExecutor;
import models.Event;
import models.EventType;
import models.FailureEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import util.Timer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


// Causal ordering of topics

public class EventBusTest {
    public static final String TOPIC_1 = "topic-1";
    public static final String TOPIC_2 = "topic-2";
    public static final String PUBLISHER_1 = "publisher-1";
    public static final String SUBSCRIBER_1 = "subscriber-1";
    public static final String SUBSCRIBER_2 = "subscriber-2";
    private Timer timer;
    private KeyedExecutor<String> keyedExecutor;
    private KeyedExecutor<String> broadcastExecutor;

    @Before
    public void setUp() {
        keyedExecutor = new KeyedExecutor<>(16);
        broadcastExecutor = new KeyedExecutor<>(16);
        timer = new Timer();
    }

    private Event constructEvent(EventType priority, String description) {
        return new Event(PUBLISHER_1, priority, description, timer.getCurrentTime());
    }

    @Test
    public void defaultBehavior() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.publish(TOPIC_1, constructEvent(EventType.LOGGING, "first event"));
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, (event) -> true).toCompletableFuture().join();
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());

        eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, "second event"));
        final Event secondEvent = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();

        Assert.assertEquals(EventType.PRIORITY, secondEvent.getEventType());
        Assert.assertEquals("second event", secondEvent.getDescription());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, null).toCompletableFuture().join();
        final Event firstEvent = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();

        Assert.assertEquals(EventType.LOGGING, firstEvent.getEventType());
        Assert.assertEquals("first event", firstEvent.getDescription());
        Assert.assertEquals(PUBLISHER_1, firstEvent.getPublisher());

        final List<Event> eventCollector = new ArrayList<>();
        eventBus.subscribeForPush(TOPIC_1,
                SUBSCRIBER_2,
                (event) -> true,
                (event) -> CompletableFuture.runAsync(() -> eventCollector.add(event)),
                0).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, constructEvent(EventType.ERROR, "third event")).toCompletableFuture().join();

        Assert.assertEquals(EventType.ERROR, eventCollector.get(0).getEventType());
        Assert.assertEquals("third event", eventCollector.get(0).getDescription());

        eventBus.unsubscribe(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, constructEvent(EventType.LOGGING, "fourth event")).toCompletableFuture().join();
        Assert.assertTrue(eventBus.poll(TOPIC_1, SUBSCRIBER_1)
                .handle((__, throwable) -> throwable.getCause() instanceof UnsubscribedPollException)
                .toCompletableFuture().join());

        eventCollector.clear();
        eventBus.unsubscribe(TOPIC_1, SUBSCRIBER_2).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, constructEvent(EventType.LOGGING, "fifth event")).toCompletableFuture().join();
        Assert.assertTrue(eventCollector.isEmpty());
    }

    @Test
    public void indexMove() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, (event) -> true).toCompletableFuture().join();
        final Event firstEvent = constructEvent(EventType.PRIORITY, "first event");
        final Event secondEvent = constructEvent(EventType.PRIORITY, "second event");
        final Event thirdEvent = constructEvent(EventType.PRIORITY, "third event");
        eventBus.publish(TOPIC_1, firstEvent).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, secondEvent).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, thirdEvent).toCompletableFuture().join();

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, secondEvent.getId()).toCompletableFuture().join();
        final Event firstPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("third event", firstPoll.getDescription());
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, null).toCompletableFuture().join();
        final Event secondPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("first event", secondPoll.getDescription());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, firstEvent.getId()).toCompletableFuture().join();
        final Event thirdPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("second event", thirdPoll.getDescription());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, thirdEvent.getId()).toCompletableFuture().join();
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
    }

    @Test
    public void timestampMove() {
        final TestTimer timer = new TestTimer();
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, (event) -> true).toCompletableFuture().join();

        final Event firstEvent = new Event(PUBLISHER_1, EventType.PRIORITY, "first event", timer.getCurrentTime());
        eventBus.publish(TOPIC_1, firstEvent).toCompletableFuture().join();
        timer.setCurrentTime(timer.getCurrentTime() + Duration.ofSeconds(10).toNanos());

        final Event secondEvent = new Event(PUBLISHER_1, EventType.PRIORITY, "second event", timer.getCurrentTime());
        eventBus.publish(TOPIC_1, secondEvent).toCompletableFuture().join();
        timer.setCurrentTime(timer.getCurrentTime() + Duration.ofSeconds(10).toNanos());

        final Event thirdEvent = new Event(PUBLISHER_1, EventType.PRIORITY, "third event", timer.getCurrentTime());
        eventBus.publish(TOPIC_1, thirdEvent).toCompletableFuture().join();

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, secondEvent.getCreationTime() + Duration.ofSeconds(5).toNanos()).toCompletableFuture().join();
        final Event firstPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("third event", firstPoll.getDescription());
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, 0).toCompletableFuture().join();
        final Event secondPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("first event", secondPoll.getDescription());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, firstEvent.getCreationTime() + Duration.ofSeconds(5).toNanos()).toCompletableFuture().join();
        final Event thirdPoll = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals("second event", thirdPoll.getDescription());

        eventBus.subscribeToEventsAfter(TOPIC_1, SUBSCRIBER_1, thirdEvent.getCreationTime() + Duration.ofNanos(1).toNanos()).toCompletableFuture().join();
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
    }

    @Test
    public void idempotency() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, (event) -> true).toCompletableFuture().join();
        Event event1 = new Gson().fromJson("{\n" +
                "  \"id\": \"event-5435\",\n" +
                "  \"publisher\": \"random-publisher-1\",\n" +
                "  \"eventType\": \"LOGGING\",\n" +
                "  \"description\": \"random-event-1\",\n" +
                "  \"creationTime\": 31884739810179363\n" +
                "}", Event.class);
        eventBus.publish(TOPIC_1, event1);

        Event event2 = new Gson().fromJson("{\n" +
                "  \"id\": \"event-5435\",\n" +
                "  \"publisher\": \"random-publisher-2\",\n" +
                "  \"eventType\": \"PRIORITY\",\n" +
                "  \"description\": \"random-event-2\",\n" +
                "  \"creationTime\": 31824735510179363\n" +
                "}", Event.class);
        eventBus.publish(TOPIC_1, event2);


        final Event firstEvent = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals(EventType.LOGGING, firstEvent.getEventType());
        Assert.assertEquals("random-event-1", firstEvent.getDescription());
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
    }

    @Test
    public void unsubscribePushEvents() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        final List<Event> topic1 = new ArrayList<>(), topic2 = new ArrayList<>();
        eventBus.subscribeForPush(TOPIC_1, SUBSCRIBER_1, event -> true, event -> {
            topic1.add(event);
            return CompletableFuture.completedStage(null);
        }, 0).toCompletableFuture().join();
        eventBus.subscribeForPush(TOPIC_2, SUBSCRIBER_1, event -> true, event -> {
            topic2.add(event);
            return CompletableFuture.completedStage(null);
        }, 0).toCompletableFuture().join();

        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        eventBus.publish(TOPIC_2, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        eventBus.unsubscribe(TOPIC_1, SUBSCRIBER_1);
        Assert.assertEquals(3, topic1.size());
        Assert.assertEquals(1, topic2.size());

        for (int i = 0; i < 2; i++) {
            eventBus.publish(TOPIC_2, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        Assert.assertEquals(3, topic1.size());
        Assert.assertEquals(3, topic2.size());

        eventBus.subscribeForPush(TOPIC_1, SUBSCRIBER_1, event -> true, event -> {
            topic1.add(event);
            return CompletableFuture.completedStage(null);
        }, 0).toCompletableFuture().join();
        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        Assert.assertEquals(6, topic1.size());
        Assert.assertEquals(3, topic2.size());
    }

    @Test
    public void unsubscribePullEvents() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, event -> true).toCompletableFuture().join();
        eventBus.subscribeForPull(TOPIC_2, SUBSCRIBER_1, event -> true).toCompletableFuture().join();
        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        eventBus.publish(TOPIC_2, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();

        for (int i = 0; i < 3; i++) {
            Assert.assertNotNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
        }
        Assert.assertNotNull(eventBus.poll(TOPIC_2, SUBSCRIBER_1).toCompletableFuture().join());

        eventBus.unsubscribe(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        for (int i = 0; i < 2; i++) {
            eventBus.publish(TOPIC_2, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }
        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }

        Assert.assertTrue(eventBus.poll(TOPIC_1, SUBSCRIBER_1)
                .handle((__, throwable) -> throwable.getCause() instanceof UnsubscribedPollException).toCompletableFuture().join());
        for (int i = 0; i < 2; i++) {
            Assert.assertNotNull(eventBus.poll(TOPIC_2, SUBSCRIBER_1).toCompletableFuture().join());
        }

        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, event -> true).toCompletableFuture().join();
        for (int i = 0; i < 3; i++) {
            eventBus.publish(TOPIC_1, constructEvent(EventType.PRIORITY, UUID.randomUUID().toString())).toCompletableFuture().join();
        }

        for (int i = 0; i < 3; i++) {
            Assert.assertNotNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
        }
        Assert.assertNull(eventBus.poll(TOPIC_2, SUBSCRIBER_1).toCompletableFuture().join());
    }

    @Test
    public void deadLetterQueue() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        final EventBus dlq = new EventBus(new KeyedExecutor<>(3), new KeyedExecutor<>(3), new Timer());
        eventBus.setDeadLetterQueue(dlq);
        dlq.subscribeForPull(TOPIC_1, SUBSCRIBER_1, event -> event.getEventType().equals(EventType.ERROR));
        final AtomicLong attempts = new AtomicLong();
        final int maxTries = 5;
        eventBus.subscribeForPush(TOPIC_1, SUBSCRIBER_1, event -> true, event -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedStage(new RuntimeException());
        }, maxTries).toCompletableFuture().join();
        final Event event = new Event(PUBLISHER_1, EventType.LOGGING, "random", timer.getCurrentTime());
        eventBus.publish(TOPIC_1, event).toCompletableFuture().join();
        Assert.assertEquals(5, attempts.intValue());
        final Event failureEvent = dlq.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertTrue(failureEvent instanceof FailureEvent);
        Assert.assertEquals(event.getId(), ((FailureEvent) failureEvent).getEvent().getId());
        Assert.assertEquals(EventType.ERROR, failureEvent.getEventType());
        Assert.assertTrue(((FailureEvent) failureEvent).getThrowable().getCause() instanceof RetryLimitExceededException);
    }

    @Test
    public void retrySuccess() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        final AtomicLong attempts = new AtomicLong();
        final int maxTries = 5;
        final List<Event> events = new ArrayList<>();
        eventBus.subscribeForPush(TOPIC_1, SUBSCRIBER_1, event -> true, event -> {
            if (attempts.incrementAndGet() == maxTries) {
                events.add(event);
                return CompletableFuture.completedStage(null);
            } else {
                return CompletableFuture.failedStage(new RuntimeException("TRY no: " + attempts.intValue()));
            }
        }, maxTries).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random", timer.getCurrentTime())).toCompletableFuture().join();

        Assert.assertEquals(EventType.LOGGING, events.get(0).getEventType());
        Assert.assertEquals("random", events.get(0).getDescription());
        Assert.assertEquals(5, attempts.intValue());
        Assert.assertEquals(1, events.size());
    }

    @Test
    public void preconditionCheckForPush() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        final List<Event> events = new ArrayList<>();
        eventBus.subscribeForPush(TOPIC_1, SUBSCRIBER_1, event -> event.getDescription().contains("-1"), event -> {
            events.add(event);
            return CompletableFuture.completedStage(null);
        }, 0).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-1", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-2", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-12", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-21", timer.getCurrentTime())).toCompletableFuture().join();

        Assert.assertEquals(events.size(), 2);
        Assert.assertEquals(EventType.LOGGING, events.get(0).getEventType());
        Assert.assertEquals("random-event-1", events.get(0).getDescription());
        Assert.assertEquals(EventType.LOGGING, events.get(1).getEventType());
        Assert.assertEquals("random-event-12", events.get(1).getDescription());
    }

    @Test
    public void preconditionCheckForPull() {
        final EventBus eventBus = new EventBus(keyedExecutor, broadcastExecutor, timer);
        eventBus.subscribeForPull(TOPIC_1, SUBSCRIBER_1, event -> event.getDescription().contains("-1")).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-1", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-2", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-12", timer.getCurrentTime())).toCompletableFuture().join();
        eventBus.publish(TOPIC_1, new Event(PUBLISHER_1, EventType.LOGGING, "random-event-21", timer.getCurrentTime())).toCompletableFuture().join();

        final Event event1 = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals(EventType.LOGGING, event1.getEventType());
        Assert.assertEquals("random-event-1", event1.getDescription());
        final Event event2 = eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join();
        Assert.assertEquals(EventType.LOGGING, event2.getEventType());
        Assert.assertEquals("random-event-12", event2.getDescription());
        Assert.assertNull(eventBus.poll(TOPIC_1, SUBSCRIBER_1).toCompletableFuture().join());
    }
}
