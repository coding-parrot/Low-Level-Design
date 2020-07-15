package models;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;

public class Subscription {
    private final String topic;
    private final String subscriber;
    private final Predicate<Event> precondition;
    private final Function<Event, CompletionStage<Void>> eventHandler;
    private final int numberOfRetries;
    private final LongAdder currentIndex;

    public Subscription(final String topic,
                        final String subscriber,
                        final Predicate<Event> precondition,
                        final Function<Event, CompletionStage<Void>> eventHandler,
                        final int numberOfRetries) {
        this.topic = topic;
        this.subscriber = subscriber;
        this.precondition = precondition;
        this.eventHandler = eventHandler;
        this.currentIndex = new LongAdder();
        this.numberOfRetries = numberOfRetries;
    }

    public String getTopic() {
        return topic;
    }

    public String getSubscriber() {
        return subscriber;
    }

    public Predicate<Event> getPrecondition() {
        return precondition;
    }

    public Function<Event, CompletionStage<Void>> getEventHandler() {
        return eventHandler;
    }

    public LongAdder getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(final int offset) {
        currentIndex.reset();
        currentIndex.add(offset);
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }
}
