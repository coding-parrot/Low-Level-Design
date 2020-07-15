package events;

import models.Record;

import java.util.UUID;

public abstract class Event<KEY, VALUE> {
    private final String id;
    private final Record<KEY, VALUE> element;
    private final long timestamp;

    public Event(Record<KEY, VALUE> element, long timestamp) {
        this.element = element;
        this.timestamp = timestamp;
        id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    public Record<KEY, VALUE> getElement() {
        return element;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "element=" + element +
                ", timestamp=" + timestamp +
                "}\n";
    }
}
