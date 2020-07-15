package models;

import java.util.Objects;
import java.util.UUID;

public class Event {
    private final String id;
    private final String publisher;
    private final EventType eventType;
    private final String description;
    private final long creationTime;

    public Event(final String publisher,
                 final EventType eventType,
                 final String description,
                 final long creationTime) {
        this.description = description;
        this.id = UUID.randomUUID().toString();
        this.publisher = publisher;
        this.eventType = eventType;
        this.creationTime = creationTime;
    }

    public String getId() {
        return id;
    }

    public String getPublisher() {
        return publisher;
    }

    public EventType getEventType() {
        return eventType;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getDescription() {
        return description;
    }
}