package events;

import models.Record;

public class Eviction<K, V> extends Event<K, V> {
    private final Type type;

    public Eviction(Record<K, V> element, Type type, long timestamp) {
        super(element, timestamp);
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        EXPIRY, REPLACEMENT
    }

    @Override
    public String toString() {
        return "Eviction{" +
                "type=" + type +
                ", "+super.toString() +
                "}\n";
    }
}
