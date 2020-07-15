package events;

import models.Record;

public class Update<K, V> extends Event<K, V> {

    private final Record<K, V> previousValue;

    public Update(Record<K, V> element, Record<K, V> previousValue, long timestamp) {
        super(element, timestamp);
        this.previousValue = previousValue;
    }
}
