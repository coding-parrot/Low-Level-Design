package models;

public class Record<KEY, VALUE> {
    private final KEY key;
    private final VALUE value;
    private final long insertionTime;
    private AccessDetails accessDetails;

    public Record(KEY key, VALUE value, long insertionTime) {
        this.key = key;
        this.value = value;
        this.insertionTime = insertionTime;
        this.accessDetails = new AccessDetails(insertionTime);
    }

    public KEY getKey() {
        return key;
    }

    public VALUE getValue() {
        return value;
    }

    public long getInsertionTime() {
        return insertionTime;
    }

    public AccessDetails getAccessDetails() {
        return accessDetails;
    }

    public void setAccessDetails(final AccessDetails accessDetails) {
        this.accessDetails = accessDetails;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + key +
                ", value=" + value +
                ", insertionTime=" + insertionTime +
                ", accessDetails=" + accessDetails +
                '}';
    }
}

