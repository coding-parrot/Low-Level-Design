package models;

import java.util.Objects;

public class Request {
    private final String requestId;
    private final long startTime;

    public Request(String requestId, long startTime) {
        this.requestId = requestId;
        this.startTime = startTime;
    }

    public String getRequestId() {
        return requestId;
    }

    public long getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return requestId.equals(((Request) o).requestId);
    }

    @Override
    public int hashCode() {
        return requestId.hashCode();
    }
}
