package models;

public class Request {
    private final String id;

    private final String serviceId;
    private final String method;

    public Request(String id, String serviceId, String method) {
        this.id = id;
        this.serviceId = serviceId;
        this.method = method;
    }

    public String getId() {
        return id;
    }

    public String getServiceId() {
        return serviceId;
    }

    public String getMethod() {
        return method;
    }
}
