package models;

import java.util.Objects;

public class Node {
    private final String id;
    private final int weight;
    private final String ipAddress;

    public Node(String id, String ipAddress) {
        this(id, ipAddress, 1);
    }

    public Node(String id, String ipAddress, int weight) {
        this.id = id;
        this.weight = weight;
        this.ipAddress = ipAddress;
    }

    public String getId() {
        return id;
    }

    public int getWeight() {
        return weight;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return id.equals(node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

}
