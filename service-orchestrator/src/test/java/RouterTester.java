import algorithms.ConsistentHashing;
import algorithms.Router;
import algorithms.WeightedRoundRobin;
import models.Node;
import models.Request;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class RouterTester {
    String ipAddress = "127.0.0.1", serviceId = "service", method = "method";

    @Test
    public void defaultRoundRobin() {
        final Router router = new WeightedRoundRobin();
        final Node node1 = newNode("node-1"), node2 = newNode("node-2"), node3 = newNode("node-3");
        router.addNode(node1);
        router.addNode(node2);
        router.addNode(node3);

        Assert.assertEquals(node1, router.getAssignedNode(newRequest("r-123")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("r-124")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("r-125")));

        router.removeNode(node1);

        Assert.assertEquals(node2, router.getAssignedNode(newRequest("r-125")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("r-126")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("r-127")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("r-128")));

        final Node node4 = new Node("node-4", ipAddress, 2);
        router.addNode(node4);

        Assert.assertEquals(node4, router.getAssignedNode(newRequest("r-129")));
        Assert.assertEquals(node4, router.getAssignedNode(newRequest("r-130")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("r-131")));
    }

    @Test
    public void defaultConsistentHashing() {
        final List<Long> hashes = new ArrayList<>();
        hashes.add(1L);
        hashes.add(11L);
        hashes.add(21L);
        hashes.add(31L);
        final Function<String, Long> hashFunction = id -> {
            if (id.contains("000000")) {
                return hashes.remove(0);
            } else {
                return Long.parseLong(id);
            }
        };
        final Router router = new ConsistentHashing(hashFunction, 1);
        final Node node1 = newNode("1000000"), node2 = newNode("2000000"), node3 = newNode("3000000");
        router.addNode(node1);
        router.addNode(node2);
        router.addNode(node3);

        Assert.assertEquals(node1, router.getAssignedNode(newRequest("35")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("5")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("15")));

        router.removeNode(node1);

        Assert.assertEquals(node2, router.getAssignedNode(newRequest("22")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("12")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("23")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("13")));

        final Node node4 = newNode("4000000");
        router.addNode(node4);

        Assert.assertEquals(node4, router.getAssignedNode(newRequest("25")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void consistentHashingConstruction() {
        new ConsistentHashing(Long::valueOf, 0);
    }

    @Test
    public void consistentHashingWithWeights() {
        final List<Long> hashes = new ArrayList<>();
        hashes.add(1L); // remaining is node 1
        hashes.add(21L); // 12 to 21 is for node 1
        hashes.add(11L); // 2 to 11 is for node 2
        hashes.add(41L); // 32 to 41 is for node 2
        hashes.add(31L); // 22 to 31 is for node 3
        hashes.add(51L); // 42 to 51 is for node 3 --> 10 points
        final Function<String, Long> hashFunction = id -> {
            //range should be (0, 60)
            if (id.contains("000000")) {
                return hashes.remove(0);
            } else {
                return Long.parseLong(id);
            }
        };
        final Router router = new ConsistentHashing(hashFunction, 2);
        final Node node1 = newNode("1000000"), node2 = newNode("2000000"), node3 = newNode("3000000");
        router.addNode(node1);
        router.addNode(node2);
        router.addNode(node3);

        Assert.assertEquals(node1, router.getAssignedNode(newRequest("55")));
        Assert.assertEquals(node1, router.getAssignedNode(newRequest("15")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("8")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("33")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("28")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("47")));

        router.removeNode(node1);
        // remaining is node 2
        // 12 to 21 is now for node 3
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("58")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("12")));
        Assert.assertEquals(node3, router.getAssignedNode(newRequest("23")));
        Assert.assertEquals(node2, router.getAssignedNode(newRequest("54")));

        final Node node4 = newNode("4000000");
        hashes.add(6L); // 0 to 6 is for node 4, 52 to remaining is for node 4
        hashes.add(26L); // 12 to 26 is for node 4
        router.addNode(node4);

        Assert.assertEquals(node4, router.getAssignedNode(newRequest("15")));
        Assert.assertEquals(node4, router.getAssignedNode(newRequest("59")));
        Assert.assertEquals(node4, router.getAssignedNode(newRequest("5")));
    }

    private Request newRequest(String s) {
        return new Request(s, serviceId, method);
    }

    private Node newNode(String s) {
        return new Node(s, ipAddress);
    }
}
