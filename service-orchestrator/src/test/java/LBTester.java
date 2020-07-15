import algorithms.ConsistentHashing;
import algorithms.WeightedRoundRobin;
import models.Node;
import models.Request;
import models.Service;
import org.junit.Assert;
import org.junit.Test;

public class LBTester {
    @Test
    public void LBDefaultBehaviour() {
        LoadBalancer loadBalancer = new LoadBalancer();
        final var consistentHashing = new ConsistentHashing(point -> (long) Math.abs(point.hashCode()) % 100, 1);
        final String profileServiceId = "profile", smsServiceId = "sms", emailServiceId = "email";

        loadBalancer.register(new Service(profileServiceId, consistentHashing, new String[]{"addProfile", "deleteProfile", "updateProfile"}));
        loadBalancer.register(new Service(smsServiceId, new WeightedRoundRobin(), new String[]{"sendSms", "addTemplate", "getSMSForUser"}));
        loadBalancer.register(new Service(emailServiceId, new WeightedRoundRobin(), new String[]{"sendEmail", "addTemplate", "getSMSForUser"}));

        final Node pNode1 = new Node("51", "35.45.55.65", 2), pNode2 = new Node("22", "35.45.55.66", 3);
        loadBalancer.addNode(profileServiceId, pNode1);
        loadBalancer.addNode(profileServiceId, pNode2);

        final Node sNode1 = new Node("13", "35.45.55.67"), sNode2 = new Node("64", "35.45.55.68");
        loadBalancer.addNode(smsServiceId, sNode1);
        loadBalancer.addNode(smsServiceId, sNode2);

        final Node eNode1 = new Node("node-35", "35.45.55.69", 2), eNode2 = new Node("node-76", "35.45.55.70");
        loadBalancer.addNode(emailServiceId, eNode1);
        loadBalancer.addNode(emailServiceId, eNode2);

        var profileNode1 = loadBalancer.getHandler(new Request("r-123", profileServiceId, "addProfile"));
        var profileNode2 = loadBalancer.getHandler(new Request("r-244", profileServiceId, "addProfile"));
        var profileNode3 = loadBalancer.getHandler(new Request("r-659", profileServiceId, "addProfile"));
        var profileNode4 = loadBalancer.getHandler(new Request("r-73", profileServiceId, "addProfile"));
        Assert.assertEquals(pNode1, profileNode1);
        Assert.assertEquals(pNode1, profileNode2);
        Assert.assertEquals(pNode2, profileNode3);
        Assert.assertEquals(pNode1, profileNode4);

        loadBalancer.removeNode(profileServiceId, pNode1.getId());

        profileNode1 = loadBalancer.getHandler(new Request("r-123", profileServiceId, "addProfile"));
        profileNode2 = loadBalancer.getHandler(new Request("r-244", profileServiceId, "addProfile"));
        profileNode3 = loadBalancer.getHandler(new Request("r-659", profileServiceId, "addProfile"));
        profileNode4 = loadBalancer.getHandler(new Request("r-73", profileServiceId, "addProfile"));
        Assert.assertEquals(pNode2, profileNode1);
        Assert.assertEquals(pNode2, profileNode2);
        Assert.assertEquals(pNode2, profileNode3);
        Assert.assertEquals(pNode2, profileNode4);

        final var smsNode1 = loadBalancer.getHandler(new Request("r-124", smsServiceId, "addTemplate"));
        final var smsNode2 = loadBalancer.getHandler(new Request("r-1214", smsServiceId, "addTemplate"));
        final var smsNode3 = loadBalancer.getHandler(new Request("r-4", smsServiceId, "addTemplate"));

        Assert.assertEquals(sNode1, smsNode1);
        Assert.assertEquals(sNode2, smsNode2);
        Assert.assertEquals(sNode1, smsNode3);

        final var emailNode1 = loadBalancer.getHandler(new Request("r-1232", emailServiceId, "addTemplate"));
        final var emailNode2 = loadBalancer.getHandler(new Request("r-4134", emailServiceId, "addTemplate"));
        final var emailNode3 = loadBalancer.getHandler(new Request("r-23432", emailServiceId, "addTemplate"));
        final var emailNode4 = loadBalancer.getHandler(new Request("r-5345", emailServiceId, "addTemplate"));

        Assert.assertEquals(eNode1, emailNode1);
        Assert.assertEquals(eNode1, emailNode2);
        Assert.assertEquals(eNode2, emailNode3);
        Assert.assertEquals(eNode1, emailNode4);
    }
}
