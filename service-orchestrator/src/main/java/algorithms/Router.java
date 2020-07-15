package algorithms;

import models.Node;
import models.Request;

public interface Router {
    void addNode(Node node);

    void removeNode(Node node);

    Node getAssignedNode(Request request);
}
