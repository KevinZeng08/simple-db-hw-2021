package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-01-26
 */

//TODO deadlock detection
public class DependencyGraph {

    private List<Node> nodes;
    private Map<TransactionId, Integer> nodeIndex;

    public DependencyGraph() {
        nodes = new ArrayList<>();
        nodeIndex = new HashMap<>();
    }

    // node represents transaction
    class Node {
        TransactionId tid;
        Edge first;

        Node(TransactionId tid) {
            this.tid = tid;
        }
    }

    // edge represents the resource that tid1 waits and tid2 holds
    class Edge {
        TransactionId tid;
        PageId pid;
        Edge next;

        Edge(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }
    }

    public boolean addNode(TransactionId tid) {
        if(nodeIndex.containsKey(tid)) {
            return false;
        }
        Node node = new Node(tid);
        nodeIndex.put(tid,nodes.size());
        nodes.add(node);
        return true;
    }

    public boolean addEdge(PageId pid) {
        return false;
    }

    public boolean removeNode(TransactionId tid) {
        return false;
    }

    public boolean removeEdge(TransactionId tid, PageId pid) {
        return false;
    }

    /** Check whether there are loops in the graph */
    public boolean hasCycle() {
        return false;
    }
}
