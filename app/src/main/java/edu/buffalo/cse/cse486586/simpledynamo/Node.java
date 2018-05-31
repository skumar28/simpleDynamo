package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by Sandeep on 5/5/2018.
 */

public class Node {
     String nodeId;
     String port;
     Node next;
     Node prev;

    Node(String key, String port){
        this.nodeId = key;
        this.port = port;
        this.next = null;
        this.prev = null;
    }
}
