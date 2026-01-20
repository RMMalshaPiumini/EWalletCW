package ds.coursework.sync;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock implements Watcher {
    private String childPath;
    private ZooKeeperClient client;
    private String lockPath;
    private boolean isAcquired = false;
    private String watchedNode;
    CountDownLatch startFlag = new CountDownLatch(1);
    CountDownLatch eventReceivedFlag;
    public static String zooKeeperUrl;
    private static String lockProcessPath = "/lp_";

    public static void setZooKeeperURL(String url) {
        zooKeeperUrl = url;
    }

    public DistributedLock(String lockName, String hostPort) throws IOException, KeeperException, InterruptedException {
        this.lockPath = "/" + lockName;
        client = new ZooKeeperClient(hostPort, 5000, this);
        startFlag.await();

        if (!client.CheckExists(lockPath)) {
            try {
                client.createNode(lockPath, false, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore if someone else created it
            }
        }
        createChildNode();
    }

    private void createChildNode() throws InterruptedException, UnsupportedEncodingException, KeeperException {
        childPath = client.createNode(lockPath + lockProcessPath, false, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public boolean acquireLock() throws KeeperException, InterruptedException {
        String smallestNode = findSmallestNodePath();
        if (smallestNode.equals(childPath)) {
            isAcquired = true;
        }
        return isAcquired;
    }

    public boolean tryAcquireLock() throws KeeperException, InterruptedException {
        // Non-blocking attempt (useful for checking "Am I the leader?")
        String smallestNode = findSmallestNodePath();
        return smallestNode.equals(childPath);
    }

    private String findSmallestNodePath() throws KeeperException, InterruptedException {
        List<String> childrenNodePaths = client.getChildrenNodePaths(lockPath);
        Collections.sort(childrenNodePaths);
        return lockPath + "/" + childrenNodePaths.get(0);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            startFlag.countDown();
        }
    }
}