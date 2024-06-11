package distributed.systems;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Handles leader election for a distributed system using Apache ZooKeeper.
 */
public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election/order_management";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    /**
     * Establishes a connection to the ZooKeeper server.
     * @throws IOException if an I/O error occurs when attempting to connect.
     */
    public void connectToZookeeper(){
        try {
            this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
            System.out.println("Connecting to Zookeeper at " + ZOOKEEPER_ADDRESS);
        } catch (IOException e) {
            System.err.println("Failed to connect to Zookeeper" + e);
        }
    }

    /**
     * Processes ZooKeeper events related to the connection state.
     *
     * @param event The watched event that occurred.
     */
    @Override
    public void process(WatchedEvent event){
        switch (event.getType()){
            case None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to Zookeeper");
                }
                else {
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }


    /**
     * Closes the ZooKeeper connection.
     *
     * @throws InterruptedException if the thread is interrupted during the process.
     */
    public void close()  {
        try {
            zooKeeper.close();
            System.out.println("ZooKeeper connection closed");
        } catch (InterruptedException e) {
            System.err.println("Failed to close ZooKeeper connection: " + e.getMessage());
        }
    }

    /**
     * Main method that keeps the application running, waiting for the connection to be closed.
     *
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    public void run() {
        synchronized (zooKeeper){
            try {
                zooKeeper.wait();
            }catch (InterruptedException e){
                System.err.println("Thread interrupted" + e.getMessage());
            }
        }
    }

    /**
     * Volunteers this instance for leadership by creating a znode in ZooKeeper.
     *
     * @throws KeeperException if there is an error interacting with ZooKeeper.
     * @throws InterruptedException if the thread is interrupted.
     */
    public void volunteerForLeadership() throws KeeperException, InterruptedException{

        try{
            String znodePrefix = ELECTION_NAMESPACE + "/c_";
            String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("znode created" + znodeFullPath);
            this.currentZnodeName = znodeFullPath.replace("/election/","");
        } catch (KeeperException | InterruptedException e) {
            System.out.println("Failed to volunteer for leadership");
            throw e;
        }
    }

    /**
     * Volunteers this instance for leadership by creating a znode in ZooKeeper.
     *
     * @throws KeeperException if there is an error interacting with ZooKeeper.
     * @throws InterruptedException if the thread is interrupted.
     */
    public void electLeader() throws  KeeperException, InterruptedException{
        try{
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if(smallestChild.equals(currentZnodeName)){
                System.out.println("I'm the leader");
                return;
            }
            System.out.println("Im not the leader," + smallestChild+" is the leader");
        } catch (KeeperException | InterruptedException e){
            System.out.println("Failed during the election process");
            throw e;
        }

    }
}
