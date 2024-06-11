package distributed.systems;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Group project: COMP-6231_A1
 * Srujana Guttula, Student ID- 40237663
 * Taranjeet Kaur, Student ID- 40263787.
 */

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }
}
