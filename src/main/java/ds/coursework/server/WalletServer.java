package ds.coursework.server;

import ds.coursework.naming.NameServiceClient;
import ds.coursework.sync.DistributedLock;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class WalletServer {
    public static void main(String[] args) {
        try {
            int port = 50051;
            String shardId = "shard-1";

            if (args.length >= 2) {
                port = Integer.parseInt(args[0]);
                shardId = args[1];
            }

            System.out.println("Starting Wallet Server for " + shardId + " on port " + port);

            String zkHost = "127.0.0.1:2181";
            String etcdHost = "http://127.0.0.1:2379";

            DistributedLock leaderLock = new DistributedLock("lock-" + shardId, zkHost);
            NameServiceClient nameService = new NameServiceClient(etcdHost);

            // Pass 'nameService' to the implementation so it can find other shards
            WalletServiceImpl service = new WalletServiceImpl(leaderLock, shardId, nameService);

            Server server = ServerBuilder.forPort(port)
                    .addService(service)
                    .build()
                    .start();

            nameService.registerService(shardId, "127.0.0.1", port);
            System.out.println("Registered " + shardId + " with NameService at " + etcdHost);

            new Thread(() -> {
                try {
                    System.out.println("Attempting to acquire Leadership...");
                    while (!leaderLock.acquireLock()) {
                        Thread.sleep(2000); // Check every 2 seconds
                    }
                    System.out.println("I AM THE LEADER now. Accepting writes.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            server.awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}