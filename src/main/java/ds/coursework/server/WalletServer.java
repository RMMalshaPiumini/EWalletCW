package ds.coursework.server;

import ds.coursework.naming.NameServiceClient;
import ds.coursework.sync.DistributedLock;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class WalletServer {
    public static void main(String[] args) {
        try {
            // Usage: java WalletServer <port> <shard_id>
            // Example: java WalletServer 50051 shard-1
            int port = 50051;
            String shardId = "shard-1";

            if (args.length >= 2) {
                port = Integer.parseInt(args[0]);
                shardId = args[1];
            }

            System.out.println("Starting Wallet Server for " + shardId + " on port " + port);

            // 1. Connect to ZooKeeper (Leader Election)
            // We use a lock name unique to this shard, e.g., "/lock-shard-1"
            String zkHost = "127.0.0.1:2181";
            DistributedLock leaderLock = new DistributedLock("lock-" + shardId, zkHost);

            // 2. Start gRPC Server
            WalletServiceImpl service = new WalletServiceImpl(leaderLock, shardId);
            Server server = ServerBuilder.forPort(port)
                    .addService(service)
                    .build()
                    .start();

            // 3. Register with etcd (Service Discovery)
            // We register as "shard-1" so clients can ask "Where is shard-1?"
            String etcdHost = "http://127.0.0.1:2379";
            NameServiceClient nameService = new NameServiceClient(etcdHost);
            nameService.registerService(shardId, "127.0.0.1", port);
            System.out.println("Registered " + shardId + " with NameService at " + etcdHost);

            // 4. Try to become Leader
            // This runs in a separate thread so it doesn't block the server startup
            new Thread(() -> {
                try {
                    System.out.println("Attempting to acquire Leadership...");
                    leaderLock.acquireLock();
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