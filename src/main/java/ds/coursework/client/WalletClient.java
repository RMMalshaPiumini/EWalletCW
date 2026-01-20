package ds.coursework.client;

import ds.coursework.grpc.generated.*;
import ds.coursework.naming.NameServiceClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

public class WalletClient {
    private NameServiceClient nameService;

    public WalletClient(String etcdUrl) {
        this.nameService = new NameServiceClient(etcdUrl);
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("--- Distributed E-Wallet Client ---");
        System.out.println("Commands:");
        System.out.println("  create <shard_id> <user_id> <balance>  (e.g., create shard-1 bob 1000)");
        System.out.println("  balance <account_id>                   (e.g., balance shard-1-bob)");
        System.out.println("  transfer <from_id> <to_id> <amount>    (e.g., transfer shard-1-bob shard-1-alice 50)");
        System.out.println("  exit");

        while (true) {
            System.out.print("\n> ");
            String line = scanner.nextLine();
            String[] parts = line.split(" ");
            String cmd = parts[0];

            try {
                if (cmd.equalsIgnoreCase("exit")) break;

                else if (cmd.equalsIgnoreCase("create")) {
                    if (parts.length != 4) { System.out.println("Usage: create <shard_id> <user_id> <balance>"); continue; }
                    handleCreate(parts[1], parts[2], Double.parseDouble(parts[3]));

                } else if (cmd.equalsIgnoreCase("balance")) {
                    if (parts.length != 2) { System.out.println("Usage: balance <account_id>"); continue; }
                    handleBalance(parts[1]);

                } else if (cmd.equalsIgnoreCase("transfer")) {
                    if (parts.length != 4) { System.out.println("Usage: transfer <from_id> <to_id> <amount>"); continue; }
                    handleTransfer(parts[1], parts[2], Double.parseDouble(parts[3]));

                } else {
                    System.out.println("Unknown command.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void handleCreate(String shardId, String userId, double balance) throws Exception {
        // 1. Find the server for this shard
        WalletServiceGrpc.WalletServiceBlockingStub stub = getStubForShard(shardId);

        // 2. Call the server
        CreateAccountResponse response = stub.createAccount(CreateAccountRequest.newBuilder()
                .setUserId(userId)
                .setInitialBalance(balance)
                .build());

        if (response.getSuccess()) {
            System.out.println("SUCCESS! Account ID: " + response.getAccountId());
        } else {
            System.out.println("FAILED: " + response.getErrorMessage());
        }
    }

    private void handleBalance(String accountId) throws Exception {
        // 1. Extract shard from AccountID (Format: shard-1-bob)
        String shardId = extractShardId(accountId);
        WalletServiceGrpc.WalletServiceBlockingStub stub = getStubForShard(shardId);

        // 2. Call server
        BalanceResponse response = stub.getBalance(BalanceRequest.newBuilder().setAccountId(accountId).build());
        System.out.println("Balance for " + accountId + ": " + response.getBalance());
    }

    private void handleTransfer(String fromId, String toId, double amount) throws Exception {
        // NOTE: This simple version assumes both users are on the SAME shard.
        // For inter-shard transfer, you would need more complex logic (Coursework Requirement).
        String fromShard = extractShardId(fromId);
        String toShard = extractShardId(toId);

        if (!fromShard.equals(toShard)) {
            System.out.println("WARNING: Inter-shard transfer (Sender=" + fromShard + ", Receiver=" + toShard + ") is complex.");
            System.out.println("Attempting to send request to Sender's shard...");
        }

        WalletServiceGrpc.WalletServiceBlockingStub stub = getStubForShard(fromShard);
        TransferResponse response = stub.transferFund(TransferRequest.newBuilder()
                .setFromAccountId(fromId)
                .setToAccountId(toId)
                .setAmount(amount)
                .build());

        if (response.getSuccess()) {
            System.out.println("Transfer SUCCESS! TxID: " + response.getTransactionId());
        } else {
            System.out.println("Transfer FAILED: " + response.getErrorMessage());
        }
    }

    // Helper: Ask etcd for the IP/Port of a shard, then connect
    private WalletServiceGrpc.WalletServiceBlockingStub getStubForShard(String shardId) throws Exception {
        NameServiceClient.ServiceDetails details = nameService.findService(shardId);
        if (details == null) {
            throw new RuntimeException("Shard '" + shardId + "' not found in Name Service (etcd). Is it running?");
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress(details.ip, details.port)
                .usePlaintext()
                .build();
        return WalletServiceGrpc.newBlockingStub(channel);
    }

    // Helper: Assumes format "shard-1-username" -> returns "shard-1"
    private String extractShardId(String accountId) {
        int lastHyphen = accountId.lastIndexOf('-');
        if (lastHyphen == -1) return "shard-1"; // Default fallback
        return accountId.substring(0, lastHyphen);
    }

    public static void main(String[] args) {
        String etcdUrl = "http://127.0.0.1:2379";
        new WalletClient(etcdUrl).start();
    }
}