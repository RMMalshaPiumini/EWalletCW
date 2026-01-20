package ds.coursework.server;

import ds.coursework.grpc.generated.*;
import ds.coursework.sync.DistributedLock;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WalletServiceImpl extends WalletServiceGrpc.WalletServiceImplBase {
    // In-memory database to store AccountID -> Balance
    private final Map<String, Double> accountBalances = new ConcurrentHashMap<>();
    private final DistributedLock leaderLock;
    private final String shardId;

    public WalletServiceImpl(DistributedLock leaderLock, String shardId) {
        this.leaderLock = leaderLock;
        this.shardId = shardId;
    }

    @Override
    public void createAccount(CreateAccountRequest request, StreamObserver<CreateAccountResponse> responseObserver) {
        // 1. Check if we are the Leader (Only Leader can create accounts)
        try {
            if (!leaderLock.tryAcquireLock()) {
                sendError(responseObserver, "I am not the Leader. Writes must go to the Primary.");
                return;
            }

            // 2. Create a unique ID (e.g., "Shard1-User123")
            String accountId = shardId + "-" + request.getUserId();

            // 3. Store it (Atomically)
            if (accountBalances.containsKey(accountId)) {
                sendError(responseObserver, "Account already exists.");
                return;
            }
            accountBalances.put(accountId, request.getInitialBalance());
            System.out.println("Created Account: " + accountId + " with balance " + request.getInitialBalance());

            // 4. Respond
            CreateAccountResponse response = CreateAccountResponse.newBuilder()
                    .setAccountId(accountId)
                    .setAssignedShardId(shardId)
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            sendError(responseObserver, e.getMessage());
        }
    }

    @Override
    public void getBalance(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        // Reads can happen on any node (Leader or Follower) - "Read your writes" handled by client sticking to shard
        Double balance = accountBalances.get(request.getAccountId());

        if (balance == null) {
            // Standard gRPC error could be used here, but returning 0 for simplicity
            responseObserver.onNext(BalanceResponse.newBuilder().setBalance(0.0).build());
        } else {
            responseObserver.onNext(BalanceResponse.newBuilder().setBalance(balance).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void transferFund(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
        try {
            if (!leaderLock.tryAcquireLock()) {
                sendError(responseObserver, "I am not the Leader.");
                return;
            }

            String fromId = request.getFromAccountId();
            String toId = request.getToAccountId();
            double amount = request.getAmount();

            // CRITICAL SECTION: Atomicity
            // We lock the whole map logic for this transaction to ensure safety
            synchronized (accountBalances) {
                Double fromBalance = accountBalances.get(fromId);
                Double toBalance = accountBalances.get(toId);

                if (fromBalance == null || toBalance == null) {
                    sendError(responseObserver, "One or both accounts do not exist.");
                    return;
                }

                if (fromBalance < amount) {
                    sendError(responseObserver, "Insufficient funds.");
                    return;
                }

                // Perform the swap
                accountBalances.put(fromId, fromBalance - amount);
                accountBalances.put(toId, toBalance + amount);
                System.out.println("Transferred " + amount + " from " + fromId + " to " + toId);
            }

            TransferResponse response = TransferResponse.newBuilder()
                    .setSuccess(true)
                    .setTransactionId(UUID.randomUUID().toString())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            sendError(responseObserver, e.getMessage());
        }
    }

    private void sendError(StreamObserver<?> observer, String msg) {
        observer.onError(io.grpc.Status.INTERNAL.withDescription(msg).asRuntimeException());
    }
}