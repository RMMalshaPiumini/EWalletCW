package ds.coursework.server;

import ds.coursework.grpc.generated.*;
import ds.coursework.naming.NameServiceClient;
import ds.coursework.sync.DistributedLock;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WalletServiceImpl extends WalletServiceGrpc.WalletServiceImplBase {
    private final Map<String, Double> accountBalances = new ConcurrentHashMap<>();
    private final DistributedLock leaderLock;
    private final String shardId;
    private final NameServiceClient nameService; // Needed to find other shards

    public WalletServiceImpl(DistributedLock leaderLock, String shardId, NameServiceClient nameService) {
        this.leaderLock = leaderLock;
        this.shardId = shardId;
        this.nameService = nameService;
    }

    @Override
    public void createAccount(CreateAccountRequest request, StreamObserver<CreateAccountResponse> responseObserver) {
        try {
            if (!leaderLock.tryAcquireLock()) {
                sendError(responseObserver, "I am not the Leader.");
                return;
            }
            // Enforce that this server ONLY creates accounts for its own shard
            String accountId = shardId + "-" + request.getUserId();

            if (accountBalances.containsKey(accountId)) {
                sendError(responseObserver, "Account already exists.");
                return;
            }
            accountBalances.put(accountId, request.getInitialBalance());
            System.out.println("Created Account: " + accountId + " with balance " + request.getInitialBalance());

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
        Double balance = accountBalances.get(request.getAccountId());
        if (balance == null) {
            // If not found locally, return 0 (or error)
            responseObserver.onNext(BalanceResponse.newBuilder().setBalance(0.0).build());
        } else {
            responseObserver.onNext(BalanceResponse.newBuilder().setBalance(balance).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void deposit(DepositRequest request, StreamObserver<DepositResponse> responseObserver) {
        // This method is called by OTHER servers to move money in
        try {
            if (!leaderLock.tryAcquireLock()) {
                sendError(responseObserver, "I am not the Leader.");
                return;
            }

            String accountId = request.getAccountId();
            double amount = request.getAmount();

            synchronized (accountBalances) {
                Double currentBalance = accountBalances.get(accountId);
                if (currentBalance == null) {
                    // Account doesn't exist here!
                    responseObserver.onNext(DepositResponse.newBuilder().setSuccess(false).setErrorMessage("Account not found on " + shardId).build());
                } else {
                    accountBalances.put(accountId, currentBalance + amount);
                    System.out.println("External Deposit: Added " + amount + " to " + accountId);
                    responseObserver.onNext(DepositResponse.newBuilder().setSuccess(true).build());
                }
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            sendError(responseObserver, e.getMessage());
        }
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

            // Check if the destination is on THIS shard
            boolean isLocalTransfer = toId.startsWith(shardId + "-");

            if (isLocalTransfer) {
                // --- SAME SHARD TRANSFER (Easy) ---
                synchronized (accountBalances) {
                    Double fromBal = accountBalances.get(fromId);
                    Double toBal = accountBalances.get(toId);

                    if (fromBal == null || toBal == null) {
                        sendError(responseObserver, "One or both accounts do not exist.");
                        return;
                    }
                    if (fromBal < amount) {
                        sendError(responseObserver, "Insufficient funds.");
                        return;
                    }
                    accountBalances.put(fromId, fromBal - amount);
                    accountBalances.put(toId, toBal + amount);
                }
            } else {
                // --- CROSS-PARTITION TRANSFER (Complex) ---
                System.out.println("Initiating Cross-Partition Transfer to " + toId);

                // 1. Deduct locally (Pessimistic Lock)
                synchronized (accountBalances) {
                    Double fromBal = accountBalances.get(fromId);
                    if (fromBal == null || fromBal < amount) {
                        sendError(responseObserver, "Insufficient funds or account not found.");
                        return;
                    }
                    accountBalances.put(fromId, fromBal - amount);
                }

                // 2. Call Remote Shard to Deposit
                boolean remoteSuccess = false;
                String remoteError = "";
                try {
                    String targetShard = toId.split("-")[0] + "-" + toId.split("-")[1]; // extract "shard-2"
                    remoteSuccess = callRemoteDeposit(targetShard, toId, amount);
                } catch (Exception e) {
                    remoteError = e.getMessage();
                }

                // 3. Handle Result
                if (remoteSuccess) {
                    System.out.println("Cross-Partition Transfer Successful.");
                } else {
                    // COMPENSATING TRANSACTION (Rollback)
                    System.out.println("Remote deposit failed (" + remoteError + "). Refunding " + fromId);
                    synchronized (accountBalances) {
                        Double current = accountBalances.get(fromId);
                        accountBalances.put(fromId, current + amount);
                    }
                    sendError(responseObserver, "Remote transfer failed: " + remoteError);
                    return;
                }
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

    // Helper to call another server
    private boolean callRemoteDeposit(String targetShardId, String accountId, double amount) {
        try {
            NameServiceClient.ServiceDetails details = nameService.findService(targetShardId);
            if (details == null) return false;

            ManagedChannel channel = ManagedChannelBuilder.forAddress(details.ip, details.port)
                    .usePlaintext()
                    .build();
            WalletServiceGrpc.WalletServiceBlockingStub stub = WalletServiceGrpc.newBlockingStub(channel);

            DepositResponse response = stub.deposit(DepositRequest.newBuilder()
                    .setAccountId(accountId)
                    .setAmount(amount)
                    .build());

            channel.shutdown();
            return response.getSuccess();
        } catch (Exception e) {
            System.out.println("Remote call failed: " + e.getMessage());
            return false;
        }
    }

    private void sendError(StreamObserver<?> observer, String msg) {
        observer.onError(io.grpc.Status.INTERNAL.withDescription(msg).asRuntimeException());
    }
}