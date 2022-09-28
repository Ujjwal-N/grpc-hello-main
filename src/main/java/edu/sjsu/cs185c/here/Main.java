package edu.sjsu.cs185c.here;

import edu.sjsu.cs185c.didyousee.*;
import edu.sjsu.cs185c.didyousee.GrpcGossip.GossipInfo;
import edu.sjsu.cs185c.didyousee.GrpcGossip.GossipRequest;
import edu.sjsu.cs185c.didyousee.GrpcGossip.GossipResponse;
import edu.sjsu.cs185c.didyousee.GrpcGossip.WhoRequest;
import edu.sjsu.cs185c.didyousee.GrpcGossip.WhoResponse;
import edu.sjsu.cs185c.didyousee.WhosHereGrpc.WhosHereImplBase;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    // Server thread manages incoming requests, updates epoch, workingList, and
    // knownAddresses
    // Client thread initializes all static variables, updates selfInfo, and sends
    // outgoing requests

    // initialized by client thread, changed by server thread as time goes on
    static AtomicInteger epoch; // both threads read and write
    static GossipInfo.Builder selfInfo; // client thread reads and writes, server thread reads

    // threadsafe lists that track information recieved by servers
    static CopyOnWriteArrayList<GossipInfo> workingList; // server thread reads and writes, client thread reads, will
                                                         // NOT have selfinfo
    static CopyOnWriteArrayList<String> knownAddresses; // server thread reads and writes, client thread reads, will NOT
                                                        // have self hostport

    @Command(name = "gossip", mixinStandardHelpOptions = true)
    static class Cli implements Callable<Integer> {
        @Parameters(index = "0", description = "human readable name")
        String name;

        @Parameters(index = "1", description = "ip address")
        String address;

        @Parameters(index = "2", description = "local port to listen on")
        int port;

        @Parameters(index = "3..*", description = "neighbor addresses")
        String[] neighborAddress;

        @Option(names = { "-e" }, description = "sets epoch to 1")
        boolean setOne;

        @Override
        public Integer call() throws Exception {
            // Some initial setup
            epoch = setOne ? new AtomicInteger(1) : new AtomicInteger(0);

            String myAddress = address + ":" + Integer.toString(port);
            selfInfo = GossipInfo.newBuilder().setName(name).setHostPort(myAddress)
                    .setEpoch(epoch.get());

            workingList = new CopyOnWriteArrayList<GossipInfo>();
            knownAddresses = new CopyOnWriteArrayList<String>();

            for (String element : neighborAddress) {
                knownAddresses.add(element);
            }

            // Will be triggered every 5 seconds
            TimerTask task = new TimerTask() {

                @Override
                public void run() {

                    // updating selfinfo with new epoch
                    selfInfo.setEpoch(epoch.get());

                    ArrayList<GossipInfo> localWorkingList = new ArrayList<GossipInfo>(workingList);
                    localWorkingList.add(selfInfo.build());
                    Collections.sort(localWorkingList, new SortByHosts());

                    // printing current list and building request
                    System.out.println("=======");
                    GrpcGossip.GossipRequest.Builder currentBuilder = GossipRequest.newBuilder();
                    int index = 0;
                    for (GossipInfo info : localWorkingList) {
                        currentBuilder.addInfo(info);
                        System.out.println(info.getEpoch() + " " + info.getHostPort() + " " + info.getName());
                    }
                    GossipRequest currentRequest = currentBuilder.build();

                    // contacting the hostports
                    ArrayList<String> localKnownAddresses = new ArrayList<String>(knownAddresses);
                    localKnownAddresses.add(myAddress);
                    Collections.sort(localKnownAddresses);

                    int myIndex = localKnownAddresses.indexOf(myAddress);
                    int total = localKnownAddresses.size();
                    index = (myIndex + 1) % total;
                    int numSuccesses = 0;
                    while (numSuccesses < 2) {
                        if (index == myIndex) { // cycled entire list
                            break;
                        }
                        var channel = ManagedChannelBuilder.forTarget(localKnownAddresses.get(index))
                                .usePlaintext()
                                .build();
                        var stub = WhosHereGrpc.newBlockingStub(channel);
                        try {
                            stub.gossip(currentRequest);
                            channel.shutdown();
                            numSuccesses += 1;
                        } catch (Exception e) {
                            epoch.set(epoch.get() + 1);
                        }
                        channel.shutdown();
                        index = (index + 1) % total;

                    }

                }
            };

            Timer timer = new Timer();
            timer.scheduleAtFixedRate(task, 5000l, 5000l);

            var server = ServerBuilder.forPort(port).addService(new WhosHereImpl()).build();
            server.start();

            server.awaitTermination();
            return 0;
        }

        class SortByHosts implements Comparator<GossipInfo> {
            // Used for sorting in ascending order of ID
            public int compare(GossipInfo a, GossipInfo b) {
                return a.getHostPort().compareTo(b.getHostPort());
            }
        }
    }

    static class WhosHereImpl extends WhosHereImplBase {

        @Override
        public void gossip(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
            // Updating working list based on current responses.
            ArrayList<GossipInfo> localWorkingList = new ArrayList<GossipInfo>(); // temporary store to avoid concurrent
                                                                                  // modification errors

            for (GossipInfo currentInfo : request.getInfoList()) {

                // No need to process my own info, I track that myself
                if (currentInfo.getHostPort().equals(selfInfo.getHostPort())) {
                    continue;
                }

                // Updating epoch if necessary
                if (currentInfo.getEpoch() > epoch.get()) {
                    epoch.set(currentInfo.getEpoch());
                }

                // handling possible overwrite
                if (knownAddresses.contains(currentInfo.getHostPort())) {

                    boolean found = false;
                    GossipInfo oldInfo = null;

                    for (GossipInfo element : workingList) {
                        if (element.getHostPort().equals(currentInfo.getHostPort())) {
                            oldInfo = element;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        localWorkingList.add(currentInfo);
                    } else {
                        if (oldInfo.getEpoch() < currentInfo.getEpoch()) {
                            localWorkingList.add(currentInfo);
                        } else {
                            localWorkingList.add(oldInfo);
                        }
                    }

                } else {
                    // new person found
                    knownAddresses.add(currentInfo.getHostPort());
                    localWorkingList.add(currentInfo);
                }
            }

            // Ensuring that servers not included in the current response are also reflected
            // on the new gossip
            for (GossipInfo oldInfo : workingList) {
                boolean contains = false;
                for (GossipInfo newInfo : localWorkingList) {
                    if (oldInfo.getHostPort().equals(newInfo.getHostPort())) {
                        contains = true;
                        break;
                    }
                }
                if (!contains) {
                    localWorkingList.add(oldInfo);
                }
            }

            workingList.clear();
            workingList.addAll(localWorkingList); // saving the processing

            // updating selfinfo with new epoch, adding self to list
            selfInfo.setEpoch(epoch.get());

            localWorkingList.add(selfInfo.build());
            Collections.sort(localWorkingList, new SortByHosts());

            // building response
            GrpcGossip.GossipResponse.Builder currentBuilder = GossipResponse.newBuilder();
            currentBuilder.addAllInfo(localWorkingList);
            responseObserver.onNext(currentBuilder.build());
            responseObserver.onCompleted();

        }

        @Override
        public void whoareyou(WhoRequest request, StreamObserver<WhoResponse> responseObserver) {
            WhoResponse.Builder builder = WhoResponse.newBuilder().setName(selfInfo.getName());
            ArrayList<GossipInfo> localWorkingList = new ArrayList<GossipInfo>(workingList);
            localWorkingList.add(selfInfo.build());
            Collections.sort(localWorkingList, new SortByHosts());
            for (GossipInfo info : workingList) {
                builder.addInfo(info);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        class SortByHosts implements Comparator<GossipInfo> {
            // Used for sorting in ascending order of ID
            public int compare(GossipInfo a, GossipInfo b) {
                return a.getHostPort().compareTo(b.getHostPort());
            }
        }

    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));

    }
}
