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
import io.grpc.stub.StreamObserver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

public class Main {

    // used as identifiers in various parts of the code
    static String myName;
    static GossipInfo selfInfo;

    // threadsafe list that tracks all the GossipInfo in a given request
    static CopyOnWriteArrayList<GossipInfo> sortedList;

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

        static int epoch;
        boolean firstRequestSent = false;

        @Override
        public Integer call() throws Exception {
            // Some initial setup
            myName = name;
            if (setOne) {
                epoch = 1;
            }
            sortedList = new CopyOnWriteArrayList<GossipInfo>();

            String myAddress = address + ":" + Integer.toString(port);

            // Will be triggered every 5 seconds
            TimerTask task = new TimerTask() {

                @Override
                public void run() {

                    int myIndex = -1; // represents the index of the current node

                    for (int i = 0; i < sortedList.size(); i++) { // recalculates epoch based on current information
                        GossipInfo info = sortedList.get(i);

                        if (info.getHostPort().equals(selfInfo.getHostPort())) {
                            myIndex = i;
                            continue;
                        }
                        int otherEpoch = info.getEpoch();
                        if (otherEpoch > epoch) {
                            epoch = otherEpoch;
                        } else if (otherEpoch == 0) {
                            epoch += 1;
                        }
                    }

                    selfInfo = GossipInfo.newBuilder().setName(name).setHostPort(myAddress)
                            .setEpoch(epoch)
                            .build(); // to update epoch

                    if (myIndex != -1) {
                        sortedList.remove(myIndex); // removes current node from list, if it exists
                    }
                    sortedList.add(selfInfo); // adds back current node with new epoch
                    Collections.sort(sortedList, new SortByHosts()); // sorts list to print

                    ArrayList<String> hostAddresses = new ArrayList<String>();
                    if (setOne) { // if it is the parent server, its important to ensure
                                  // hostAddresses from commandline are added
                        for (int i = 0; i < neighborAddress.length; i++) {
                            hostAddresses.add(neighborAddress[i]);
                        }
                        firstRequestSent = true;
                    }

                    System.out.println("=======");

                    // Building request and populating addresses
                    GrpcGossip.GossipRequest.Builder currentBuilder = GossipRequest.newBuilder();
                    int index = 0;
                    for (GossipInfo info : sortedList) {
                        currentBuilder.addInfo(info);
                        System.out.println(info.getEpoch() + " " + info.getHostPort() + " " + info.getName());
                        if (!hostAddresses.contains(info.getHostPort())) {
                            hostAddresses.add(info.getHostPort());
                        }
                        index += 1;
                    }
                    GossipRequest currentRequest = currentBuilder.build();

                    Collections.sort(hostAddresses);
                    myIndex = hostAddresses.indexOf(myAddress);

                    int total = hostAddresses.size();
                    index = (myIndex + 1) % total;
                    int numSuccesses = 0;
                    while (numSuccesses < 2 && index != myIndex) {
                        try {
                            var channel = ManagedChannelBuilder.forTarget(hostAddresses.get(index))
                                    .usePlaintext()
                                    .build();
                            var stub = WhosHereGrpc.newFutureStub(channel);
                            stub.gossip(currentRequest);
                            channel.shutdown();
                            numSuccesses += 1;
                        } catch (Exception e) {
                            epoch += 1;
                        }
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
            for (GossipInfo info : request.getInfoList()) {
                boolean found = false;
                for (var i = 0; i < sortedList.size(); i++) {
                    if (info.getName().equals(sortedList.get(i).getName())) {
                        sortedList.set(i, info);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    sortedList.add(info);
                }
            }

        }

        @Override
        public void whoareyou(WhoRequest request, StreamObserver<WhoResponse> responseObserver) {
            WhoResponse.Builder builder = WhoResponse.newBuilder().setName(myName);
            for (GossipInfo info : sortedList) {
                builder.addInfo(info);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Cli()).execute(args));

    }
}
