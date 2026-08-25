package com.solace.samples.requestreply.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/**
 * One Solace broker container, shared by every integration test in the JVM.
 *
 * <h2>Why GenericContainer rather than the Testcontainers Solace module</h2>
 * The module rejects {@code default} as a client username and, more importantly, does not set the
 * two things this image needs to start at all: the {@code container=docker} environment variable
 * its platform-detection script looks for, and a shared-memory size above the Docker default.
 * Without them the broker logs "Determining platform type: [ FAIL ]" and exits with code 2. The
 * flags below are the ones verified by hand against this image.
 *
 * <h2>Why static</h2>
 * A Solace broker takes tens of seconds to become healthy. Paying that per test class would make
 * the suite something people skip, and a skipped integration suite protects nothing.
 */
public final class SolaceTestBroker {

    private static final String VPN = "default";
    private static final String USER = "default";
    private static final String PASS = "default";

    private static final GenericContainer<?> CONTAINER;
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();

    static {
        CONTAINER = new GenericContainer<>(DockerImageName.parse("solace/solace-pubsub-standard:latest"))
                .withExposedPorts(55555, 8080)
                .withEnv("container", "docker")
                .withEnv("routername", "rrtest")
                .withEnv("username_admin_globalaccesslevel", "admin")
                .withEnv("username_admin_password", "admin")
                .withEnv("system_scaling_maxconnectioncount", "1000")
                .withSharedMemorySize(2L * 1024 * 1024 * 1024)
                // The management API answering is the only honest readiness signal: the SMF port
                // opens before the message VPN is usable.
                .waitingFor(Wait.forHttp("/SEMP/v2/monitor/about/api")
                        .forPort(8080)
                        .withBasicCredentials("admin", "admin")
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        CONTAINER.start();
        awaitMessagingReady();
    }

    private SolaceTestBroker() { }

    /**
     * Waits until a client can actually log in.
     *
     * <p>SEMP answering is not readiness: the management API responds well before the message VPN
     * is operational, and connecting in that window fails with "503 Service Unavailable" subcode
     * 50. The only signal that means anything to a messaging client is a successful login, so that
     * is what this waits for.
     */
    private static void awaitMessagingReady() {
        // Generous: a loaded Docker host with other brokers already running needs it.
        long deadline = System.nanoTime() + Duration.ofMinutes(6).toNanos();
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                com.solacesystems.jcsmp.JCSMPProperties p = new com.solacesystems.jcsmp.JCSMPProperties();
                p.setProperty(com.solacesystems.jcsmp.JCSMPProperties.HOST, smfHost());
                p.setProperty(com.solacesystems.jcsmp.JCSMPProperties.VPN_NAME, VPN);
                p.setProperty(com.solacesystems.jcsmp.JCSMPProperties.USERNAME, USER);
                p.setProperty(com.solacesystems.jcsmp.JCSMPProperties.PASSWORD, PASS);
                var session = com.solacesystems.jcsmp.JCSMPFactory.onlyInstance().createSession(p);
                session.connect();
                session.closeSession();
                return;
            } catch (Exception e) {
                last = new IllegalStateException("broker not accepting logins yet", e);
                try {
                    Thread.sleep(2_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted waiting for the broker", ie);
                }
            }
        }
        throw last == null ? new IllegalStateException("broker never became ready") : last;
    }

    public static String smfHost() {
        return "tcp://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(55555);
    }

    public static String vpn() { return VPN; }

    public static String username() { return USER; }

    public static String password() { return PASS; }

    public static String sempUrl() {
        return "http://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(8080);
    }

    /**
     * Disconnects a client by name, forcing JCSMP to reconnect.
     *
     * <p>The only way to exercise reply-path recovery. {@code closeSession()} is a clean shutdown
     * rather than a reconnect, so it runs none of the interesting code.
     */
    public static int disconnectClient(String clientName) {
        String path = "/SEMP/v2/action/msgVpns/" + VPN + "/clients/"
                + java.net.URLEncoder.encode(clientName, StandardCharsets.UTF_8) + "/disconnect";
        return semp("PUT", path, "{}").statusCode();
    }

    /** Finds the broker-side client name beginning with {@code prefix}. */
    public static String findClientName(String prefix) {
        String body = semp("GET", "/SEMP/v2/monitor/msgVpns/" + VPN + "/clients?count=100", null).body();
        int i = 0;
        String needle = "\"clientName\":\"";
        while ((i = body.indexOf(needle, i)) >= 0) {
            int start = i + needle.length();
            int end = body.indexOf('"', start);
            String name = body.substring(start, end);
            if (name.startsWith(prefix)) { return name; }
            i = end;
        }
        return null;
    }

    /** A queue's partition count, or null when the queue does not exist. */
    public static Integer partitionCount(String queueName) {
        HttpResponse<String> res = semp("GET", "/SEMP/v2/config/msgVpns/" + VPN + "/queues/"
                + java.net.URLEncoder.encode(queueName, StandardCharsets.UTF_8), null);
        if (res.statusCode() == 404) { return null; }
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"partitionCount\"\\s*:\\s*(\\d+)").matcher(res.body());
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    /**
     * How many messages are sitting on a queue, read from the message list rather than a
     * counter. Spooled-message counters on a Solace queue are cumulative and lag; the message
     * listing is what actually answers "is it in there right now".
     */
    public static int queueDepth(String queueName) {
        HttpResponse<String> res = semp("GET", "/SEMP/v2/monitor/msgVpns/" + VPN + "/queues/"
                + java.net.URLEncoder.encode(queueName, StandardCharsets.UTF_8)
                + "/msgs?count=100", null);
        if (res.statusCode() / 100 != 2) { return 0; }
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"msgId\"").matcher(res.body());
        int n = 0;
        while (m.find()) { n++; }
        return n;
    }

    /** True when the DMQ holds a message that was published DMQ-eligible. */
    public static boolean queueHasDmqEligibleMsg(String queueName) {
        HttpResponse<String> res = semp("GET", "/SEMP/v2/monitor/msgVpns/" + VPN + "/queues/"
                + java.net.URLEncoder.encode(queueName, StandardCharsets.UTF_8)
                + "/msgs?count=100", null);
        return res.statusCode() / 100 == 2 && res.body().contains("\"dmqEligibleAsPublished\":true");
    }

    /**
     * A queue with a topic subscription and deliberately no consumer, for testing what happens
     * to a message nobody ever takes. {@code respectTtlEnabled} matters: SEMP defaults it to
     * false, and without it messages never expire and the test would wait forever.
     */
    public static void createUnconsumedQueue(String queueName, String topic) {
        semp("POST", "/SEMP/v2/config/msgVpns/" + VPN + "/queues",
                ("{\"queueName\":\"%s\",\"accessType\":\"non-exclusive\",\"permission\":\"consume\","
                        + "\"ingressEnabled\":true,\"egressEnabled\":true,\"respectTtlEnabled\":true,"
                        + "\"maxRedeliveryCount\":1}").formatted(queueName));
        semp("POST", "/SEMP/v2/config/msgVpns/" + VPN + "/queues/"
                        + java.net.URLEncoder.encode(queueName, StandardCharsets.UTF_8)
                        + "/subscriptions",
                "{\"subscriptionTopic\":\"%s\"}".formatted(topic));
    }

    public static void deleteQueue(String queueName) {
        semp("DELETE", "/SEMP/v2/config/msgVpns/" + VPN + "/queues/"
                + java.net.URLEncoder.encode(queueName, StandardCharsets.UTF_8), null);
    }

    public static HttpResponse<String> semp(String method, String path, String body) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(sempUrl() + path))
                    .timeout(Duration.ofSeconds(20))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + Base64.getEncoder()
                            .encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
            b.method(method, body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body));
            return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IllegalStateException("SEMP " + method + " " + path + " failed", e);
        }
    }
}
