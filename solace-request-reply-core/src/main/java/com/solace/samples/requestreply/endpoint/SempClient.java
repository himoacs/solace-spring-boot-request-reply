package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.exception.EndpointProvisioningException;
import com.solace.samples.requestreply.exception.PartitionMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal SEMP v2 client, used for the one thing the messaging API cannot do: partition count.
 *
 * <p>Creating a partitioned queue is safe to automate — the queue is empty. <b>Changing</b> one
 * is not: Solace requires draining before altering partition count, and a decrease deletes the
 * messages held in removed partitions. An application that patched this on every startup could
 * destroy queued bookings during a routine redeploy, so a mismatch is fatal by default.
 *
 * <p>These are management credentials, unlike the messaging ones the rest of the library uses.
 * In production this bootstrap belongs in an init container or Terraform, leaving the running
 * application on {@code VALIDATE} with no elevated access at all.
 *
 * <p>Hand-rolled over {@code java.net.http} rather than pulling in a REST client: this is two
 * calls, and a sample should not acquire a dependency for that.
 */
public class SempClient {

    private static final Logger log = LoggerFactory.getLogger(SempClient.class);
    private static final Pattern PARTITION_COUNT = Pattern.compile("\"partitionCount\"\\s*:\\s*(\\d+)");

    private final SolaceRequestReplyProperties.Partitioning cfg;
    private final HttpClient http;

    public SempClient(SolaceRequestReplyProperties.Partitioning cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** Creates the queue partitioned, or verifies an existing one matches. */
    public void ensurePartitionedQueue(String queueName, int desired) {
        requireConfigured(queueName);
        Integer actual = readPartitionCount(queueName);

        if (actual == null) {
            createPartitioned(queueName, desired);
            return;
        }
        if (actual == desired) {
            log.info("Queue '{}' already partitioned with partitionCount={}", queueName, desired);
            return;
        }
        if (!cfg.isAllowPartitionResize()) {
            throw new PartitionMismatchException(queueName, desired, actual);
        }
        log.warn("Resizing partitionCount on '{}' from {} to {}. Messages held in removed "
                        + "partitions are DELETED by the broker, and Solace recommends draining "
                        + "the queue first. Proceeding because allow-partition-resize=true.",
                queueName, actual, desired);
        patch(queueName, "{\"partitionCount\":" + desired + "}");
    }

    private void requireConfigured(String queueName) {
        if (cfg.getSemp().getUrl() == null || cfg.getSemp().getUrl().isBlank()) {
            throw new EndpointProvisioningException(queueName,
                    "partition-count > 0 requires SEMP access, but "
                            + "solace.request-reply.replier.partitioning.semp.url is not set. "
                            + "JCSMP cannot provision a partitioned queue: EndpointProperties has no "
                            + "partition member at any version. Either configure SEMP, create the "
                            + "queue out of band, or set partition-count=0 for a flat queue.");
        }
    }

    private Integer readPartitionCount(String queueName) {
        HttpResponse<String> res = send("GET", queuePath(queueName), null);
        if (isNotFound(res)) { return null; }
        expectSuccess(res, queueName, "read");
        Matcher m = PARTITION_COUNT.matcher(res.body());
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    private void createPartitioned(String queueName, int desired) {
        String body = ("{\"queueName\":\"%s\",\"accessType\":\"non-exclusive\",\"partitionCount\":%d,"
                + "\"partitionRebalanceDelay\":%d,\"partitionRebalanceMaxHandoffTime\":%d,"
                + "\"ingressEnabled\":true,\"egressEnabled\":true,\"permission\":\"consume\"}")
                .formatted(queueName, desired,
                        cfg.getRebalanceDelay().toSeconds(), cfg.getRebalanceMaxHandoffTime().toSeconds());
        HttpResponse<String> res = send("POST", collectionPath(), body);
        expectSuccess(res, queueName, "create");
        log.info("Created partitioned queue '{}' via SEMP: partitionCount={} rebalanceDelay={}s "
                        + "maxHandoff={}s", queueName, desired,
                cfg.getRebalanceDelay().toSeconds(), cfg.getRebalanceMaxHandoffTime().toSeconds());
    }

    private void patch(String queueName, String body) {
        expectSuccess(send("PATCH", queuePath(queueName), body), queueName, "patch");
    }

    private String collectionPath() {
        return "/SEMP/v2/config/msgVpns/" + enc(cfg.getSemp().getMsgVpn()) + "/queues";
    }

    private String queuePath(String queueName) {
        return collectionPath() + "/" + enc(queueName);
    }

    private HttpResponse<String> send(String method, String path, String body) {
        String base = cfg.getSemp().getUrl().replaceAll("/+$", "");
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .header("Authorization", basicAuth());
        HttpRequest.BodyPublisher pub = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
        // Some HTTP stacks refuse PATCH; SEMP accepts the documented override header.
        if ("PATCH".equals(method)) {
            b.header("X-Http-Method-Override", "PATCH").POST(pub);
        } else {
            b.method(method, pub);
        }
        try {
            return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new EndpointProvisioningException(path, "SEMP " + method + " failed", e);
        }
    }

    private String basicAuth() {
        String raw = cfg.getSemp().getUsername() + ":" + cfg.getSemp().getPassword();
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * SEMP signals a missing object with <b>HTTP 400</b> and {@code "status":"NOT_FOUND"} in the
     * body, not with 404. Checking only the status code makes "create if missing" never fire, and
     * turns a routine first run into a startup failure.
     */
    private static boolean isNotFound(HttpResponse<String> res) {
        if (res.statusCode() == 404) { return true; }
        return res.statusCode() / 100 == 4
                && res.body() != null
                && res.body().contains("\"status\":\"NOT_FOUND\"");
    }

    private static void expectSuccess(HttpResponse<String> res, String queueName, String what) {
        if (res.statusCode() / 100 != 2) {
            throw new EndpointProvisioningException(queueName,
                    "SEMP " + what + " returned HTTP " + res.statusCode() + ": " + abbreviate(res.body()));
        }
    }

    private static String abbreviate(String s) {
        if (s == null) { return "(no body)"; }
        return s.length() <= 400 ? s : s.substring(0, 400) + "...";
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
