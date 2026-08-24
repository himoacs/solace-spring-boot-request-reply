package com.solace.samples.requestreply.endpoint;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A reply topic template with two kinds of placeholder.
 *
 * <p><b>Static</b> placeholders resolve once at startup and appear literally in the
 * subscription. <b>Per-request</b> placeholders resolve at publish time and appear as
 * {@code *} in the subscription, so one subscription covers every value.
 *
 * <pre>
 * pattern       cris/booking/seatReserve/reply/v1/{zone}/{trainNo}/{instanceId}
 * per-request   [trainNo]
 *
 * subscription  cris/booking/seatReserve/reply/v1/nr/&#42;/client-0-a3f9
 * reply-to      cris/booking/seatReserve/reply/v1/nr/12951/client-0-a3f9
 * </pre>
 *
 * <p>This is what lets request-derived levels ride along on the reply topic without coupling
 * the replier to its structure: the <em>requestor</em> builds the concrete reply-to, because
 * it already knows the train number, and the replier still echoes the value verbatim.
 */
public final class ReplyTopicPattern {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([A-Za-z0-9_]+)}");

    private final String pattern;
    private final Set<String> perRequest;
    private final Map<String, String> staticValues;
    private final String subscription;

    public ReplyTopicPattern(String pattern, List<String> perRequestPlaceholders,
                             Map<String, String> staticValues) {
        this.pattern = pattern;
        this.perRequest = new LinkedHashSet<>(perRequestPlaceholders == null ? List.of() : perRequestPlaceholders);
        this.staticValues = new LinkedHashMap<>(staticValues == null ? Map.of() : staticValues);
        this.subscription = render(Map.of(), true);
        String unresolved = firstUnresolved(subscription);
        if (unresolved != null) {
            throw new IllegalStateException("Reply topic pattern '" + pattern + "' leaves {"
                    + unresolved + "} unresolved. Either add it to "
                    + "solace.request-reply.reply.placeholders, or declare it in "
                    + "per-request-placeholders so it is wildcarded in the subscription.");
        }
    }

    /** The subscription, with per-request placeholders wildcarded. */
    public String subscription() { return subscription; }

    /** A concrete reply-to for one request. */
    public String resolve(Map<String, String> perRequestValues) {
        return render(perRequestValues == null ? Map.of() : perRequestValues, false);
    }

    public Set<String> perRequestPlaceholders() { return Set.copyOf(perRequest); }

    private String render(Map<String, String> perRequestValues, boolean wildcardPerRequest) {
        Matcher m = PLACEHOLDER.matcher(pattern);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            String value;
            if (perRequest.contains(name)) {
                value = wildcardPerRequest ? "*" : sanitize(perRequestValues.get(name));
            } else {
                value = staticValues.get(name);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value == null ? "{" + name + "}" : value));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static String firstUnresolved(String rendered) {
        Matcher m = PLACEHOLDER.matcher(rendered);
        return m.find() ? m.group(1) : null;
    }

    /** Topic levels may not contain the level separator or either wildcard character. */
    static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) { return "unknown"; }
        return raw.replaceAll("[/>*\\s]", "-");
    }

    @Override
    public String toString() { return subscription; }
}
