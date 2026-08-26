import com.solacesystems.jcsmp.*;

/** Phase 0 spike: does provision(..., FLAG_IGNORE_ALREADY_EXISTS) tolerate a PROPERTY MISMATCH? */
public class ProvisionDriftSpike {

    static JCSMPSession session;

    public static void main(String[] args) throws Exception {
        JCSMPProperties p = new JCSMPProperties();
        p.setProperty(JCSMPProperties.HOST, "tcp://localhost:55565");
        p.setProperty(JCSMPProperties.VPN_NAME, "default");
        p.setProperty(JCSMPProperties.USERNAME, "default");
        p.setProperty(JCSMPProperties.PASSWORD, "default");
        session = JCSMPFactory.onlyInstance().createSession(p);
        session.connect();
        System.out.println("connected\n");

        Queue q = JCSMPFactory.onlyInstance().createQueue("q.spike.drift");
        try { session.deprovision(q, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST); } catch (Exception ignore) {}

        System.out.println("STEP 1  create with quota=100, maxRedelivery=3");
        provision(q, 100, 3, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

        System.out.println("\nSTEP 2  re-provision IDENTICAL props + IGNORE_ALREADY_EXISTS");
        provision(q, 100, 3, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

        System.out.println("\nSTEP 3  re-provision DIFFERENT quota=200 + IGNORE_ALREADY_EXISTS   <-- the question");
        provision(q, 200, 3, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

        System.out.println("\nSTEP 4  re-provision DIFFERENT maxRedelivery=9 + IGNORE_ALREADY_EXISTS");
        provision(q, 100, 9, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

        System.out.println("\nSTEP 5  re-provision existing WITHOUT the ignore flag");
        provision(q, 100, 3, JCSMPSession.WAIT_FOR_CONFIRM);

        Queue missing = JCSMPFactory.onlyInstance().createQueue("q.spike.drift.missing");
        try { session.deprovision(missing, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST); } catch (Exception ignore) {}
        System.out.println("\nSTEP 6  provision a MISSING queue WITHOUT the ignore flag   <-- does it refuse, or create it?");
        provision(missing, 100, 3, JCSMPSession.WAIT_FOR_CONFIRM);
        try { session.deprovision(missing, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST); } catch (Exception ignore) {}

        System.out.println("\nsubcode reference:");
        System.out.println("  ENDPOINT_ALREADY_EXISTS    = " + JCSMPErrorResponseSubcodeEx.ENDPOINT_ALREADY_EXISTS);
        System.out.println("  ENDPOINT_PROPERTY_MISMATCH = " + JCSMPErrorResponseSubcodeEx.ENDPOINT_PROPERTY_MISMATCH);

        session.closeSession();
    }

    static void provision(Queue q, int quotaMb, int maxRedelivery, long flags) {
        EndpointProperties ep = new EndpointProperties();
        ep.setPermission(EndpointProperties.PERMISSION_CONSUME);
        ep.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        ep.setQuota(quotaMb);
        ep.setMaxMsgRedelivery(maxRedelivery);
        ep.setRespectsMsgTTL(Boolean.TRUE);
        ep.setDiscardBehavior(EndpointProperties.DISCARD_NOTIFY_SENDER_ON);
        try {
            session.provision(q, ep, flags);
            System.out.println("  -> OK, no exception");
        } catch (JCSMPErrorResponseException e) {
            System.out.println("  -> JCSMPErrorResponseException"
                + "  subcode=" + e.getSubcodeEx()
                + "  responseCode=" + e.getResponseCode()
                + "  phrase='" + e.getResponsePhrase() + "'");
        } catch (JCSMPException e) {
            System.out.println("  -> " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
