package com.solace.samples.booking.domain;

/** Reservation classes, as used by Indian Railways. */
public enum SeatClass {
    AC1("1a"), AC2("2a"), AC3("3a"), SLEEPER("sl"), CHAIR_CAR("cc"), SECOND_SITTING("2s");

    private final String code;

    SeatClass(String code) { this.code = code; }

    /** Lowercase topic-level form, e.g. {@code 3a}. */
    public String code() { return code; }

    public static SeatClass fromCode(String code) {
        for (SeatClass c : values()) {
            if (c.code.equalsIgnoreCase(code)) { return c; }
        }
        throw new IllegalArgumentException("Unknown seat class code: " + code);
    }
}
