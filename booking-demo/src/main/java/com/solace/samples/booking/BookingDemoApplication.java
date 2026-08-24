package com.solace.samples.booking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Runnable train seat-reservation demo.
 *
 * <p>Both sides of the conversation run in one process by default, which is what makes a
 * single {@code curl} demonstrate the whole round trip. They are independent beans over
 * separate queues, so splitting them across two deployments needs no code change — run with
 * {@code --spring.profiles.active=requestor} or {@code replier} to prove it.
 */
@SpringBootApplication
public class BookingDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(BookingDemoApplication.class, args);
    }
}
