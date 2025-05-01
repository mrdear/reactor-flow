package fun.libx.flow.mvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application entry point.
 * This class serves as the main class for the Spring Boot application.
 * 
 * @author quding
 * @since 2025/5/1
 */
@SpringBootApplication
public class Application {

    /**
     * Main method to start the Spring Boot application.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}