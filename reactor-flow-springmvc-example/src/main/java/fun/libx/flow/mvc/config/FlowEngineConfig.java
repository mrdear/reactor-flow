package fun.libx.flow.mvc.config;

import fun.libx.flow.common.CustomThreadFactory;
import fun.libx.flow.event.FlowEventBus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration class for the Flow Engine.
 * Defines beans needed for the flow engine to work with Spring Boot.
 * 
 * @author quding
 * @since 2025/5/1
 */
@Configuration
public class FlowEngineConfig {

    /**
     * Creates a FlowEventBus bean.
     * 
     * @return the FlowEventBus instance
     */
    @Bean
    public FlowEventBus flowEventBus() {
        return new FlowEventBus();
    }

    /**
     * Creates an ExecutorService bean for the flow engine.
     * 
     * @return the ExecutorService instance
     */
    @Bean
    public ExecutorService flowExecutorService() {
        return Executors.newFixedThreadPool(1, new CustomThreadFactory("flow-scheduler"));
    }

}
