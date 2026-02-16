package fun.libx.flow.mvc.config;

import fun.libx.flow.common.CustomThreadFactory;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.llm.OpenAiLlmTaskInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Bean
    public OpenAiLlmTaskInstance openAiLlmTaskInstance(
            FlowEventBus eventBus,
            @Value("${openai.base-url:https://api.openai.com/v1}") String baseUrl,
            @Value("${openai.api-key:}") String apiKey,
            @Value("${openai.default-model:gpt-4o-mini}") String defaultModel,
            @Value("${openai.default-temperature:0.2}") double defaultTemperature,
            @Value("${openai.default-max-tokens:512}") int defaultMaxTokens) {
        return new OpenAiLlmTaskInstance(
                eventBus,
                baseUrl,
                apiKey,
                defaultModel,
                defaultTemperature,
                defaultMaxTokens
        );
    }

}
