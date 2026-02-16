package fun.libx.flow.mvc.task;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.FlowContext;
import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * OpenAI LLM节点实现（单轮对话）。
 */
@Component
public class OpenAiLlmTaskInstance extends AbstractTaskInstance {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @Value("${openai.base-url:https://api.openai.com/v1}")
    private String openAiBaseUrl;

    @Value("${openai.api-key:}")
    private String openAiApiKey;

    @Value("${openai.default-model:gpt-4o-mini}")
    private String defaultModel;

    @Value("${openai.default-temperature:0.2}")
    private double defaultTemperature;

    @Value("${openai.default-max-tokens:512}")
    private int defaultMaxTokens;

    @Autowired
    public OpenAiLlmTaskInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        String prompt = resolvePrompt(taskNode, context);
        if (prompt == null || prompt.trim().isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("llm prompt cannot be empty"));
        }

        String apiKey = resolveApiKey(taskNode, context);
        if (apiKey == null || apiKey.trim().isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("OpenAI api key is required"));
        }

        String model = FlowDataKeys.NODE_LLM_MODEL.getDataOr(taskNode, defaultModel);
        String systemPrompt = FlowDataKeys.NODE_LLM_SYSTEM_PROMPT.getDataOr(taskNode, "You are a helpful assistant.");
        double temperature = normalizeTemperature(FlowDataKeys.NODE_LLM_TEMPERATURE.getDataOr(taskNode, defaultTemperature));
        int maxTokens = Math.max(1, FlowDataKeys.NODE_LLM_MAX_TOKENS.getDataOr(taskNode, defaultMaxTokens));

        JSONObject requestBody = new JSONObject();
        requestBody.put("model", model);
        requestBody.put("temperature", temperature);
        requestBody.put("max_tokens", maxTokens);
        requestBody.put("messages", buildMessages(systemPrompt, prompt));

        String baseUrl = normalizeBaseUrl(openAiBaseUrl);
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(5L, FlowDataKeys.NODE_TIMEOUT_SECOND.getDataOr(taskNode, 60L))))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody.toJSONString(), StandardCharsets.UTF_8))
                .build();

        CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();
        CompletableFuture<HttpResponse<String>> httpFuture = HTTP_CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        FlowContext.CancellationRegistration cancellationRegistration = context.registerCancellationAction(() -> {
            httpFuture.cancel(true);
            future.completeExceptionally(new CancellationException("flow cancellation triggered"));
        });

        httpFuture.whenComplete((response, throwable) -> {
            cancellationRegistration.unregister();
            if (future.isDone()) {
                return;
            }

            if (throwable != null) {
                future.completeExceptionally(unwrapCompletionException(throwable));
                return;
            }

            if (response == null) {
                future.completeExceptionally(new IllegalStateException("OpenAI empty response"));
                return;
            }

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                future.completeExceptionally(new IllegalStateException(
                        "OpenAI request failed, status=" + response.statusCode() + ", body=" + abbreviate(response.body(), 512)));
                return;
            }

            try {
                String answer = extractAssistantContent(response.body());
                String resultStateKey = resolveResultStateKey(taskNode);
                context.putState(resultStateKey, answer);
                result.setResult(answer);
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private static Throwable unwrapCompletionException(Throwable throwable) {
        Throwable current = throwable;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static JSONArray buildMessages(String systemPrompt, String prompt) {
        JSONArray messages = new JSONArray();
        JSONObject systemMessage = new JSONObject();
        systemMessage.put("role", "system");
        systemMessage.put("content", systemPrompt);
        messages.add(systemMessage);

        JSONObject userMessage = new JSONObject();
        userMessage.put("role", "user");
        userMessage.put("content", prompt);
        messages.add(userMessage);
        return messages;
    }

    private static String extractAssistantContent(String body) {
        JSONObject responseJson = JSONObject.parseObject(body);
        if (responseJson == null) {
            throw new IllegalStateException("OpenAI response is not valid JSON");
        }

        JSONArray choices = responseJson.getJSONArray("choices");
        if (choices == null || choices.isEmpty()) {
            throw new IllegalStateException("OpenAI response has no choices");
        }

        JSONObject firstChoice = choices.getJSONObject(0);
        if (firstChoice == null) {
            throw new IllegalStateException("OpenAI first choice is empty");
        }

        JSONObject message = firstChoice.getJSONObject("message");
        if (message == null) {
            throw new IllegalStateException("OpenAI response message is empty");
        }

        String content = message.getString("content");
        if (content == null) {
            throw new IllegalStateException("OpenAI response content is empty");
        }
        return content;
    }

    private static String abbreviate(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        if (text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength) + "...";
    }

    private static String normalizeBaseUrl(String baseUrl) {
        String normalized = Objects.requireNonNullElse(baseUrl, "").trim();
        if (normalized.isEmpty()) {
            normalized = "https://api.openai.com/v1";
        }
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static double normalizeTemperature(Double temperatureValue) {
        double value = temperatureValue == null ? 0.2d : temperatureValue;
        if (value < 0d) {
            return 0d;
        }
        if (value > 2d) {
            return 2d;
        }
        return value;
    }

    private String resolveApiKey(TaskNode taskNode, NodeContext context) {
        String apiKeyStateKey = FlowDataKeys.NODE_LLM_API_KEY_STATE_KEY.getData(taskNode);
        if (apiKeyStateKey != null && !apiKeyStateKey.trim().isEmpty()) {
            Object stateValue = context.getState(apiKeyStateKey);
            if (stateValue instanceof String stateApiKey && !stateApiKey.trim().isEmpty()) {
                return stateApiKey.trim();
            }
        }

        if (openAiApiKey != null && !openAiApiKey.trim().isEmpty()) {
            return openAiApiKey.trim();
        }

        String envApiKey = System.getenv("OPENAI_API_KEY");
        if (envApiKey != null && !envApiKey.trim().isEmpty()) {
            return envApiKey.trim();
        }
        return null;
    }

    private static String resolvePrompt(TaskNode taskNode, NodeContext context) {
        String promptStateKey = FlowDataKeys.NODE_LLM_PROMPT_STATE_KEY.getData(taskNode);
        if (promptStateKey != null && !promptStateKey.trim().isEmpty()) {
            Object stateValue = context.getState(promptStateKey);
            if (stateValue instanceof String promptText) {
                return promptText;
            }
            if (stateValue != null) {
                return stateValue.toString();
            }
        }
        return FlowDataKeys.NODE_LLM_PROMPT.getDataOr(taskNode, "");
    }

    private static String resolveResultStateKey(TaskNode taskNode) {
        return FlowDataKeys.NODE_LLM_RESULT_STATE_KEY.getDataOr(taskNode, "llm.result." + taskNode.getId());
    }
}
