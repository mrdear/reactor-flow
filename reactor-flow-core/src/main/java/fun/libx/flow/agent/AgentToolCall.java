package fun.libx.flow.agent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 模型发起的工具调用描述。
 */
public class AgentToolCall {

    private final String id;
    private final String name;
    private final Map<String, Object> arguments;

    public AgentToolCall(String id, String name, Map<String, Object> arguments) {
        this.id = normalize(id, "toolCall.id");
        this.name = normalize(name, "toolCall.name");
        this.arguments = copyArguments(arguments);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    private static String normalize(String value, String fieldName) {
        String normalized = Objects.requireNonNull(value, fieldName + " cannot be null").trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return normalized;
    }

    private static Map<String, Object> copyArguments(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
