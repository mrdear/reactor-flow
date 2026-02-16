package fun.libx.flow.agent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 工具执行结果。
 */
public class AgentToolResult {

    private final String text;
    private final Map<String, Object> details;

    public AgentToolResult(String text, Map<String, Object> details) {
        this.text = text == null ? "" : text;
        this.details = copyDetails(details);
    }

    public static AgentToolResult ofText(String text) {
        return new AgentToolResult(text, Collections.emptyMap());
    }

    public String getText() {
        return text;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    private static Map<String, Object> copyDetails(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
