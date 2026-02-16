package fun.libx.flow;

/**
 * @author quding
 * @since 2025/5/2
 */
public enum TaskTypeEnum implements TaskType {

    START,

    END,

    EXCEPTION,

    TIMEOUT,

    AGENT,

    LLM,
    ;
}
