package fun.libx.flow.mvc.model;

import fun.libx.flow.FlowContext;
import lombok.Getter;
import lombok.Setter;

/**
 * Extended FlowContext with additional fields for the Spring Boot application.
 * 
 * @author quding
 * @since 2025/5/1
 */
public class ExtendedFlowContext extends FlowContext {
    
    /**
     * Unique identifier for the flow.
     */
    @Getter
    @Setter
    private String flowId;
    
    /**
     * Start time of the flow execution.
     */
    @Getter
    @Setter
    private long startTime;
    
    /**
     * End time of the flow execution.
     */
    @Getter
    @Setter
    private long endTime;
    
    /**
     * Creates a new ExtendedFlowContext with a generated flow ID and the current time as start time.
     */
    public ExtendedFlowContext() {
        this.startTime = System.currentTimeMillis();
    }

    private ExtendedFlowContext(ExtendedFlowContext parent, String scope) {
        super(parent, scope);
        this.flowId = parent.flowId;
        this.startTime = parent.startTime;
        this.endTime = parent.endTime;
    }

    @Override
    public FlowContext forkForNode(String nodeId) {
        return new ExtendedFlowContext(this, nodeId);
    }

    @Override
    protected void mergeCustomStateFrom(FlowContext childContext) {
        if (!(childContext instanceof ExtendedFlowContext extendedChild)) {
            return;
        }
        if (extendedChild.flowId != null) {
            this.flowId = extendedChild.flowId;
        }
        this.startTime = extendedChild.startTime;
        this.endTime = extendedChild.endTime;
    }

    @Override
    protected void resetCustomStateFrom(FlowContext sourceContext) {
        if (!(sourceContext instanceof ExtendedFlowContext source)) {
            return;
        }
        this.flowId = source.flowId;
        this.startTime = source.startTime;
        this.endTime = source.endTime;
    }
    
    /**
     * Calculates the duration of the flow execution in milliseconds.
     * 
     * @return the duration in milliseconds, or -1 if the flow is still running
     */
    public long getDuration() {
        if (endTime == 0) {
            return -1;
        }
        return endTime - startTime;
    }
    
    /**
     * Marks the flow as completed by setting the end time.
     */
    public void markCompleted() {
        this.endTime = System.currentTimeMillis();
    }
}
