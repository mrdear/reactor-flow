package fun.libx.flow.mvc.task;

import fun.libx.flow.FlowContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 延迟节点
 * @author quding
 * @since 2025/5/1
 */
@Component
public class HttpDelayInstance extends AbstractTaskInstance {

    private static final CloseableHttpAsyncClient CLIENT = HttpAsyncClients.createDefault();


    @Autowired
    public HttpDelayInstance(FlowEventBus eventBus) {
        super(eventBus);
        // 启动HTTP客户端
        CLIENT.start();
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, FlowContext context, TaskOutputResult result) {
        CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();

        // 使用较短的延迟时间以便于测试
        SimpleHttpRequest request = SimpleRequestBuilder.get("https://httpbin.org/delay/5").build();
        CLIENT.execute(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse simpleHttpResponse) {
                String bodyText = simpleHttpResponse.getBodyText();
                result.setResult(bodyText);
                future.complete(result);
            }

            @Override
            public void failed(Exception e) {
                future.completeExceptionally(e);
            }

            @Override
            public void cancelled() {
                future.completeExceptionally(new RuntimeException("cancelled"));
            }

        });
        return future;
    }

}
