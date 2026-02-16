package fun.libx.flow.mvc.task;

import fun.libx.flow.NodeContext;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * 延迟节点
 * @author quding
 * @since 2025/5/1
 */
@Component
public class HttpDelayInstance extends AbstractTaskInstance {

    private static final CloseableHttpAsyncClient CLIENT;


    static {
        PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnPerRoute(200)  // Increase connections per route
                .setMaxConnTotal(200)    // Increase total connections
                .build();


        // Configure request config with timeouts
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(10))
                .setResponseTimeout(Timeout.ofSeconds(10))
                .build();

        // Build the client with custom configuration
        CLIENT = HttpAsyncClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();
        CLIENT.start();
    }

    @Autowired
    public HttpDelayInstance(FlowEventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
        CompletableFuture<TaskOutputResult> future = new CompletableFuture<>();

        // 使用较短的延迟时间以便于测试
        SimpleHttpRequest request = SimpleRequestBuilder.get("https://httpbin.org/delay/5").build();
        Future<SimpleHttpResponse> requestFuture = CLIENT.execute(request, new FutureCallback<SimpleHttpResponse>() {
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

        var cancellationRegistration = context.registerCancellationAction(() -> {
            requestFuture.cancel(true);
            future.completeExceptionally(new CancellationException("flow cancellation triggered"));
        });
        future.whenComplete((r, e) -> cancellationRegistration.unregister());

        return future;
    }

}
