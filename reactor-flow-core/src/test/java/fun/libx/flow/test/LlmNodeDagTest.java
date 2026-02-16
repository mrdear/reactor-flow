package fun.libx.flow.test;

import com.alibaba.fastjson2.JSONObject;
import com.sun.net.httpserver.HttpServer;
import fun.libx.flow.FlowContext;
import fun.libx.flow.FlowDataKeys;
import fun.libx.flow.FlowFutureExecuteGraph;
import fun.libx.flow.FlowTaskEngineRouter;
import fun.libx.flow.NodeContext;
import fun.libx.flow.TaskType;
import fun.libx.flow.TaskTypeEnum;
import fun.libx.flow.event.FlowEventBus;
import fun.libx.flow.llm.OpenAiLlmTaskInstance;
import fun.libx.flow.model.FlowDag;
import fun.libx.flow.model.TaskNode;
import fun.libx.flow.task.AbstractTaskInstance;
import fun.libx.flow.task.EndTaskInstance;
import fun.libx.flow.task.StartTaskInstance;
import fun.libx.flow.task.TaskOutputResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author quding
 * @since 2026/2/16
 */
public class LlmNodeDagTest {

    @Test
    public void test_llm_node_should_call_openai_and_write_state() throws Exception {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);

        AtomicReference<String> requestBodyRef = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.createContext("/chat/completions", exchange -> {
            String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            requestBodyRef.set(requestBody);
            writeJsonResponse(exchange, 200, "{\"choices\":[{\"message\":{\"content\":\"mock-openai-answer\"}}]}");
        });
        server.start();

        try {
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            OpenAiLlmTaskInstance llmTask = new OpenAiLlmTaskInstance(eventBus, baseUrl, "test-api-key", "gpt-4o-mini", 0.2d, 128);

            FlowTaskEngineRouter router = node -> switch (node.getId()) {
                case "start" -> start;
                case "llm" -> llmTask;
                case "end" -> end;
                default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
            };

            JSONObject llmConfig = new JSONObject();
            FlowDataKeys.NODE_LLM_PROMPT_STATE_KEY.putData(llmConfig, "llm.prompt");
            FlowDataKeys.NODE_LLM_RESULT_STATE_KEY.putData(llmConfig, "llm.result");

            FlowDag dag = new FlowDag();
            dag.addTaskNode(createTaskNode("start", TaskTypeEnum.START));
            dag.addTaskNode(createTaskNode("llm", TaskTypeEnum.LLM, llmConfig));
            dag.addTaskNode(createTaskNode("end", TaskTypeEnum.END));
            dag.addEdge("start", "llm");
            dag.addEdge("llm", "end");

            FlowContext context = new FlowContext();
            context.putState("llm.prompt", "hello core llm");

            FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(2), router);
            Throwable throwable = executeAndGetThrowable(graph, 10);

            Assert.assertNull(throwable);
            Assert.assertEquals("mock-openai-answer", context.getState("llm.result", String.class));
            Assert.assertNotNull(requestBodyRef.get());
            Assert.assertTrue(requestBodyRef.get().contains("\"model\":\"gpt-4o-mini\""));
            Assert.assertTrue(requestBodyRef.get().contains("hello core llm"));
        } finally {
            server.stop(0);
            if (server.getExecutor() instanceof java.util.concurrent.ExecutorService executorService) {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    public void test_llm_node_should_cancel_pending_http_call_when_flow_is_cancelled() throws Exception {
        FlowEventBus eventBus = new FlowEventBus();
        StartTaskInstance start = new StartTaskInstance(eventBus);
        EndTaskInstance end = new EndTaskInstance(eventBus);

        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch allowResponse = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));
        server.createContext("/chat/completions", exchange -> {
            requestStarted.countDown();
            try {
                allowResponse.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            writeJsonResponse(exchange, 200, "{\"choices\":[{\"message\":{\"content\":\"late-answer\"}}]}");
        });
        server.start();

        try {
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            OpenAiLlmTaskInstance llmTask = new OpenAiLlmTaskInstance(eventBus, baseUrl, "test-api-key", "gpt-4o-mini", 0.2d, 128);
            WaitAndFailTask failTask = new WaitAndFailTask(eventBus, requestStarted);

            FlowTaskEngineRouter router = node -> switch (node.getId()) {
                case "start" -> start;
                case "llm" -> llmTask;
                case "fail" -> failTask;
                case "end" -> end;
                default -> throw new IllegalArgumentException("unknown node id: " + node.getId());
            };

            JSONObject llmConfig = new JSONObject();
            FlowDataKeys.NODE_LLM_PROMPT_STATE_KEY.putData(llmConfig, "llm.prompt.cancel");
            FlowDataKeys.NODE_LLM_RESULT_STATE_KEY.putData(llmConfig, "llm.result.cancel");
            FlowDataKeys.NODE_TIMEOUT_SECOND.putData(llmConfig, 60L);

            FlowDag dag = new FlowDag();
            dag.addTaskNode(createTaskNode("start", TaskTypeEnum.START));
            dag.addTaskNode(createTaskNode("llm", TaskTypeEnum.LLM, llmConfig));
            dag.addTaskNode(createTaskNode("fail", TaskTypeEnum.EXCEPTION));
            dag.addTaskNode(createTaskNode("end", TaskTypeEnum.END));
            dag.addEdge("start", "llm");
            dag.addEdge("start", "fail");
            dag.addEdge("llm", "end");
            dag.addEdge("fail", "end");

            FlowContext context = new FlowContext();
            context.putState("llm.prompt.cancel", "please wait");

            FlowFutureExecuteGraph graph = new FlowFutureExecuteGraph(dag, context, Executors.newFixedThreadPool(4), router);

            long startMillis = System.currentTimeMillis();
            Throwable throwable = executeAndGetThrowable(graph, 10);
            long cost = System.currentTimeMillis() - startMillis;

            Assert.assertTrue("llm request should start", requestStarted.await(1, TimeUnit.SECONDS));
            Assert.assertNotNull(throwable);
            Assert.assertTrue("flow should stop quickly after cancellation, cost=" + cost, cost < 3000L);
        } finally {
            allowResponse.countDown();
            server.stop(0);
            if (server.getExecutor() instanceof java.util.concurrent.ExecutorService executorService) {
                executorService.shutdownNow();
            }
        }
    }

    private static Throwable executeAndGetThrowable(FlowFutureExecuteGraph graph, int waitSeconds) {
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        try {
            graph.bfsExecute().whenComplete((r, e) -> throwableRef.set(e)).get(waitSeconds, TimeUnit.SECONDS);
        } catch (Exception ignore) {
            // ignore
        }
        return throwableRef.get();
    }

    private static TaskNode createTaskNode(String id, TaskType type) {
        return createTaskNode(id, type, new JSONObject());
    }

    private static TaskNode createTaskNode(String id, TaskType type, JSONObject nodeConfig) {
        TaskNode node = new TaskNode();
        node.setId(id);
        node.setType(type);
        node.setDefineConfig(nodeConfig);
        return node;
    }

    private static void writeJsonResponse(com.sun.net.httpserver.HttpExchange exchange, int statusCode, String responseBody) throws IOException {
        byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        } finally {
            exchange.close();
        }
    }

    private static class WaitAndFailTask extends AbstractTaskInstance {
        private final CountDownLatch requestStarted;

        private WaitAndFailTask(FlowEventBus eventBus, CountDownLatch requestStarted) {
            super(eventBus);
            this.requestStarted = requestStarted;
        }

        @Override
        protected CompletableFuture<TaskOutputResult> internalExecute(TaskNode taskNode, NodeContext context, TaskOutputResult result) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    if (!requestStarted.await(3, TimeUnit.SECONDS)) {
                        throw new RuntimeException("llm request did not start");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("fail branch");
            });
        }
    }
}
