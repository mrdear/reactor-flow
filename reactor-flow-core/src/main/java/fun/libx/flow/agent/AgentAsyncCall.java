package fun.libx.flow.agent;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 可取消的异步调用抽象，用于将flow取消信号桥接到模型/工具调用。
 *
 * @param <T> 返回值类型
 */
public interface AgentAsyncCall<T> {

    CompletableFuture<T> future();

    default void cancel() {
        // default no-op
    }

    static <T> AgentAsyncCall<T> fromFuture(CompletableFuture<T> future) {
        Objects.requireNonNull(future, "future cannot be null");
        return new AgentAsyncCall<>() {
            @Override
            public CompletableFuture<T> future() {
                return future;
            }
        };
    }

    static <T> AgentAsyncCall<T> fromFuture(CompletableFuture<T> future, Runnable cancelAction) {
        Objects.requireNonNull(future, "future cannot be null");
        return new AgentAsyncCall<>() {
            @Override
            public CompletableFuture<T> future() {
                return future;
            }

            @Override
            public void cancel() {
                if (cancelAction != null) {
                    cancelAction.run();
                }
            }
        };
    }
}
