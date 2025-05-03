package fun.libx.flow.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个自定义的线程工厂实现，允许为创建的线程设置名称前缀。
 * 创建的线程名称格式为 "prefix-N"，其中 N 是一个自动递增的数字。
 * @author quding
 * @since 2025/5/3
 */
public class CustomThreadFactory implements ThreadFactory {

    private final String namePrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1); // 原子整型，确保线程安全地递增编号

    /**
     * 创建一个新的 CustomThreadFactory。
     *
     * @param namePrefix 线程名称的前缀。最终线程名将是 "namePrefix-N"。不能为空或空白。
     */
    public CustomThreadFactory(String namePrefix) {
        if (namePrefix == null || namePrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("Name prefix cannot be null or empty");
        }
        this.namePrefix = namePrefix + "-"; // 预先添加分隔符
    }

    /**
     * 创建一个新线程。
     *
     * @param r 一个 runnable 对象，新线程将执行它。
     * @return 创建的新线程。
     */
    @Override
    public Thread newThread(Runnable r) {
        // 构造线程名称: prefix + 递增编号
        String threadName = namePrefix + threadNumber.getAndIncrement();

        // 创建线程，指定线程组、要执行的 Runnable 和线程名称
        Thread t = new Thread(null, r, threadName, 0);

        // 设置线程属性 (可选，但推荐)
        if (t.isDaemon()) {
            // 通常我们希望工作线程是非守护线程，以防止 JVM 过早退出
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            // 设置为标准优先级
            t.setPriority(Thread.NORM_PRIORITY);
        }

        return t;
    }

    /**
     * 获取线程名称前缀。
     *
     * @return 线程名称前缀。
     */
    public String getNamePrefix() {
        return namePrefix.substring(0, namePrefix.length() - 1); // 移除末尾的 '-'
    }

}
