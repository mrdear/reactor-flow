package fun.libx.flow.common;

import com.alibaba.fastjson2.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author quding
 * @since 2025/5/2
 */
public class DataKey<T> {

    /**
     * 缓存系统的key,避免重复创建
     */
    private static final Map<String, DataKey<?>> CACHE = new ConcurrentHashMap<>();

    public final String dataId;

    public final Class<T> clazz;

    public final String comment;

    private DataKey(String dataId, Class<T> clazz, String comment) {
        this.dataId = dataId;
        this.comment = comment;
        this.clazz = clazz;
    }

    @SuppressWarnings("unchecked")
    public static <T> DataKey<T> of(String dataId, Class<T> clazz, String comment) {
        return (DataKey<T>) CACHE.computeIfAbsent(dataId, k -> new DataKey<>(dataId, clazz, comment));
    }

    @SuppressWarnings("unchecked")
    public static <T> DataKey<T> of(String dataId, Class<T> clazz) {
        return (DataKey<T>) CACHE.computeIfAbsent(dataId, k -> new DataKey<>(dataId, clazz, ""));
    }

    /**
     * 获取数据
     */
    public T getData(DataProvider provider) {
        return provider.getData(this);
    }

    /**
     * 是否有true值
     * @return 获取结果
     */
    public boolean hasTureData(DataProvider provider) {
        Boolean data = (Boolean) getData(provider);
        return data != null && data;
    }

    /**
     * 获取数据
     */
    public T getDataOr(DataProvider provider, T defaultValue) {
        return provider.getDataOr(this, defaultValue);
    }

    /**
     * 存放值
     */
    public void putData(DataProvider provider, T data) {
        provider.setData(this, data);
    }

    /**
     * 存放值
     */
    public void putData(JSONObject provider, T data) {
        provider.put(dataId, data);
    }

}
