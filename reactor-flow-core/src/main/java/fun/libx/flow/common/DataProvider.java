package fun.libx.flow.common;

/**
 * @author quding
 * @since 2025/5/2
 */
public interface DataProvider {

    /**
     * 根据指定key获取数据
     * @param dataKey key属性
     * @return 获取结果
     * @param <T> 值
     */
    <T> T getData(DataKey<T> dataKey);

    default <T> T getDataOr(DataKey<T> dataKey, T defaultValue) {
        T data = getData(dataKey);
        if (null == data) {
            return defaultValue;
        }
        return data;
    }


}
