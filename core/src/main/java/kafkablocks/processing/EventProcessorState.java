package kafkablocks.processing;

import kafkablocks.utils.TimeUtils;
import lombok.Getter;

public abstract class EventProcessorState {
    /**
     * Время, когда последний раз было обновлено состояние
     */
    @Getter
    private long timestamp;

    protected EventProcessorState() {
        updateTimestamp();
    }

    void updateTimestamp() {
        timestamp = TimeUtils.getNowTimestamp();
    }

    /**
     * "Возраст" состояния
     * @return кол-во миллисекунд, прошедшее с момента последнего обновления (и соотв. сохранения) состояния
     */
    public long getAge() {
        return TimeUtils.getNowTimestamp() - timestamp;
    }
}
