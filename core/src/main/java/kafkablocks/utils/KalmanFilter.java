package kafkablocks.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Реализация фильтра Калмана для гео-данных
 * Оригинальный алгоритм здесь: https://stackoverflow.com/questions/1134579/smooth-gps-data
 */
public final class KalmanFilter {
    /**
     * Минимальная точность измерения в метрах
     */
    private static final double MIN_ACCURACY = 0.1;

    /**
     * Parameter Q (in meters per second) describes how quickly the accuracy decays in the absence of any new location estimates.
     * A higher Q parameter means that the accuracy decays faster.
     * Kalman filters generally work better when the accuracy decays a bit quicker than one might expect.
     */
    private static final double Q = 3.0;

    private KalmanFilter() {
    }

    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class State {
        private long timeStamp;
        private double latitude;
        private double longitude;
        private double altitude;
        private double variance;
    }

    /**
     * @param prevState предыдущее состояние
     * @param timeStamp временная метка новых данных
     * @param latitude  новое значение широты
     * @param longitude новое значение долготы
     * @param altitude  новое значение высоты
     * @param accuracy  точность измерения в метрах
     * @return новое состояние, вычисленное на основе предыдущего и новых данных
     */
    public static State process(
            State prevState,
            long timeStamp,
            double latitude,
            double longitude,
            double altitude,
            double accuracy) {

        if (accuracy < MIN_ACCURACY) {
            accuracy = MIN_ACCURACY;
        }

        // if previous state undefined, so initialize it with new values
        if (prevState == null) {
            return new State(timeStamp, latitude, longitude, altitude, accuracy * accuracy);
        }

        double variance = prevState.variance;

        long duration = timeStamp - prevState.timeStamp;
        if (duration > 0) {
            variance = variance + duration * Q * Q / 1000;
        }

        // Kalman gain matrix 'k' = Covariance * Inverse(Covariance + MeasurementVariance)
        double k = variance / (variance + accuracy * accuracy);

        return new State(
                timeStamp,
                prevState.latitude + k * (latitude - prevState.latitude),
                prevState.longitude + k * (longitude - prevState.longitude),
                prevState.altitude + k * (altitude - prevState.altitude),
                // new Covariance matrix is (IdentityMatrix - k) * Covariance
                (1 - k) * variance);
    }
}
