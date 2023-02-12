package Logger;

import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class GetTimer {

    public static LocalTime getCurrentTime() throws InterruptedException {
        LocalTime localTime = LocalTime.now();
        return localTime;
    }

    public static long duration(LocalTime l1 , LocalTime l2){
        Duration duration = Duration.between(l1, l2);
        long durationInMs = TimeUnit.MILLISECONDS.convert(duration.getNano(), TimeUnit.NANOSECONDS);
        return durationInMs;
    }




}
