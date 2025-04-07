package com.streamprocessing;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
*
* Input
*
*
*   1. input can be key and value pair of <timestamp, value>
    2. inputs are read from stream one at a time
    3. Group the timestamps by minutes, and print <time , avg of that minute> in no later 5 mins later than arrival
    4. some columns are noisy, which doesn't follow the above pattern, drop them
    Few sample records from continuous input stream:
    
    timestamp           unified_anomaly
    -------------------------------------
    2025-02-10 5:47:10  0.001025318456
    2025-02-10 5:38:00  0.001025318456
    2025-02-10 6:16:00  0.4645070349
    2025-02-10 5:47:10  0.001025318456
    2025-02-10 6:11:00  0.240809372
    2025-02-10 5:47:25  0.001025318456
    2025-02-10 5:51:00  0.001025318456
    2025-02-10 6:07:00  0.2016774278
    2025-02-10 5:55:00  0.001025318456
    2025-02-10 5:56:00  0.001025318456
* 
*
*
*
*
* Output
*
    2025-02-10 05:47:00: Average = 0.001025
    2025-02-10 05:38:00: Average = 0.001025
    2025-02-10 06:16:00: Average = 0.464507
    2025-02-10 05:47:00: Average = 0.001025
    2025-02-10 06:11:00: Average = 0.240809
    2025-02-10 05:47:00: Average = 0.001025
    2025-02-10 05:51:00: Average = 0.001025
    2025-02-10 06:07:00: Average = 0.201677
    2025-02-10 05:55:00: Average = 0.001025
    2025-02-10 05:56:00: Average = 0.001025
* */


public class StreamProcessor {
    private static final ConcurrentHashMap<String, List<Double>> dataByMinute = new ConcurrentHashMap<>();
    private static final int BUFFER_TIME_MINUTES = 5;

    public static void main(String[] args) {
        List<String[]> inputStream = Arrays.asList(
                new String[]{"2025-02-10 5:47:10", "0.001025318456"},
                new String[]{"2025-02-10 5:38:00", "0.001025318456"},
                new String[]{"2025-02-10 6:16:00", "0.4645070349"},
                new String[]{"2025-02-10 5:47:10", "0.001025318456"},
                new String[]{"2025-02-10 6:11:00", "0.240809372"},
                new String[]{"2025-02-10 5:47:25", "0.001025318456"},
                new String[]{"2025-02-10 5:51:00", "0.001025318456"},
                new String[]{"2025-02-10 6:07:00", "0.2016774278"},
                new String[]{"2025-02-10 5:55:00", "0.001025318456"},
                new String[]{"2025-02-10 5:56:00", "0.001025318456"}
        );

        processStream(inputStream);
    }

    private static void processStream(List<String[]> inputStream) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (String[] record : inputStream) {
            if (record == null || record.length != 2) {
                System.out.println("Skipping invalid record: " + Arrays.toString(record));
                continue;
            }
            try {
                String timestampStr = record[0];
                double value = Double.parseDouble(record[1]);
                Date timestamp = sdf.parse(timestampStr);

                // Group by minute
                Calendar cal = Calendar.getInstance();
                cal.setTime(timestamp);
                cal.set(Calendar.SECOND, 0);
                String minuteKey = sdf.format(cal.getTime());

                dataByMinute.putIfAbsent(minuteKey, new ArrayList<>());
                dataByMinute.get(minuteKey).add(value);

                // Process old records (older than BUFFER_TIME_MINUTES)
                Calendar cutoff = Calendar.getInstance();
                cutoff.add(Calendar.MINUTE, -BUFFER_TIME_MINUTES);

                Iterator<Map.Entry<String, List<Double>>> iterator = dataByMinute.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, List<Double>> entry = iterator.next();
                    Date minuteDate = sdf.parse(entry.getKey());
                    if (minuteDate.before(cutoff.getTime())) {
                        List<Double> values = entry.getValue();
                        double avg = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                        System.out.printf("%s: Average = %.6f%n", entry.getKey(), avg);
                        iterator.remove();
                    }
                }
            } catch (ParseException e) {
                System.out.println("Skipping noisy record due to invalid timestamp: " + Arrays.toString(record));
            } catch (NumberFormatException e) {
                System.out.println("Skipping noisy record due to invalid value: " + Arrays.toString(record));
            }
        }
    }
}
