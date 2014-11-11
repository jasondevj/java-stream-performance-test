package dev.j;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.LongStream;

/**
 * Jave 8 stream performance test
 */
public class Java8PerfTest {

    private static long LIMIT = 5_000_000_00l;
    public static final int NUMBER_OF_CYCLE = 2;

    private final static int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executors = Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);

    public static void main(String[] args) {
        //warm up
        long sum = warmUp();
        System.out.println(sum);

        //        Start testing
//        ==================
        System.out.println("Core count : " + NUMBER_OF_PROCESSORS);
        sum = 0;
        Map<String, Supplier<Long>> tests = new LinkedHashMap<>();
        tests.put("Normal (S)", Java8PerfTest::serialSum);
        tests.put("Sync O (P)", Java8PerfTest::parallelObjectSynchronised);
        tests.put("Sync A (P)", Java8PerfTest::parallelArraySynchronised);
        tests.put("Atomic (S)", Java8PerfTest::serialAtomic);
        tests.put("Atomic (P)", Java8PerfTest::parallelAtomic);
        tests.put("Range  (S)", Java8PerfTest::serialRange);
        tests.put("Range  (P)", Java8PerfTest::parallelRange);
        tests.put("Smart  (P)", Java8PerfTest::parallelSmart);

        for (Map.Entry<String, Supplier<Long>> entry : tests.entrySet()) {
            long time = System.currentTimeMillis();
            sum = run(entry.getValue());
            time = System.currentTimeMillis() - time;
            System.out.println("Time Taken " + entry.getKey() + "\t : " + time + " Total [" + sum + "]");
        }
        executors.shutdown();
    }

    private static long parallelAtomic() {
        AtomicLong sum = new AtomicLong();
        CountDownLatch countDownLatch = new CountDownLatch(NUMBER_OF_PROCESSORS);
        long totalPerThread = LIMIT / NUMBER_OF_PROCESSORS;
        for (int i = 0; i < NUMBER_OF_PROCESSORS; i++) {
            final int finalI = i;
            executors.execute(() -> {
                for (int j = (int) (totalPerThread * finalI); j < (totalPerThread * (finalI + 1)); j++) {
                    sum.addAndGet(j);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sum.get();
    }

    public static long serialSum() {
        long sum = 0;
        for (int i = 0; i < LIMIT; i++) {
            sum += i;
        }
        return sum;
    }


    public static long parallelArraySynchronised() {
        final long[] sum = {0};
        CountDownLatch countDownLatch = new CountDownLatch(NUMBER_OF_PROCESSORS);
        long totalPerThread = LIMIT / NUMBER_OF_PROCESSORS;
        for (int i = 0; i < NUMBER_OF_PROCESSORS; i++) {
            final int finalI = i;
            executors.execute(() -> {
                for (int j = (int) (totalPerThread * finalI); j < (totalPerThread * (finalI + 1)); j++) {
                    synchronized (Java8PerfTest.class) {
                        sum[0] += j;
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sum[0];
    }

    static class ObjectSynchronisedCounter {
        long sum;
    }

    public static long parallelObjectSynchronised() {
        final ObjectSynchronisedCounter counter = new ObjectSynchronisedCounter();
        CountDownLatch countDownLatch = new CountDownLatch(NUMBER_OF_PROCESSORS);
        long totalPerThread = LIMIT / NUMBER_OF_PROCESSORS;
        for (int i = 0; i < NUMBER_OF_PROCESSORS; i++) {
            final int finalI = i;
            executors.execute(() -> {
                for (int j = (int) (totalPerThread * finalI); j < (totalPerThread * (finalI + 1)); j++) {
                    synchronized (Java8PerfTest.class) {
                        counter.sum += j;
                    }
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return counter.sum;
    }

    public static long parallelSmart() {
        CountDownLatch countDownLatch = new CountDownLatch(NUMBER_OF_PROCESSORS);
        long totalPerThread = LIMIT / NUMBER_OF_PROCESSORS;
        Future<Long>[] fu = new Future[NUMBER_OF_PROCESSORS];
        for (int i = 0; i < NUMBER_OF_PROCESSORS; i++) {
            final int finalI = i;
            fu[i] = executors.submit(() -> {
                long sum = 0;
                for (int j = (int) (totalPerThread * finalI); j < (totalPerThread * (finalI + 1)); j++) {
                    sum += j;
                }
                countDownLatch.countDown();
                return sum;
            });
        }
        long sum = 0;
        try {
            countDownLatch.await();
            for (Future<Long> aFu : fu) {
                sum += aFu.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sum;
    }

    public static long serialAtomic() {
        AtomicLong aLong = new AtomicLong();
        for (int i = 0; i < LIMIT; i++) {
            aLong.addAndGet(i);
        }
        return aLong.get();
    }


    public static long parallelRange() {
        return LongStream.range(1, LIMIT)
                .parallel()
                .sum();

    }

    public static long serialRange() {
        return LongStream.range(1, LIMIT)
                .sum();

    }

    private static long run(Supplier<Long> function) {
        long sum = 0;
        for (int i = 0; i < NUMBER_OF_CYCLE; i++) {
            sum += function.get();
        }
        return sum;
    }

    private static long warmUp() {
        long sum = serialSum();
        sum = sum + serialSum();
        return sum;
    }
}
