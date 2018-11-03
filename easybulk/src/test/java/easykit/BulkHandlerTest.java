package easykit;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BulkHandlerTest {
    private BulkHandler<String> bulkHandler;

    @Before
    public void init() {
        if (bulkHandler != null) {
            bulkHandler.close();
        }
    }

    @Test
    public void testMaxWaitTimeFlush() {
        bulkHandler = BulkHandler.builder().setBulkActions(100)
                .setOperation(new Consumer<Collection<String>>() {
                    @Override
                    public void accept(Collection<String> strings) {
                    }
                })
                .setListener(new BulkHandler.Listener<String>() {
                    @Override
                    public void beforeBulk(List<String> footprints) {
                        for (String footprint : footprints) {
                            System.out.println("before " + footprint);
                        }
                    }

                    @Override
                    public void afterBulk(List<String> footprints) {
                        for (String footprint : footprints) {
                            System.out.println("after " + footprint);
                        }
                    }

                    @Override
                    public void afterBulk(List<String> footprints, Throwable failure) {
                        for (String footprint : footprints) {
                            System.out.println("after throw " + footprint);
                        }
                    }
                })
                .setFlushInterval(2000, TimeUnit.MILLISECONDS)
                .setRetry(3)
                .build();
        bulkHandler.add("test1");
        bulkHandler.add("test2");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMaxCapacityFlush() {
        bulkHandler = BulkHandler.builder().setBulkActions(2000)
                .setOperation(new Consumer<Collection<String>>() {
                    @Override
                    public void accept(Collection<String> strings) {
                    }
                })
                .setListener(new BulkHandler.Listener<String>() {
                    @Override
                    public void beforeBulk(List<String> footprints) {
                        System.out.println("before " + footprints.size());
                    }

                    @Override
                    public void afterBulk(List<String> footprints) {
                        System.out.println("after " + footprints.size());
                    }

                    @Override
                    public void afterBulk(List<String> footprints, Throwable failure) {
                        System.out.println("after throw " + footprints.size());
                    }
                })
                .setFlushInterval(2000, TimeUnit.MILLISECONDS)
                .setRetry(3)
                .build();
        for (int i = 0; i < 2001; i++) {
            bulkHandler.add("test" + i);
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRetry() {
        bulkHandler = BulkHandler.builder().setBulkActions(1)
                .setOperation(new Consumer<Collection<String>>() {
                    @Override
                    public void accept(Collection<String> strings) {
                        throw new RuntimeException("test exception");
                    }
                })
                .setListener(new BulkHandler.Listener<String>() {
                    @Override
                    public void beforeBulk(List<String> footprints) {
                        for (String footprint : footprints) {
                            System.out.println("before " + footprint);
                        }
                    }

                    @Override
                    public void afterBulk(List<String> footprints) {
                        for (String footprint : footprints) {
                            System.out.println("after " + footprint);
                        }
                    }

                    @Override
                    public void afterBulk(List<String> footprints, Throwable failure) {
                        for (String footprint : footprints) {
                            System.out.println("after throw " + footprint);
                        }
                    }
                })
                .setFlushInterval(2000, TimeUnit.MILLISECONDS)
                .setRetry(3)
                .build();
        bulkHandler.add("test1");
        bulkHandler.add("test2");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
