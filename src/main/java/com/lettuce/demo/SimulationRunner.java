package com.lettuce.demo;

import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class SimulationRunner implements ApplicationRunner {
    private static final Random RAND = new Random();
    private static final int MAX_ITERATIONS = 1000000;

    private final SomeValueRedisRepo repo;
    private final RedisAsyncCommands<String, SomeValue> asyncCommands;

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Autowired
    public SimulationRunner(SomeValueRedisRepo repo,
                            StatefulRedisConnection<String, SomeValue> dataConnection) {
        this.repo = repo;
        this.asyncCommands = dataConnection.async();
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        repo.addListener(key -> repo.findAndDeleteAtomically(key)
                .thenAccept(result -> {
                    if (result.isPresent()) {
                        log.debug("Found by {} in atomic operation {}", key, result.get());
                    } else {
                        log.error("Not found in atomic operation for key {}", key);
                    }
                })
                .exceptionally(e -> {
                    log.error("Error in atomic operation for {}", key, e);
                    return null;
                }));

        doSimulate();
    }

    @PostConstruct
    public void start() {
        executor = Executors.newFixedThreadPool(4);
        scheduler = Executors.newScheduledThreadPool(4);
    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    private void doSimulate() {
        scheduler.scheduleWithFixedDelay(() -> {
            asyncCommands.ping().toCompletableFuture().
                    whenCompleteAsync((pong, e) -> {
                        if (e != null) {
                            log.error("Error pong", e);
                        } else if (!pong.equals("PONG")) {
                            log.error("Not PONG: {}", pong);
                        }

                    });
        }, 0, 1, TimeUnit.SECONDS);

        SomeValue value = new SomeValue("value", 0);
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            SomeKey someKey = generateKey();

            final int iteration = i;
            executor.execute(() -> {
                repo.save(someKey, value, 30)
                        .thenAccept(response ->
                                log.debug("#{}. Saved {} got response {}", iteration, someKey, response))
                        .exceptionally(e -> {
                            log.error("Failed to save {}", someKey, e);
                            return null;
                        });
            });

            int delay = RAND.nextInt(60);
            scheduler.schedule(() -> {
                try {
                    repo.find(someKey)
                            .thenAccept(result -> {
                                if (result.isPresent()) {
                                    log.debug("Found by {} result {}", someKey, result);
                                } else {
                                    log.debug("Not found by {}", someKey);
                                }
                            }).exceptionally(e -> {
                        log.error("Failed to find {}", someKey, e);
                        return null;
                    });
                } catch (Exception e) {
                    log.error("Error scheduling", e);
                }
            }, delay, TimeUnit.SECONDS);
        }
    }

    public SomeKey generateKey() {
        int id = RAND.nextInt(100000);
        return new SomeKey(id, String.valueOf(id));
    }

}
