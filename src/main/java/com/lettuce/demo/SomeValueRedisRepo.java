package com.lettuce.demo;


import com.lambdaworks.redis.RedisCommandExecutionException;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class SomeValueRedisRepo {
    private static final String EXPIRATION_CHANNEL = "__keyevent@0__:expired";
    private static final long TTL_OFFSET = TimeUnit.MINUTES.toSeconds(30);
    private static final LuaScript GET_DEL_SCRIPT = new LuaScript(
            "local value = redis.call('GET',KEYS[1])" +
                    "redis.call('DEL',KEYS[1])" +
                    "return value");
    private static final String REDIS_LUA_USE_EVAL = "NOSCRIPT No matching script. Please use EVAL.";

    private final RedisAsyncCommands<String, SomeValue> dataCommands;
    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final InternalPubSubListener pubSubListener;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private ExecutorService executor;

    @Autowired
    public SomeValueRedisRepo(
            StatefulRedisConnection<String, SomeValue> dataConnection,
            StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        this.dataCommands = dataConnection.async();
        this.pubSubConnection = pubSubConnection;
        this.pubSubListener = new InternalPubSubListener();
    }

    @PostConstruct
    public void start() {
        executor = Executors.newFixedThreadPool(4);
        pubSubConnection.sync().addListener(pubSubListener);
        pubSubConnection.sync().configSet("notify-keyspace-events", "Eex");
        pubSubConnection.sync().subscribe(EXPIRATION_CHANNEL);

    }

    @PreDestroy
    public void stop() {
        executor.shutdownNow();
        listeners.clear();
        pubSubConnection.removeListener(pubSubListener);
        pubSubConnection.sync().unsubscribe(EXPIRATION_CHANNEL);
    }

    public CompletableFuture<String> save(SomeKey someKey, SomeValue value, int ttl) {
        CompletableFuture<String> dataFuture = dataCommands.setex(someKey.toDataKey(), ttl + TTL_OFFSET, value).toCompletableFuture();
        CompletableFuture<String> expirationFuture = dataCommands.setex(someKey.toExpirationKey(), ttl, null).toCompletableFuture();
        return CompletableFuture.allOf(dataFuture, expirationFuture)
                .thenApplyAsync(_void -> dataFuture.join(), executor);
    }

    public CompletableFuture<Optional<SomeValue>> find(SomeKey key) {
        return dataCommands.get(key.toDataKey())
                .toCompletableFuture()
                .thenApplyAsync(Optional::ofNullable, executor);
    }

    public CompletableFuture<Long> delete(SomeKey key) {
        CompletableFuture<Long> dataFuture = dataCommands.del(key.toDataKey()).toCompletableFuture();
        CompletableFuture<Long> expirationFuture = dataCommands.del(key.toExpirationKey()).toCompletableFuture();
        return CompletableFuture.allOf(dataFuture, expirationFuture)
                .thenApplyAsync(_void -> {
                    return dataFuture.join();
                }, executor);
    }

    public CompletableFuture<Optional<SomeValue>> findAndDeleteAtomically(SomeKey key) {
        CompletableFuture<Object> shaFuture = dataCommands.evalsha(
                GET_DEL_SCRIPT.getSha1(), ScriptOutputType.VALUE, key.toDataKey()).toCompletableFuture();

        return shaFuture
                .handleAsync((result, error) -> error, executor)
                .thenCompose(error -> {
                    if (error != null && (error instanceof RedisCommandExecutionException)
                            && REDIS_LUA_USE_EVAL.equals(error.getMessage())) {
                        return dataCommands.eval(GET_DEL_SCRIPT.getScript(), ScriptOutputType.VALUE, key.toDataKey());
                    }
                    return shaFuture;
                }).thenApply(result -> {
                    if (result instanceof SomeValue) {
                        return Optional.of((SomeValue) result);
                    } else {
                        return Optional.empty();
                    }
                });
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }


    private class InternalPubSubListener extends RedisPubSubAdapter<String, String> {
        @Override
        public void message(String channel, String redisKey) {
            if (!EXPIRATION_CHANNEL.equals(channel)) {
                return;
            }

            if (SomeKey.isExpirationKeyCandidate(redisKey)) {
                executor.execute(() -> {
                    for (Listener listener : listeners) {
                        try {
                            listener.expired(SomeKey.fromExpirationCandidate(redisKey));
                        } catch (Exception e) {
                            log.error("Error notifying {}", redisKey, e);
                        }
                    }
                });
            }
        }
    }

    public interface Listener {
        void expired(SomeKey key);
    }
}
