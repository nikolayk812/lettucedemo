package com.lettuce.demo;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class LettuceConfig {

    @Bean
    public RedisClient redisClient(RedisProperties properties) {
        RedisURI.Builder uriBuilder = RedisURI.builder();

        /*String sentinelNodes = properties.getSentinel().getNodes();
        String[] sentinelNodeParts = sentinelNodes.split(",");

        RedisURI.Builder uriBuilder = RedisURI.builder();
        for (String sentinelNode : sentinelNodeParts) {
            String[] sentinel = sentinelNode.split(":");
            uriBuilder.withSentinel(sentinel[0], Integer.valueOf(sentinel[1]));
        }

        uriBuilder.withSentinelMasterId(properties.getSentinel().getMaster());*/

        uriBuilder.withHost("127.0.0.1").withPort(6379);
        return RedisClient.create(uriBuilder.build());
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, SomeValue> dataConnection(RedisClient redisClient, SomeValueCodec codec) {
        return redisClient.connect(codec);
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisPubSubConnection<String, String> pubSubConnection(RedisClient redisClient) {
        return redisClient.connectPubSub();
    }

}
