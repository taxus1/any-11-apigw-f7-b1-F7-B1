package com.apigw.infrastructure.store;

import com.apigw.support.EnabledIfRedis;
import com.apigw.support.RedisAvailableCondition;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.resource.ClientResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 屏障（协调运行时状态）生命周期的真机测试：
 * - PREPARING 建立时整套 barrier/ready/checksum 都带 TTL，进程全挂也不会永久残留；
 * - 已尘埃落定的老 revision，pruneSettledBefore 会成套回收，只留保留窗口内的；
 * - 回收严格按 revision 单调性，不会误删可能还要追赶/查询的新屏障。
 */
@EnabledIfRedis
class RouteBarrierStoreLifecycleIT {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    private RouteBarrierStore barriers;

    @BeforeEach
    void setUp() {
        var clientConfig = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.create())
                .build();
        var connConfig = new org.springframework.data.redis.connection.RedisStandaloneConfiguration(
                RedisAvailableCondition.host(), RedisAvailableCondition.port());
        var lettuceFactory = new LettuceConnectionFactory(connConfig, clientConfig);
        lettuceFactory.afterPropertiesSet();
        factory = lettuceFactory;
        redis = new ReactiveStringRedisTemplate(lettuceFactory);
        barriers = new RouteBarrierStore(redis, new ObjectMapper());
        cleanup();
    }

    @AfterEach
    void tearDown() {
        cleanup();
        ((LettuceConnectionFactory) factory).destroy();
    }

    private void cleanup() {
        redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match(RouteRevisionStore.BARRIER_PREFIX + "*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then())
                .block();
    }

    @Test
    void preparing_setsTtlOnBarrierAndReadyFamily() {
        long rev = 100;
        barriers.createPreparing(rev, "sha256:x", java.time.Instant.now().toString()).block();
        barriers.markReady(rev, "{\"instanceId\":\"gw-0\"}", "sha256:x").block();

        // markReady 后 ready/checksum 都有成员，屏障主 key 必然存在，三者都应有 TTL
        for (String key : List.of(RouteBarrierStore.barrierKey(rev),
                RouteBarrierStore.readyKey(rev),
                RouteBarrierStore.checksumKey(rev))) {
            java.time.Duration ttl = redis.getExpire(key).block();
            assertThat(ttl).as(key + " 必须存在且有 TTL").isNotNull();
            assertThat(ttl.getSeconds())
                    .as(key + " TTL 应落在 7 天兜底窗口")
                    .isBetween(Duration.ofDays(6).getSeconds(), Duration.ofDays(7).getSeconds());
        }
    }

    @Test
    void prune_removesSettledOldRevisions_butKeepsRetainedAndNewer() {
        long[] revs = {10, 20, 30};
        for (long rev : revs) {
            barriers.createPreparing(rev, "sha:" + rev, java.time.Instant.now().toString()).block();
            barriers.markReady(rev, "{\"instanceId\":\"gw-0\"}", "sha:" + rev).block();
        }
        // active 已到 30；保留窗口里留着 20、30（实例短暂落后还要追赶/查询）。
        // rev=10 的屏障主 key + ready + checksum 三套应被回收（leader/fenced 本场景没创建）。
        long removed = barriers.pruneSettledBefore(30, Set.of(20L, 30L)).block();
        assertThat(removed).isGreaterThanOrEqualTo(3L);

        assertThat(redis.hasKey(RouteBarrierStore.barrierKey(10)).block()).isFalse();
        assertThat(redis.hasKey(RouteBarrierStore.readyKey(10)).block()).isFalse();
        assertThat(redis.hasKey(RouteBarrierStore.checksumKey(10)).block()).isFalse();
        assertThat(redis.hasKey(RouteBarrierStore.barrierKey(20)).block()).isTrue();
        assertThat(redis.hasKey(RouteBarrierStore.barrierKey(30)).block()).isTrue();

        // 再跑幂等：没有可回收的
        assertThat(barriers.pruneSettledBefore(30, Set.of(20L, 30L)).block()).isZero();
    }
}
