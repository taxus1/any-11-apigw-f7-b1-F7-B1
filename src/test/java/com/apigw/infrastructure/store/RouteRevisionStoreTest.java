package com.apigw.infrastructure.store;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RuleTypes;
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
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfRedis
class RouteRevisionStoreTest {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    private RouteStore store;
    private RouteRevisionStore revisions;

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
        revisions = new RouteRevisionStore(redis);
        store = new RouteStore(redis, new ObjectMapper(), revisions);
        cleanupKeys().block();
    }

    @AfterEach
    void tearDown() {
        cleanupKeys().block();
        ((LettuceConnectionFactory) factory).destroy();
    }

    private reactor.core.publisher.Mono<Void> cleanupKeys() {
        return redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match("apigw:route*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then());
    }

    private GatewayRoute route(String no, Integer version) {
        GatewayRoute r = GatewayRoute.create(no, "名称-" + no,
                "http://order-svc:8080", 1, null);
        r.setVersion(version);
        r.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/order/", 1)),
                List.of(GatewayRule.create(null, RuleTypes.TYPE_REQ_ADD_HEADER, "X-Gw", "1", 1)));
        return r;
    }

    @Test
    void commitCreate_updatesRevisionAndCreatesImmutableSnapshot() {
        store.create(route("order-rev-1", 99)).block();

        assertEquals(1L, revisions.latestRevision().block());
        Map<String, String> snapshot = revisions.loadSnapshot(1).block();
        assertEquals(1, snapshot.size());
        assertTrue(snapshot.containsKey("order-rev-1"));
    }

    @Test
    void commits_keepEachRevisionSnapshotAndDoNotMixConfigurations() {
        store.create(route("order-rev-2", 0)).block();
        GatewayRoute changed = route("order-rev-2", 0);
        changed.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/pay/", 1)),
                List.of());
        store.update(changed).block();
        store.delete("order-rev-2", 1).block();

        assertEquals(3L, revisions.latestRevision().block());
        assertEquals(1, revisions.loadSnapshot(1).block().size());
        assertEquals(1, revisions.loadSnapshot(2).block().size());
        assertEquals(0, revisions.loadSnapshot(3).block().size());
        String revisionTwoJson = revisions.loadSnapshot(2).block().get("order-rev-2");
        assertTrue(revisionTwoJson.contains("\\/pay\\/") || revisionTwoJson.contains("/pay/"));
    }

    @Test
    void commitDelete_withoutVersion_isRejectedBeforeTouchingRevision() {
        store.create(route("order-rev-4", 0)).block();
        BizException error = assertThrows(BizException.class,
                () -> store.delete("order-rev-4", null).block());
        assertTrue(error.getMessage().contains("删除必须带上读取时拿到的版本号"));
        // 删除被拒，revision 不前进，路由还在
        assertEquals(1L, revisions.latestRevision().block());
        assertTrue(store.findByRouteNo("order-rev-4").block() != null);
    }

    @Test
    void commitDelete_atomicallyRemovesRuleIndexProjection() {
        RouteRuleIndex ruleIndex = new RouteRuleIndex(redis, new ObjectMapper());
        RouteStore indexedStore = new RouteStore(redis, new ObjectMapper(), revisions, ruleIndex);

        indexedStore.create(route("order-rev-5", 0)).block();
        assertEquals(1, ruleIndex.read("order-rev-5").block().conditions.size());

        indexedStore.delete("order-rev-5", 0).block();

        // 权威 field 与派生投影同一次 Lua 提交清掉，不存在无主记录，也不会污染同编号重建
        StepVerifier.create(ruleIndex.read("order-rev-5")).verifyComplete();
        StepVerifier.create(indexedStore.findByRouteNo("order-rev-5")).verifyComplete();

        // 立刻同编号重建：投影只有新规则，老规则不会冒出来
        GatewayRoute fresh = route("order-rev-5", 0);
        fresh.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/new/", 1)),
                List.of());
        indexedStore.create(fresh).block();
        assertEquals(1, ruleIndex.read("order-rev-5").block().conditions.size());
        assertEquals("/new/", ruleIndex.read("order-rev-5").block().conditions.get(0).value);
    }

    @Test
    void commitUpdate_staleVersionDoesNotAdvanceRevision() {
        store.create(route("order-rev-3", 0)).block();
        GatewayRoute first = route("order-rev-3", 0);
        store.update(first).block();

        GatewayRoute stale = route("order-rev-3", 0);
        BizException error = assertThrows(BizException.class,
                () -> store.update(stale).block());
        assertEquals(409, error.getCode());
        assertEquals(2L, revisions.latestRevision().block());
    }
}
