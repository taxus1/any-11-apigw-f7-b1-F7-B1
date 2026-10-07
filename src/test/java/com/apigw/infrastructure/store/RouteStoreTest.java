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
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RouteStore 的真机集成测试（连真实 Redis，不用 mock）。
 *
 * 重点验证题目里的硬要求：
 * - 整树存取：一条路由连同全部条件/动作一次落库、一次读回，没有半截数据；
 * - 编号占用：HSETNX 语义，停用也占号，并发建同号只有一个成功；
 * - 乐观锁：不带版本不让改，旧版本提交被拒并提示「你这份旧了」，版本号自增；
 * - 删除：不存在给明确失败（404），存在则整树消失。
 */
@EnabledIfRedis
class RouteStoreTest {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    private RouteStore store;

    @BeforeEach
    void setUp() {
        var clientConfig = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.create())
                .build();
        var connConfig = new org.springframework.data.redis.connection.RedisStandaloneConfiguration(
                RedisAvailableCondition.host(), RedisAvailableCondition.port());
        var lettuceFactory = new LettuceConnectionFactory(connConfig, clientConfig);
        lettuceFactory.afterPropertiesSet();
        this.factory = lettuceFactory;
        this.redis = new ReactiveStringRedisTemplate(lettuceFactory);
        this.store = new RouteStore(redis, new ObjectMapper());
        // 每个用例从空配置开始，避免相互污染
        this.redis.delete(RouteStore.ROUTES_KEY).block();
    }

    @AfterEach
    void tearDown() {
        this.redis.delete(RouteStore.ROUTES_KEY).block();
        // 清掉可能残留的锁
        this.redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match("apigw:lock:route:*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : this.redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then())
                .block();
        ((LettuceConnectionFactory) factory).destroy();
    }

    private GatewayRoute route(String routeNo, Integer version) {
        GatewayRoute r = GatewayRoute.create(routeNo, "名称-" + routeNo,
                "http://order-svc:8080", 1, "备注");
        r.setVersion(version);
        r.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/order/", 1),
                        GatewayRule.create(null, RuleTypes.TYPE_METHOD, null, "GET", 2)),
                List.of(GatewayRule.create(null, RuleTypes.TYPE_REQ_ADD_HEADER, "X-Gw", "1", 1),
                        GatewayRule.create(null, RuleTypes.TYPE_RESP_REMOVE_HEADER, "X-Internal", null, 2)));
        return r;
    }

    @Test
    void create_thenReadBack_wholeTreeIntact() {
        GatewayRoute saved = store.create(route("order-01", 99)).block();
        assertEquals(0, saved.getVersion(), "新建后版本号必须归 0，不理会入参");

        GatewayRoute got = store.findByRouteNo("order-01").block();
        assertNotNull(got);
        assertEquals("http://order-svc:8080", got.getUpstream());
        assertEquals(2, got.getConditions().size());
        assertEquals(2, got.getActions().size());
        // 顺序号、类型、删头动作的值归一化都要原样读回
        assertEquals(RuleTypes.TYPE_PATH_PREFIX, got.getConditions().get(0).getType());
        assertEquals(RuleTypes.TYPE_RESP_REMOVE_HEADER, got.getActions().get(1).getType());
        assertEquals(null, got.getActions().get(1).getValue());
    }

    @Test
    void create_duplicateRouteNo_isRejected_evenWhenDisabled() {
        store.create(route("order-02", 0)).block();
        GatewayRoute disabled = route("order-02", 0);
        disabled.changeEnabled(0);
        StepVerifier.create(store.create(disabled))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof BizException);
                    assertTrue(e.getMessage().contains("路由编号已被占用"), e.getMessage());
                })
                .verify();
    }

    @Test
    void create_duplicateRouteNo_underConcurrency_onlyOneWins() {
        GatewayRoute a = route("race-01", 0);
        GatewayRoute b = route("race-01", 0);
        long success = reactor.core.publisher.Flux.merge(store.create(a).thenReturn(1L).onErrorReturn(0L),
                        store.create(b).thenReturn(1L).onErrorReturn(0L))
                .reduce(0L, Long::sum)
                .block();
        assertEquals(1L, success, "并发建同号必须只有一个成功");
        assertEquals(1L, store.findAll().count().block());
    }

    @Test
    void update_withoutVersion_isRejected() {
        store.create(route("order-03", 0)).block();
        GatewayRoute stale = route("order-03", null);
        StepVerifier.create(store.update(stale))
                .expectErrorSatisfies(e -> assertTrue(e.getMessage().contains("必须带上读取时拿到的版本号")))
                .verify();
    }

    @Test
    void update_staleVersion_isRejected_andReportsCurrentVersion() {
        store.create(route("order-04", 0)).block();
        // 先有一次成功的修改，版本推进到 1
        GatewayRoute first = route("order-04", 0);
        first.rename("新名称-第一次改");
        assertEquals(1, store.update(first).block().getVersion());

        // 另一个人还拿着版本 0 来改
        GatewayRoute stale = route("order-04", 0);
        stale.rename("新名称-拿着旧版改");
        StepVerifier.create(store.update(stale))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof BizException);
                    assertEquals(409, ((BizException) e).getCode());
                    assertTrue(e.getMessage().contains("你这份配置已经旧了"), e.getMessage());
                    assertTrue(e.getMessage().contains("当前版本 1"), e.getMessage());
                })
                .verify();

        // 被拒的那次不能留下任何痕迹
        assertEquals("新名称-第一次改", store.findByRouteNo("order-04").block().getName());
    }

    @Test
    void update_wholeTreeReplacesConditionsAndActions() {
        store.create(route("order-05", 0)).block();
        GatewayRoute changed = route("order-05", 0);
        // 整批重排/替换：条件只留 1 条，动作换成 3 条
        changed.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/pay/", 1)),
                List.of(GatewayRule.create(null, RuleTypes.TYPE_REQ_ADD_HEADER, "X-A", "1", 1),
                        GatewayRule.create(null, RuleTypes.TYPE_REQ_REMOVE_HEADER, "X-B", null, 2),
                        GatewayRule.create(null, RuleTypes.TYPE_RESP_ADD_HEADER, "X-C", "2", 3)));
        GatewayRoute saved = store.update(changed).block();
        assertEquals(1, saved.getVersion());
        GatewayRoute got = store.findByRouteNo("order-05").block();
        assertEquals(1, got.getConditions().size());
        assertEquals(3, got.getActions().size());
        assertEquals("/pay/", got.getConditions().get(0).getValue());
        // 旧的两条条件不能残留（单 field 整树覆盖，天然无孤儿）
    }

    @Test
    void update_notExists_isRejected() {
        StepVerifier.create(store.update(route("missing-01", 0)))
                .expectErrorSatisfies(e -> assertTrue(e.getMessage().contains("路由不存在")))
                .verify();
    }

    @Test
    void delete_removesWholeTree() {
        store.create(route("order-06", 0)).block();
        store.delete("order-06", 0).block();
        StepVerifier.create(store.findByRouteNo("order-06"))
                .verifyComplete();
        assertEquals(0L, store.findAll().count().block());
    }

    @Test
    void delete_withoutVersion_isRejected() {
        store.create(route("order-08", 0)).block();
        StepVerifier.create(store.delete("order-08", null))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof BizException);
                    assertTrue(e.getMessage().contains("删除必须带上读取时拿到的版本号"), e.getMessage());
                })
                .verify();
        // 被拒的删除不能真删
        assertNotNull(store.findByRouteNo("order-08").block());
    }

    @Test
    void delete_thenRecreateSameRouteNo_startsClean_noLeftoverChildren() {
        // 老路由：2 条件 + 2 动作
        GatewayRoute old = route("re-01", 0);
        String oldId = old.getId();
        store.create(old).block();
        store.delete("re-01", 0).block();

        // 同编号重建：只带 1 条件 + 1 动作，内容完全不同
        GatewayRoute fresh = GatewayRoute.create("re-01", "重建", "http://pay-svc:9090", 1, null);
        // id 不接受客户端指定：create 统一由服务端重新分配，保证新一代与老路由属主不同
        String clientGivenId = "new-id-re-01";
        fresh.setId(clientGivenId);
        fresh.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/pay/", 1)),
                List.of(GatewayRule.create(null, RuleTypes.TYPE_REQ_REMOVE_HEADER, "X-Old", null, 1)));
        GatewayRoute saved = store.create(fresh).block();

        GatewayRoute got = store.findByRouteNo("re-01").block();
        assertNotNull(got);
        // 老的条件/动作一条都不许冒出来：读回的必须恰好是新建那一批
        assertEquals(1, got.getConditions().size());
        assertEquals("/pay/", got.getConditions().get(0).getValue());
        assertEquals(1, got.getActions().size());
        assertEquals("X-Old", got.getActions().get(0).getName());
        assertEquals("http://pay-svc:9090", got.getUpstream());
        // 版本从 0 重新计；id 是服务端分配的新值，既不等于老路由，也不理会客户端自带的那个
        assertEquals(0, got.getVersion());
        assertNotEquals(clientGivenId, got.getId());
        assertEquals(saved.getId(), got.getId());
        assertTrue(oldId == null || !oldId.equals(got.getId()));
    }

    @Test
    void delete_notExists_reports404_notSilentSuccess() {
        StepVerifier.create(store.delete("ghost", 0))
                .expectErrorSatisfies(e -> {
                    assertTrue(e instanceof BizException);
                    assertEquals(404, ((BizException) e).getCode());
                    assertTrue(e.getMessage().contains("路由不存在，删除未执行"));
                })
                .verify();
    }

    @Test
    void delete_staleVersion_isRejected() {
        store.create(route("order-07", 0)).block();
        GatewayRoute changed = route("order-07", 0);
        store.update(changed).block(); // 版本推进到 1
        StepVerifier.create(store.delete("order-07", 0))
                .expectErrorSatisfies(e -> assertEquals(409, ((BizException) e).getCode()))
                .verify();
        // 没删掉
        assertNotNull(store.findByRouteNo("order-07").block());
    }
}
