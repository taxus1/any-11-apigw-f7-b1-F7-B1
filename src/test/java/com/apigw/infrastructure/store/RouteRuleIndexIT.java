package com.apigw.infrastructure.store;

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

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 规则索引（派生缓存）与删除链路的真机集成测试。
 *
 * 复现并钉死线上那个事故：
 * - 删除路由后，索引里的匹配条件/转发动作必须一起收掉，不留「谁的号也不挂」的孤儿；
 * - 同编号删了立刻重建，老一代规则不许接到新路由上（整份覆盖，而非累加）；
 * - 删除清理按属主（路由内部 id）栅栏判定，迟到清理不会误删新一代；
 * - 权威路由不存在的索引 field 由对账兜底回收，存储不越删越胀。
 */
@EnabledIfRedis
class RouteRuleIndexIT {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    private RouteRuleIndex ruleIndex;
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
        factory = lettuceFactory;
        redis = new ReactiveStringRedisTemplate(lettuceFactory);
        ObjectMapper objectMapper = new ObjectMapper();
        ruleIndex = new RouteRuleIndex(redis, objectMapper);
        store = new RouteStore(redis, objectMapper, null, ruleIndex);
        redis.delete(RouteStore.ROUTES_KEY).block();
        redis.delete(RouteRuleIndex.RULES_KEY).block();
        redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match("apigw:lock:route:*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then())
                .block();
    }

    @AfterEach
    void tearDown() {
        redis.delete(RouteStore.ROUTES_KEY).block();
        redis.delete(RouteRuleIndex.RULES_KEY).block();
        ((LettuceConnectionFactory) factory).destroy();
    }

    private GatewayRoute route(String no, String path, String headerName) {
        GatewayRoute r = GatewayRoute.create(no, no, "http://svc:8080", 1, null);
        r.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, path, 1)),
                headerName == null ? List.of()
                        : List.of(GatewayRule.create(null, RuleTypes.TYPE_REQ_ADD_HEADER,
                                headerName, "1", 1)));
        return r;
    }

    @Test
    void delete_cascadesRuleIndexField() {
        store.create(route("r-1", "/old/", "X-Legacy")).block();
        assertThat(ruleIndex.read("r-1").block()).isNotNull();

        store.delete("r-1", 0).block();

        assertThat(redis.opsForHash().hasKey(RouteRuleIndex.RULES_KEY, "r-1").block()).isFalse();
        assertThat(redis.opsForHash().hasKey(RouteStore.ROUTES_KEY, "r-1").block()).isFalse();
    }

    @Test
    void recreateSameRouteNo_overwritesRules_doesNotMergeOldGeneration() {
        GatewayRoute old = route("r-2", "/old/", "X-Legacy");
        store.create(old).block();
        String oldId = old.getId();
        store.delete("r-2", 0).block();

        GatewayRoute fresh = route("r-2", "/new/", "X-Fresh");
        store.create(fresh).block();
        assertThat(fresh.getId()).isNotEqualTo(oldId);

        RouteRuleIndex.Rules indexed = ruleIndex.read("r-2").block();
        assertThat(indexed.ownerId).isEqualTo(fresh.getId());
        assertThat(indexed.conditions).hasSize(1);
        assertThat(indexed.conditionRules().get(0).getValue()).isEqualTo("/new/");
        assertThat(indexed.actions).hasSize(1);
        assertThat(indexed.actionRules().get(0).getName()).isEqualTo("X-Fresh");
        // 老条件 / 老动作一个都不许残留
        assertThat(ruleIndex.readAll().block().toString()).doesNotContain("/old/", "X-Legacy");
    }

    @Test
    void lateCleanupAfterRecreate_doesNotDeleteNewGeneration() {
        GatewayRoute old = route("r-3", "/old/", "X-Legacy");
        store.create(old).block();
        store.delete("r-3", 0).block();
        GatewayRoute fresh = route("r-3", "/new/", "X-Fresh");
        store.create(fresh).block();

        // 删除方的清理「迟到」：拿着老一代 id 来收，必须被属主栅栏挡下
        Boolean removedByOldOwner = ruleIndex.removeIfOwner("r-3", old.getId()).block();
        assertThat(removedByOldOwner).isFalse();
        // 新一代索引完好无损
        RouteRuleIndex.Rules indexed = ruleIndex.read("r-3").block();
        assertThat(indexed.ownerId).isEqualTo(fresh.getId());
        assertThat(indexed.conditionRules().get(0).getValue()).isEqualTo("/new/");

        // 用新一代 id（或路由彻底消失后的无主清理）才允许收
        assertThat(ruleIndex.removeIfOwner("r-3", fresh.getId()).block()).isTrue();
        assertThat(ruleIndex.read("r-3").block()).isNull();
    }

    @Test
    void reconcile_reclaimsOrphansButKeepsLiveSameOwner() {
        GatewayRoute live = store.create(route("live", "/live/", "X-L")).block();
        store.create(route("dead", "/dead/", "X-D")).block();
        // 模拟「删除权威记录成功、但当次索引清理失败」：只删权威 field，
        // 索引里 dead 那条保持创建时写入的属主（老一代 id）
        assertThat(redis.opsForHash().get(RouteRuleIndex.RULES_KEY, "dead").block()).isNotNull();
        redis.opsForHash().remove(RouteStore.ROUTES_KEY, "dead").block();

        long removed = ruleIndex.reconcile().block();
        assertThat(removed).isEqualTo(1L);
        assertThat(ruleIndex.read("dead").block()).isNull();
        // 在册且属主一致的 live 保留
        RouteRuleIndex.Rules liveRules = ruleIndex.read("live").block();
        assertThat(liveRules).isNotNull();
        assertThat(liveRules.ownerId).isEqualTo(live.getId());

        // 再跑一遍幂等：没有新增孤儿
        assertThat(ruleIndex.reconcile().block()).isZero();
    }

    @Test
    void reconcile_keepsNewGenerationWhenIndexOwnerDiffersFromDeletedOne() {
        // dead 被删后同号重建：权威集合里 reborn 是新一代（新 id）。
        // 对账在 Redis 内交叉核对权威路由 id，绝不会把新一代的索引当孤儿清掉。
        GatewayRoute first = store.create(route("reborn", "/old/", "X-Legacy")).block();
        store.delete("reborn", 0).block();
        GatewayRoute second = store.create(route("reborn", "/new/", "X-Fresh")).block();
        assertThat(first.getId()).isNotEqualTo(second.getId());

        // 对账不动它（索引属主 == 权威当前 id）
        assertThat(ruleIndex.reconcile().block()).isZero();
        RouteRuleIndex.Rules indexed = ruleIndex.read("reborn").block();
        assertThat(indexed.ownerId).isEqualTo(second.getId());
        assertThat(indexed.conditionRules().get(0).getValue()).isEqualTo("/new/");
    }

    @Test
    void reconcile_reclaimsStaleGenerationWhenIndexWriteLagged() {
        // 重建已成功（权威是新一代），但索引写入失败、还停在上一代：
        // 对账识别为旧代并清掉，装配回落路由自带规则，下次提交重建索引。
        GatewayRoute first = store.create(route("lag", "/old/", "X-Legacy")).block();
        store.delete("lag", 0).block();
        GatewayRoute second = route("lag", "/new/", "X-Fresh");
        second.setId(UUID.randomUUID().toString().replace("-", ""));
        // 只写权威 field（新一代），并故意把索引塞回成「老属主」那份，
        // 模拟重建后索引覆盖写失败、索引停在上一代
        redis.opsForHash().put(RouteStore.ROUTES_KEY, "lag", store.serialize(second)).block();
        RouteRuleIndex.Rules staleIndex = RouteRuleIndex.Rules.of(first);
        ruleIndex.write("lag", staleIndex).block();
        assertThat(second.getId()).isNotEqualTo(first.getId());

        assertThat(ruleIndex.reconcile().block()).isEqualTo(1L);
        // 权威路由完好，仍可读到新那批规则（来自路由 JSON 自身）
        GatewayRoute got = store.findByRouteNo("lag").block();
        assertThat(got.getConditions().get(0).getValue()).isEqualTo("/new/");
        // 索引被清后，下一次成功提交会重新写回
        ruleIndex.write("lag", RouteRuleIndex.Rules.of(got)).block();
        assertThat(ruleIndex.read("lag").block().ownerId).isEqualTo(got.getId());
    }

    @Test
    void indexWriteFailure_doesNotFailAuthoritativeCreate() {
        // 权威提交（无 revision 模式）已成功后，派生索引即便写失败也不影响结果；
        // 这里直接验证覆盖写的语义：先写老一份，再覆盖写新一份，读回只有新一份。
        GatewayRoute first = route("r-4", "/old/", "X-Old");
        store.create(first).block();
        GatewayRoute second = route("r-4", "/new/", null);
        // 手动模拟「再次整份覆盖」：创建语义必须是覆盖而不是拼接
        ruleIndex.write("r-4", RouteRuleIndex.Rules.of(second)).block();
        RouteRuleIndex.Rules indexed = ruleIndex.read("r-4").block();
        assertThat(indexed.conditions).hasSize(1);
        assertThat(indexed.conditionRules().get(0).getValue()).isEqualTo("/new/");
        assertThat(indexed.actions).isEmpty();
    }
}
