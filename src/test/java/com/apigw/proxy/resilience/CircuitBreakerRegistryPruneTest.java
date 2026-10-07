package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.GrayGroup;
import com.apigw.domain.route.ResiliencePolicy;
import com.apigw.domain.route.RuleTypes;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 熔断器登记表的快照对账：路由是派生运行时状态的属主。
 * 新快照里消失的路由/灰度上游，其本机熔断器必须被回收，不能越攒越多；
 * 同编号重建是新配置重新 obtain，旧条目不跨代复用。
 */
class CircuitBreakerRegistryPruneTest {

    private CircuitBreakerPolicy cbPolicy() {
        CircuitBreakerPolicy cb = new CircuitBreakerPolicy();
        cb.setWindowSize(10);
        cb.setMinimumNumberOfCalls(5);
        cb.setFailureRateThreshold(50);
        cb.setMinFailureCount(5);
        cb.setOpenWaitMs(1_000L);
        cb.setTrialFraction(100);
        cb.setSuccessThreshold(1);
        return cb;
    }

    private GatewayRoute routeWithBreaker(String no, String upstream) {
        GatewayRoute r = GatewayRoute.create(no, no, upstream, 1, null);
        r.replaceRules(List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/" + no + "/", 1)),
                List.of());
        ResiliencePolicy p = new ResiliencePolicy();
        p.setCircuitBreakerEnabled(1);
        p.setCircuitBreaker(cbPolicy());
        r.replaceResilience(p);
        return r;
    }

    @Test
    void prune_removesBreakersOfDeletedRoutesAndGrayUpstreams() {
        CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
        GatewayRoute a = routeWithBreaker("a", "http://a:8080");
        GatewayRoute b = routeWithBreaker("b", "http://b:8080");
        // b 还带一个灰度分组上游
        b.replaceGrayGroups(List.of(
                GrayGroup.create("stable", "http://b:8080", 90, List.of()),
                GrayGroup.create("canary", "http://b-canary:8080", 10, List.of())));

        registry.obtain("a", "http://a:8080", cbPolicy());
        registry.obtain("b", "http://b:8080", cbPolicy());
        registry.obtain("b", "http://b-canary:8080", cbPolicy());
        assertThat(registry.size()).isEqualTo(3);

        // 新快照：a 被删；b 保留但去掉了 canary 灰度组
        GatewayRoute bNow = routeWithBreaker("b", "http://b:8080");
        int pruned = registry.pruneAbsent(List.of(bNow));

        assertThat(pruned).isEqualTo(2);
        assertThat(registry.size()).isEqualTo(1);
        // 留下的就是 b 主上游；a 已不在集合里（再次对账也不会有它的条目）
        assertThat(registry.obtain("b", "http://b:8080", cbPolicy())).isNotNull();
        assertThat(registry.pruneAbsent(List.of(bNow))).isZero();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void prune_keepsBreakersWhenRouteSetUnchanged() {
        CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
        GatewayRoute a = routeWithBreaker("a", "http://a:8080");
        registry.obtain("a", "http://a:8080", cbPolicy());

        assertThat(registry.pruneAbsent(List.of(a))).isZero();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void prune_dropsBreakerWhenRouteDisablesCircuitBreaking() {
        CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
        GatewayRoute a = routeWithBreaker("a", "http://a:8080");
        registry.obtain("a", "http://a:8080", cbPolicy());

        // 同一路由关掉熔断：新快照里它不再产出熔断策略，旧条目也应回收
        GatewayRoute noBreaker = GatewayRoute.create("a", "a", "http://a:8080", 1, null);
        noBreaker.replaceRules(
                List.of(GatewayRule.create(null, RuleTypes.TYPE_PATH_PREFIX, null, "/a/", 1)),
                List.of());

        assertThat(registry.pruneAbsent(List.of(noBreaker))).isEqualTo(1);
        assertThat(registry.size()).isZero();
    }
}
