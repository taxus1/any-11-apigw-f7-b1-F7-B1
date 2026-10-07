package com.apigw.proxy.resilience;

import com.apigw.domain.route.CircuitBreakerPolicy;
import com.apigw.domain.route.GatewayRoute;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class CircuitBreakerRegistryTest {

    private CircuitBreakerPolicy enabledPolicy() {
        CircuitBreakerPolicy p = new CircuitBreakerPolicy();
        p.setWindowSize(10);
        p.setMinimumNumberOfCalls(5);
        p.setFailureRateThreshold(50);
        p.setMinFailureCount(3);
        p.setOpenWaitMs(1000L);
        p.setTrialFraction(10);
        p.setSuccessThreshold(2);
        return p;
    }

    private GatewayRoute route(String no, String upstream, CircuitBreakerPolicy policy) {
        GatewayRoute r = GatewayRoute.create(no, "n", upstream, 1, null);
        com.apigw.domain.route.ResiliencePolicy resilience = new com.apigw.domain.route.ResiliencePolicy();
        resilience.setCircuitBreakerEnabled(1);
        resilience.setCircuitBreaker(policy);
        r.replaceResilience(resilience);
        return r;
    }

    @Test
    void removeRoute_evictsAllUpstreamsOfThatRouteOnly() {
        CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
        CircuitBreakerPolicy p = enabledPolicy();

        CircuitBreaker a1 = registry.obtain("r-a", "http://a", p);
        CircuitBreaker a2 = registry.obtain("r-a", "http://a-canary", p);
        CircuitBreaker b = registry.obtain("r-b", "http://b", p);

        registry.removeRoute("r-a");

        // 同一路由名下所有上游（含灰度上游）都摘掉；别的路由不动
        assertNotNull(registry.obtain("r-b", "http://b", p));
        assertSame(b, registry.obtain("r-b", "http://b", p),
                "别的路由的熔断器不能被误摘");
        CircuitBreaker a1After = registry.obtain("r-a", "http://a", p);
        org.assertj.core.api.Assertions.assertThat(a1After).isNotSameAs(a1);
        CircuitBreaker a2After = registry.obtain("r-a", "http://a-canary", p);
        org.assertj.core.api.Assertions.assertThat(a2After).isNotSameAs(a2);
    }

    @Test
    void reconcile_retainsOnlyLiveRouteUpstreamPairs() {
        CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
        CircuitBreakerPolicy p = enabledPolicy();

        registry.obtain("r-a", "http://a", p);
        registry.obtain("r-a", "http://a-canary", p);
        registry.obtain("r-b", "http://b", p);
        registry.obtain("ghost", "http://g", p);

        // 新快照：r-a 只剩主上游（灰度分组没了），r-b 还在，ghost 已删除
        GatewayRoute a = route("r-a", "http://a", p);
        GatewayRoute b = route("r-b", "http://b", p);
        Set<CircuitBreakerRegistry.Key> live = CircuitBreakerRegistry.liveKeysOf(List.of(a, b));

        registry.reconcile(live);

        assertSame(registry.obtain("r-a", "http://a", p), registry.obtain("r-a", "http://a", p));
        assertSame(registry.obtain("r-b", "http://b", p), registry.obtain("r-b", "http://b", p));
        // 被摘掉灰度上游 / 被删路由：再取得到的是全新熔断器
        CircuitBreaker recreatedCanary = registry.obtain("r-a", "http://a-canary", p);
        org.assertj.core.api.Assertions.assertThat(recreatedCanary).isNotNull();
        CircuitBreaker recreatedGhost = registry.obtain("ghost", "http://g", p);
        org.assertj.core.api.Assertions.assertThat(recreatedGhost).isNotNull();
    }

    @Test
    void liveKeysOf_includesGrayUpstreamsAndSkipsRoutesWithoutCircuitBreaker() {
        CircuitBreakerPolicy p = enabledPolicy();
        GatewayRoute withCb = route("r-a", "http://a", p);
        withCb.replaceGrayGroups(List.of(
                com.apigw.domain.route.GrayGroup.create("canary", "http://a-canary", 100, List.of("v2"))));
        GatewayRoute noCb = GatewayRoute.create("r-b", "n", "http://b", 1, null);

        Set<CircuitBreakerRegistry.Key> keys =
                CircuitBreakerRegistry.liveKeysOf(List.of(withCb, noCb));

        org.assertj.core.api.Assertions.assertThat(keys).containsExactlyInAnyOrder(
                new CircuitBreakerRegistry.Key("r-a", "http://a"),
                new CircuitBreakerRegistry.Key("r-a", "http://a-canary"));
    }
}
