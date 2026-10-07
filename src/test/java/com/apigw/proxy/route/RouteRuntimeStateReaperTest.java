package com.apigw.proxy.route;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GrayGroup;
import com.apigw.proxy.gray.GrayProperties;
import com.apigw.proxy.gray.GrayReleaseSelector;
import com.apigw.proxy.resilience.CircuitBreaker;
import com.apigw.proxy.resilience.CircuitBreakerRegistry;
import com.apigw.domain.route.CircuitBreakerPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 快照激活后的本机派生状态对账：删除路由名下的熔断器与灰度计划都要收干净，
 * 存活路由的状态保留。
 */
class RouteRuntimeStateReaperTest {

    private CircuitBreakerPolicy policy() {
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

    private GatewayRoute routeWithBreaker(String no, String upstream) {
        GatewayRoute r = GatewayRoute.create(no, "n", upstream, 1, null);
        com.apigw.domain.route.ResiliencePolicy resilience = new com.apigw.domain.route.ResiliencePolicy();
        resilience.setCircuitBreakerEnabled(1);
        resilience.setCircuitBreaker(policy());
        r.replaceResilience(resilience);
        return r;
    }

    @Test
    void reconcile_evictsDeletedRouteState_andKeepsLiveState() {
        CircuitBreakerRegistry breakers = new CircuitBreakerRegistry();
        GrayReleaseSelector gray = new GrayReleaseSelector(new GrayProperties(null));
        RouteRuntimeStateReaper reaper = new RouteRuntimeStateReaper(breakers, gray);

        GatewayRoute live = routeWithBreaker("r-live", "http://live");
        GatewayRoute dead = routeWithBreaker("r-dead", "http://dead");
        CircuitBreaker liveBreaker = breakers.obtain("r-live", "http://live", policy());
        CircuitBreaker deadBreaker = breakers.obtain("r-dead", "http://dead", policy());

        GatewayRoute grayDead = GatewayRoute.create("r-gray-dead", "n", "http://g", 1, null);
        grayDead.replaceGrayGroups(List.of(GrayGroup.create("s", "http://g", 100, List.of())));
        gray.select(grayDead, MockServerHttpRequest.get("/a").build());
        assertThat(gray.cachedPlanCount()).isEqualTo(1);

        // 新快照：dead 两条都消失，live 还在
        reaper.reconcile(List.of(live));

        CircuitBreaker liveNow = breakers.obtain("r-live", "http://live", policy());
        CircuitBreaker deadNow = breakers.obtain("r-dead", "http://dead", policy());
        assertThat(liveNow).isSameAs(liveBreaker);
        assertThat(deadNow).isNotSameAs(deadBreaker);
        assertThat(gray.cachedPlanCount()).isZero();
    }

    @Test
    void reconcile_swallowsErrors_doesNotPropagate() {
        CircuitBreakerRegistry breakers = new CircuitBreakerRegistry();
        GrayReleaseSelector gray = new GrayReleaseSelector(new GrayProperties(null));
        RouteRuntimeStateReaper reaper = new RouteRuntimeStateReaper(breakers, gray);

        // 入参为 null 也不能抛到转发/协调链路里（下次激活会再次对账）
        reaper.reconcile(null);
    }
}
