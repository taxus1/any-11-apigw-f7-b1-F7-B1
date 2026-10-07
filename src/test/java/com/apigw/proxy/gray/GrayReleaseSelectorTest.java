package com.apigw.proxy.gray;

import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GrayGroup;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/**
 * 灰度分流选择器测试（不启容器：请求用 {@link MockServerHttpRequest}）。
 *
 * 覆盖硬性口径：
 * - 无灰度分组 → null（旧链路零变化）；
 * - 标记精确命中 → 直达目标组，权重再小、甚至为 0 也不被权重分走；
 * - 标记没带/值不对/大小写不一致/多空格 → 一律当没带，落权重；
 * - 无标记按平滑加权轮询：长期比例严格等于权重（每 100 次恰好），
 *   每组正权重都拿得到量，0 权重组永远拿不到；
 * - 极端 100/0、0/100 行为确定；
 * - 快照 10s 兜底重载产生同版本新对象时计数器延续，小权重组在低流量下也不会永远选不上；
 *   配置一改（版本 +1）立刻按新计划。
 */
class GrayReleaseSelectorTest {

    private final GrayReleaseSelector selector = new GrayReleaseSelector(new GrayProperties(null));

    private GatewayRoute route(int version, List<GrayGroup> groups) {
        GatewayRoute r = GatewayRoute.create("gray-r", "灰度", "http://stable:8080", 1, null);
        r.setVersion(version);
        r.replaceGrayGroups(groups);
        return r;
    }

    private GrayGroup g(String name, String host, int weight, String... tags) {
        return GrayGroup.create(name, "http://" + host + ":8080", weight,
                tags.length == 0 ? List.of() : List.of(tags));
    }

    private MockServerHttpRequest request(String tagValue) {
        MockServerHttpRequest.BaseBuilder<?> b = MockServerHttpRequest.get("/order/1");
        if (tagValue != null) {
            b.header(GrayProperties.DEFAULT_TAG_HEADER, tagValue);
        }
        return b.build();
    }

    @Test
    void noGrayGroups_returnsNull() {
        GatewayRoute r = route(0, null);
        assertThat(selector.select(r, request(null))).isNull();
        // 显式空列表同样不介入
        assertThat(selector.select(route(0, List.of()), request("v2"))).isNull();
    }

    @Test
    void tag_pinsToTaggedGroup_evenWhenItsWeightIsZero() {
        GatewayRoute r = route(0, List.of(
                g("stable", "stable", 100),
                // 新版权重 0：无标记请求一个都不会去，但带对标记的请求必须稳稳落到它
                g("canary", "canary", 0, "v2")));

        for (int i = 0; i < 20; i++) {
            GrayTarget t = selector.select(r, request("v2"));
            assertThat(t.groupName()).isEqualTo("canary");
            assertThat(t.upstream()).isEqualTo("http://canary:8080");
            assertThat(t.byTag()).isTrue();
        }
    }

    @Test
    void tag_tinyWeightStillWinsEveryTime() {
        GatewayRoute r = route(0, List.of(
                g("stable", "stable", 99),
                g("canary", "canary", 1, "v2")));
        // 权重 1/99 也分不走带标的请求
        for (int i = 0; i < 50; i++) {
            assertThat(selector.select(r, request("v2")).groupName()).isEqualTo("canary");
        }
    }

    @Test
    void wrongTagValues_fallBackToWeight() {
        GatewayRoute r = route(0, List.of(
                g("stable", "stable", 100),
                g("canary", "canary", 0, "v2")));

        // 没带、大小写不一致、前后多空格、看着像对不上——全部当没带标记
        // 权重是 100/0，所以全部稳定落 stable
        for (String bad : new String[]{null, "", "V2", " v2", "v2 ", "v2\n", "v"}) {
            GrayTarget t = selector.select(r, request(bad));
            assertThat(t.groupName())
                    .as("标记值 %s 应被当作没带，回落权重落到 stable", bad)
                    .isEqualTo("stable");
            assertThat(t.byTag()).isFalse();
        }
    }

    @Test
    void weightTenNinety_isExactOver100Requests_andSmooth() {
        GatewayRoute r = route(0, List.of(
                g("stable", "stable", 90),
                g("canary", "canary", 10, "v2")));

        Map<String, Integer> counts = new TreeMap<>();
        boolean canaryGotTraffic = false;
        for (int i = 0; i < 100; i++) {
            GrayTarget t = selector.select(r, request(null));
            counts.merge(t.groupName(), 1, Integer::sum);
            if (t.groupName().equals("canary")) {
                canaryGotTraffic = true;
            }
        }
        assertThat(counts.get("canary")).isEqualTo(10);
        assertThat(counts.get("stable")).isEqualTo(90);
        assertThat(canaryGotTraffic).isTrue();

        // 「平滑」的含义：10 次选择内至少出现一次新版，不是前 90 次全老版本
        int windowHits = 0;
        for (int i = 0; i < 10; i++) {
            if (selector.select(r, request(null)).groupName().equals("canary")) {
                windowHits++;
            }
        }
        assertThat(windowHits).isGreaterThanOrEqualTo(1);
    }

    @Test
    void weightOneGroup_onePercentGroupStillGetsItsShare_over10k() {
        GatewayRoute r = route(0, List.of(
                g("old", "old", 99),
                g("new", "new", 1, "beta")));
        Map<String, Integer> counts = new TreeMap<>();
        int n = 10_000;
        for (int i = 0; i < n; i++) {
            counts.merge(selector.select(r, request(null)).groupName(), 1, Integer::sum);
        }
        // 长期比例对得上：1% 的组拿到约 100 次（允许 ±2 的取整/节拍误差）
        assertThat(counts.get("new")).isCloseTo(100, offset(2));
        assertThat(counts.get("old")).isCloseTo(9_900, offset(2));
    }

    @Test
    void threeGroups_fiftyThirtyTwenty_exactEachCycle() {
        GatewayRoute r = route(0, List.of(
                g("a", "a", 50),
                g("b", "b", 30),
                g("c", "c", 20)));
        Map<String, Integer> counts = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            counts.merge(selector.select(r, request(null)).groupName(), 1, Integer::sum);
        }
        // 每个 100 窗口各组恰好拿到权重值次：多组也说得清
        assertThat(counts).containsEntry("a", 50).containsEntry("b", 30).containsEntry("c", 20);
    }

    @Test
    void extremes_100to0_and_0to100_areStable() {
        GatewayRoute allOld = route(0, List.of(
                g("old", "old", 100), g("new", "new", 0, "v2")));
        for (int i = 0; i < 30; i++) {
            assertThat(selector.select(allOld, request(null)).groupName()).isEqualTo("old");
        }

        GrayReleaseSelector s2 = new GrayReleaseSelector(new GrayProperties(null));
        GatewayRoute allNew = route(0, List.of(
                g("old", "old", 0), g("new", "new", 100)));
        for (int i = 0; i < 30; i++) {
            assertThat(s2.select(allNew, request(null)).groupName()).isEqualTo("new");
        }
    }

    @Test
    void zeroWeightGroup_neverReceivesWeightedTraffic_butStaysInConfig() {
        GatewayRoute r = route(0, List.of(
                g("a", "a", 60), g("b", "b", 40), g("paused", "paused", 0, "internal")));
        for (int i = 0; i < 500; i++) {
            String name = selector.select(r, request(null)).groupName();
            assertThat(name).isIn("a", "b");
        }
        // 配置留着：标记仍可直达；改权重新版本一到就能接量
        assertThat(selector.select(r, request("internal")).groupName()).isEqualTo("paused");
        assertThat(r.getGrayGroups()).hasSize(3);
    }

    @Test
    void snapshotReloadWithSameVersion_keepsCounterMomentum_lowTraffic() {
        // 低流量场景：路由快照 10s 兜底重载，每次请求拿到的都是「同版本号的新反序列化对象」。
        // 正确行为：计数器按 routeNo+version 复用，节拍延续 —— 10 次请求（=10 个新对象）内新版必出现；
        // 错误实现（每对象重建计数器）会每次都选权重 90 的 old，1%~10% 的灰度永远放不出去。
        int canary = 0;
        for (int i = 0; i < 10; i++) {
            GatewayRoute reloaded = route(7, List.of(
                    g("old", "old", 90), g("new", "new", 10, "v2")));
            if (selector.select(reloaded, request(null)).groupName().equals("new")) {
                canary++;
            }
        }
        assertThat(canary).isGreaterThanOrEqualTo(1);
    }

    @Test
    void configChange_versionBump_switchesPlanImmediately() {
        GatewayRoute v0 = route(0, List.of(
                g("old", "old", 100), g("new", "new", 0, "v2")));
        assertThat(selector.select(v0, request(null)).groupName()).isEqualTo("old");

        // 一键开量：new 权重改 50，保存后版本 +1，新对象经快照进来
        GatewayRoute v1 = route(1, List.of(
                g("old", "old", 50), g("new", "new", 50, "v2")));
        int newCount = 0;
        for (int i = 0; i < 100; i++) {
            if (selector.select(v1, request(null)).groupName().equals("new")) {
                newCount++;
            }
        }
        assertThat(newCount).isEqualTo(50);
        // 标记仍然直达
        assertThat(selector.select(v1, request("v2")).groupName()).isEqualTo("new");
    }

    @Test
    void customTagHeader_isHonored() {
        GrayReleaseSelector s = new GrayReleaseSelector(new GrayProperties("X-Canary"));
        GatewayRoute r = route(0, List.of(
                g("stable", "stable", 100), g("canary", "canary", 0, "v2")));
        MockServerHttpRequest req = MockServerHttpRequest.get("/a")
                .header("X-Canary", "v2").build();
        assertThat(s.select(r, req).groupName()).isEqualTo("canary");
    }

    @Test
    void evictAfterDelete_thenRecreateAtVersion0_doesNotReuseLegacyPlan() {
        // 老路由：canary 权重 0，且认领标记 v2
        GatewayRoute legacy = route(0, List.of(
                g("stable", "stable", 100), g("canary", "canary", 0, "v2")));
        selector.select(legacy, request("v2"));
        assertThat(selector.cachedPlanCount()).isEqualTo(1);

        // 删除：显式摘除计划
        selector.evict("gray-r");
        assertThat(selector.cachedPlanCount()).isEqualTo(0);

        // 同编号重建，版本仍从 0 起算，但分组完全不同（不再有任何组认领 v2）
        GatewayRoute recreated = route(0, List.of(g("blue", "blue", 100)));
        GrayTarget target = selector.select(recreated, request("v2"));
        // 若错误复用了老计划，带 v2 标记会被分到一个已经不存在的 canary 组
        assertThat(target.groupName()).isEqualTo("blue");
        assertThat(target.byTag()).isFalse();
    }

    @Test
    void reconcileTo_keepsOnlyPlansOfRoutesStillActiveWithGrayGroups() {
        selector.select(route(0, List.of(g("stable", "a", 100))), request(null));

        GatewayRoute other = GatewayRoute.create("gray-other", "灰度", "http://b:8080", 1, null);
        other.setVersion(0);
        other.replaceGrayGroups(List.of(g("stable", "b", 100)));
        selector.select(other, request(null));
        assertThat(selector.cachedPlanCount()).isEqualTo(2);

        // 新快照：gray-r 已删除，gray-other 还在但关掉了灰度
        selector.reconcileTo(List.of(GatewayRoute.create("gray-other", "灰度", "http://b:8080", 1, null)));
        assertThat(selector.cachedPlanCount()).isZero();
    }
}
