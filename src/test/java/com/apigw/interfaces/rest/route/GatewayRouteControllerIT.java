package com.apigw.interfaces.rest.route;

import com.apigw.infrastructure.store.RouteStore;
import com.apigw.support.EnabledIfRedis;
import com.apigw.support.RedisAvailableCondition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 管理接口端到端测试：起完整 Spring 服务，经真实 HTTP + Redis 走一遍。
 *
 * 覆盖题目点：
 * - 建/查/改/删的完整链路与统一返回结构；
 * - 顺序号撞号、跳号、类型不支持、值为空时，错误信息带「哪组第几条」；
 * - 编号重复（含停用占号）、改编号、上游地址非法、缺 version / 版本旧了；
 * - 列表分页四元组（pageNum/pageSize/total/totalPages）与子项计数，页数对上；
 * - 模糊查、pageSize 封顶；
 * - 删不存在返回 code=404，不静默成功。
 */
@EnabledIfRedis
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        // Redis 地址不钉死：沿用 application-dev.yml 的 ${REDIS_HOST:localhost} 环境变量口径，
        // 与 RedisAvailableCondition 探测的是同一台，避免「条件说能连、应用连的是另一台」
        "logging.level.com.apigw=warn"
})
class GatewayRouteControllerIT {

    @Autowired
    private WebTestClient web;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @BeforeEach
    void clean() {
        redis.delete(RouteStore.ROUTES_KEY).block();
        redis.delete("apigw:route:rules").block();
        redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match("apigw:lock:route:*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then())
                .block();
    }

    private Map<String, Object> rule(String type, String name, String value, int sortNo) {
        return Map.of("type", type,
                "name", name == null ? "" : name,
                "value", value == null ? "" : value,
                "sortNo", sortNo);
    }

    private Map<String, Object> routeBody(String routeNo, String name, String upstream,
                                          Object enabled, Object version,
                                          List<Map<String, Object>> conditions,
                                          List<Map<String, Object>> actions) {
        java.util.Map<String, Object> m = new java.util.HashMap<>();
        m.put("routeNo", routeNo);
        m.put("name", name);
        m.put("upstream", upstream);
        if (enabled != null) {
            m.put("enabled", enabled);
        }
        if (version != null) {
            m.put("version", version);
        }
        m.put("conditions", conditions == null ? List.of() : conditions);
        m.put("actions", actions == null ? List.of() : actions);
        return m;
    }

    private WebTestClient.BodyContentSpec createRoute(Map<String, Object> body) {
        return web.post().uri("/api/gateway/routes")
                .bodyValue(body)
                .exchange()
                .expectBody();
    }

    @Test
    void fullCrudFlow() {
        // 1. 新建
        Map<String, Object> body = routeBody("order-route", "订单路由", "http://order-svc:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/order/", 1),
                        rule("METHOD", null, "GET", 2)),
                List.of(rule("REQ_ADD_HEADER", "X-Gw", "1", 1)));
        web.post().uri("/api/gateway/routes").bodyValue(body).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.routeNo").isEqualTo("order-route")
                .jsonPath("$.data.version").isEqualTo(0)
                .jsonPath("$.data.conditions[0].sortNo").isEqualTo(1);

        // 2. 查详情
        web.get().uri("/api/gateway/routes/order-route").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.upstream").isEqualTo("http://order-svc:8080")
                .jsonPath("$.data.conditions.length()").isEqualTo(2)
                .jsonPath("$.data.actions.length()").isEqualTo(1);

        // 3. 改：名称 + 整批动作重排，带 version=0
        Map<String, Object> upd = routeBody("order-route", "订单路由v2", "http://order-svc:8080", 1, 0,
                List.of(rule("PATH_PREFIX", null, "/order/", 1)),
                List.of(rule("RESP_ADD_HEADER", "X-Trace", "t", 1)));
        web.put().uri("/api/gateway/routes/order-route").bodyValue(upd).exchange().expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.name").isEqualTo("订单路由v2")
                .jsonPath("$.data.version").isEqualTo(1)
                .jsonPath("$.data.conditions.length()").isEqualTo(1)
                .jsonPath("$.data.actions.length()").isEqualTo(1);

        // 4. 旧版本再改，必须被拒
        web.put().uri("/api/gateway/routes/order-route").bodyValue(upd).exchange().expectBody()
                .jsonPath("$.code").isEqualTo(409)
                .jsonPath("$.msg").value(v ->
                        org.assertj.core.api.Assertions.assertThat(v.toString()).contains("你这份配置已经旧了"));

        // 5. 删除（带最新版本），再查是 404
        web.delete().uri("/api/gateway/routes/order-route?expectVersion=1").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(0);
        web.get().uri("/api/gateway/routes/order-route").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(404);
    }

    @Test
    void create_duplicateRouteNoIncludingDisabled_isRejected() {
        createRoute(routeBody("dup-01", "n", "http://h:8080", 0, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of())).jsonPath("$.code").isEqualTo(0);

        // 停用也占号
        createRoute(routeBody("dup-01", "n2", "http://h:8081", 1, null,
                List.of(rule("PATH_PREFIX", null, "/b/", 1)), List.of()))
                .jsonPath("$.code").isEqualTo(1)
                .jsonPath("$.msg").value(v ->
                        org.assertj.core.api.Assertions.assertThat(v.toString()).contains("路由编号已被占用"));
    }

    @Test
    void update_routeNoInBodyCannotDifferFromPath() {
        createRoute(routeBody("imm-01", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of())).jsonPath("$.code").isEqualTo(0);

        Map<String, Object> evil = routeBody("imm-02", "n", "http://h:8080", 1, 0,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of());
        web.put().uri("/api/gateway/routes/imm-01").bodyValue(evil).exchange().expectBody()
                .jsonPath("$.code").isEqualTo(1)
                .jsonPath("$.msg").value(v ->
                        org.assertj.core.api.Assertions.assertThat(v.toString()).contains("路由编号建后不可修改"));
    }

    @Test
    void duplicateSortNo_reportsWhichTwo() {
        Map<String, Object> body = routeBody("sort-01", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1),
                        rule("METHOD", null, "GET", 1)),
                List.of());
        createRoute(body)
                .jsonPath("$.code").isEqualTo(1)
                .jsonPath("$.msg").isEqualTo("匹配条件第 1 条与第 2 条的顺序号撞了，都是 1，同组内顺序号不能重");
    }

    @Test
    void gapInSortNo_reportsMissingNumber() {
        Map<String, Object> body = routeBody("sort-02", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1),
                        rule("METHOD", null, "GET", 3)),
                List.of());
        createRoute(body)
                .jsonPath("$.msg").isEqualTo("匹配条件的顺序号必须从 1 起连续不跳号，缺了 2（现在有 2 条）");
    }

    @Test
    void action_sortNoError_isLabeledAsAction() {
        Map<String, Object> body = routeBody("sort-03", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)),
                List.of(rule("REQ_REMOVE_HEADER", "X-A", null, 1),
                        rule("REQ_REMOVE_HEADER", "X-B", null, 1)));
        createRoute(body)
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .startsWith("转发动作第 1 条与第 2 条"));
    }

    @Test
    void unknownType_andEmptyValue_areRejectedWithItemPosition() {
        createRoute(routeBody("t-01", "n", "http://h:8080", 1, null,
                List.of(rule("COOKIE", "k", "v", 1)), List.of()))
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("匹配条件第 1 条的类型不支持：COOKIE"));

        createRoute(routeBody("t-02", "n", "http://h:8080", 1, null,
                List.of(rule("HEADER", "X-K", "   ", 1)), List.of()))
                .jsonPath("$.msg").isEqualTo("匹配条件第 1 条缺取值");

        createRoute(routeBody("t-03", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)),
                List.of(rule("REWRITE_BODY", "X-K", "v", 1))))
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("转发动作第 1 条的类型不支持：REWRITE_BODY"));
    }

    @Test
    void badUpstream_isRejected() {
        createRoute(routeBody("up-01", "n", "  ", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of()))
                .jsonPath("$.msg").isEqualTo("上游地址不能为空");

        createRoute(routeBody("up-02", "n", "order-svc:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of()))
                .jsonPath("$.msg").isEqualTo("上游地址必须以 http:// 或 https:// 开头");

        createRoute(routeBody("up-03", "n", "http://###", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of()))
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .containsAnyOf("不是合法的 URL", "主机:端口不合法"));
    }

    @Test
    void update_withoutVersion_isRejected() {
        createRoute(routeBody("ver-01", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of())).jsonPath("$.code").isEqualTo(0);

        web.put().uri("/api/gateway/routes/ver-01")
                .bodyValue(routeBody("ver-01", "n2", "http://h:8080", 1, null,
                        List.of(rule("PATH_PREFIX", null, "/a/", 1)), List.of()))
                .exchange().expectBody()
                .jsonPath("$.code").isEqualTo(1)
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("必须带上读取时拿到的版本号"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void paging_numbersMatch_andRowsCarryCounts_andKeywordWorks() {
        for (int i = 1; i <= 5; i++) {
            String no = "p-%02d".formatted(i);
            List<Map<String, Object>> conds = new java.util.ArrayList<>();
            conds.add(rule("PATH_PREFIX", null, "/p" + i + "/", 1));
            if (i % 2 == 0) {
                conds.add(rule("METHOD", null, "GET", 2));
            }
            createRoute(routeBody(no, "分页路由" + i, "http://h:8080", 1, null,
                    conds, List.of(rule("REQ_REMOVE_HEADER", "X-A", null, 1))))
                    .jsonPath("$.code").isEqualTo(0);
        }
        // 再加一个名字/编号都不沾关键字的
        createRoute(routeBody("other-99", "别的", "http://h:8081", 1, null,
                List.of(rule("PATH_PREFIX", null, "/o/", 1)), List.of())).jsonPath("$.code").isEqualTo(0);

        // 全量 6 条，每页 5：第一页 5 条，总页 2
        web.get().uri("/api/gateway/routes?pageNum=1&pageSize=5").exchange().expectBody()
                .jsonPath("$.data.pageNum").isEqualTo(1)
                .jsonPath("$.data.pageSize").isEqualTo(5)
                .jsonPath("$.data.total").isEqualTo(6)
                .jsonPath("$.data.totalPages").isEqualTo(2)
                .jsonPath("$.data.content.length()").isEqualTo(5);

        // 第二页 1 条
        web.get().uri("/api/gateway/routes?pageNum=2&pageSize=5").exchange().expectBody()
                .jsonPath("$.data.content.length()").isEqualTo(1)
                .jsonPath("$.data.total").isEqualTo(6);

        // 模糊找：编号或名称含 "p-0"（命中 p-01..p-05？只有 p-0x 五位 + 名称不含 p-0）
        web.get().uri("/api/gateway/routes?pageNum=1&pageSize=20&keyword=p-0").exchange().expectBody()
                .jsonPath("$.data.total").isEqualTo(5)
                .jsonPath("$.data.totalPages").isEqualTo(1);

        // 按中文名称模糊找
        web.get().uri("/api/gateway/routes?keyword=分页").exchange().expectBody()
                .jsonPath("$.data.total").isEqualTo(5);

        // pageSize 超过上限被压到 200，且计数带在每行上。
        // 行序按编号字典序：other-99 在最前（1 条件、0 动作），p-01 紧随其后（1 条件、1 动作）
        web.get().uri("/api/gateway/routes?pageSize=99999").exchange().expectBody()
                .jsonPath("$.data.pageSize").isEqualTo(200)
                .jsonPath("$.data.total").isEqualTo(6)
                .jsonPath("$.data.content[0].routeNo").isEqualTo("other-99")
                .jsonPath("$.data.content[0].conditionCount").isEqualTo(1)
                .jsonPath("$.data.content[0].actionCount").isEqualTo(0)
                .jsonPath("$.data.content[1].routeNo").isEqualTo("p-01")
                .jsonPath("$.data.content[1].conditionCount").isEqualTo(1)
                .jsonPath("$.data.content[1].actionCount").isEqualTo(1);
    }

    @Test
    void delete_missing_returns404() {
        web.delete().uri("/api/gateway/routes/never-existed?expectVersion=0").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(404)
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("路由不存在，删除未执行"));
    }

    @Test
    void delete_withoutVersion_isRejected_andRouteKept() {
        createRoute(routeBody("del-nover", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)),
                List.of())).jsonPath("$.code").isEqualTo(0);

        // 不带版本不许删：显式业务失败，而不是当成功
        web.delete().uri("/api/gateway/routes/del-nover").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(1)
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("必须带上路由当前版本号"));

        Boolean exists = redis.opsForHash().hasKey(RouteStore.ROUTES_KEY, "del-nover").block();
        assertEquals(true, exists, "被拒绝的删除不能真的删掉路由");
    }

    @Test
    void delete_staleVersion_isRejected() {
        createRoute(routeBody("del-stale", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1)),
                List.of())).jsonPath("$.code").isEqualTo(0);
        Map<String, Object> upd = routeBody("del-stale", "n2", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/b/", 1)),
                List.of());
        upd.put("version", 0);
        web.put().uri("/api/gateway/routes/del-stale").bodyValue(upd).exchange()
                .expectBody().jsonPath("$.code").isEqualTo(0);

        // 版本已到 1，还拿 0 来删：409，路由保留
        web.delete().uri("/api/gateway/routes/del-stale?expectVersion=0").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(409)
                .jsonPath("$.msg").value(v -> org.assertj.core.api.Assertions.assertThat(v.toString())
                        .contains("你这份配置已经旧了"));
        Boolean exists = redis.opsForHash().hasKey(RouteStore.ROUTES_KEY, "del-stale").block();
        assertEquals(true, exists);
    }

    @Test
    void delete_cascadesWholeTree_andRuleIndex() {
        createRoute(routeBody("del-01", "n", "http://h:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/a/", 1), rule("METHOD", null, "GET", 2)),
                List.of(rule("REQ_ADD_HEADER", "X-K", "v", 1)))).jsonPath("$.code").isEqualTo(0);

        // 规则索引在创建后应有一份
        Object indexed = redis.opsForHash().get("apigw:route:rules", "del-01").block();
        assertEquals(true, indexed != null);

        web.delete().uri("/api/gateway/routes/del-01?expectVersion=0").exchange()
                .expectBody().jsonPath("$.code").isEqualTo(0);

        // Redis 里整条 field 消失，条件/动作没有独立 key；规则索引 field 也一并收掉，无孤儿
        Boolean exists = redis.opsForHash().hasKey(RouteStore.ROUTES_KEY, "del-01").block();
        assertEquals(false, exists);
        Boolean indexExists = redis.opsForHash().hasKey("apigw:route:rules", "del-01").block();
        assertEquals(false, indexExists, "删除后规则索引不能留下无主 field");
    }

    @Test
    void delete_thenRecreateSameRouteNo_newRouteIsNotContaminated() {
        // 老路由：2 条件 + 1 动作，指 order 上游
        createRoute(routeBody("re-01", "老路由", "http://order-svc:8080", 1, null,
                List.of(rule("PATH_PREFIX", null, "/order/", 1), rule("METHOD", null, "GET", 2)),
                List.of(rule("REQ_ADD_HEADER", "X-Legacy", "1", 1)))).jsonPath("$.code").isEqualTo(0);
        web.delete().uri("/api/gateway/routes/re-01?expectVersion=0").exchange()
                .expectBody().jsonPath("$.code").isEqualTo(0);

        // 同编号重建：1 条件 + 1 动作，指 pay 上游
        createRoute(routeBody("re-01", "新路由", "http://pay-svc:9090", 1, null,
                List.of(rule("PATH_PREFIX", null, "/pay/", 1)),
                List.of(rule("REQ_REMOVE_HEADER", "X-Old", null, 1)))).jsonPath("$.code").isEqualTo(0);

        // 详情必须恰好是新建那一批：老的条件/动作一个都不许混进来
        web.get().uri("/api/gateway/routes/re-01").exchange().expectBody()
                .jsonPath("$.code").isEqualTo(0)
                .jsonPath("$.data.name").isEqualTo("新路由")
                .jsonPath("$.data.upstream").isEqualTo("http://pay-svc:9090")
                .jsonPath("$.data.version").isEqualTo(0)
                .jsonPath("$.data.conditions.length()").isEqualTo(1)
                .jsonPath("$.data.conditions[0].value").isEqualTo("/pay/")
                .jsonPath("$.data.actions.length()").isEqualTo(1)
                .jsonPath("$.data.actions[0].type").isEqualTo("REQ_REMOVE_HEADER");

        // 规则索引同样只有新那一批：老条件/老动作不许残留在索引里被装配捡到
        String indexJson = String.valueOf(
                redis.opsForHash().get("apigw:route:rules", "re-01").block());
        org.assertj.core.api.Assertions.assertThat(indexJson)
                .contains("/pay/").doesNotContain("/order/").doesNotContain("X-Legacy");
    }
}
