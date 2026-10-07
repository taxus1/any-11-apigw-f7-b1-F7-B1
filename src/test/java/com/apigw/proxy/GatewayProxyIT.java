package com.apigw.proxy;

import com.apigw.infrastructure.store.RouteStore;
import com.apigw.support.EnabledIfRedis;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 完整链路集成测试（真实 Spring 容器 + 真实 Redis + 真实 JDK 上游）：
 *
 * 1. 通过管理接口新建一条路由（落 Redis）；
 * 2. 不重启，立刻打转发路径——验证配置变更事件即时生效；
 * 3. 请求补头动作真正到上游、响应补头真正回到调用方；
 * 4. 没配的路径回 404 NO_ROUTE；
 * 5. 删掉路由后，原路径立刻变成 404。
 *
 * 本机探不到 Redis 时自动跳过（与既有 IT 一致）。
 */
@EnabledIfRedis
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        // Redis 地址不钉死：沿用 application-dev.yml 的 ${REDIS_HOST:localhost} 环境变量口径，
        // 与 RedisAvailableCondition 探测的是同一台
        "logging.level.com.apigw=warn",
        "apigw.proxy.response-timeout=5s"
})
class GatewayProxyIT {

    @Autowired
    private WebTestClient web;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private HttpServer upstream;
    private int upstreamPort;

    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        redis.delete(RouteStore.ROUTES_KEY).block();
        redis.scan(org.springframework.data.redis.core.ScanOptions
                        .scanOptions().match("apigw:lock:route:*").build())
                .collectList()
                .flatMap(keys -> keys.isEmpty() ? reactor.core.publisher.Mono.empty()
                        : redis.delete(reactor.core.publisher.Flux.fromIterable(keys)).then())
                .block();

        upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        upstream.createContext("/", exchange -> {
            String gw = exchange.getRequestHeaders().getFirst("X-Gw");
            String leaked = exchange.getRequestHeaders().getFirst("X-Internal");
            byte[] body = ("{\"gw\":\"" + gw + "\",\"leaked\":\"" + leaked + "\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        upstream.start();
        upstreamPort = upstream.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (upstream != null) {
            upstream.stop(0);
        }
    }

    private Map<String, Object> rule(String type, String name, String value, int sortNo) {
        var m = new java.util.HashMap<String, Object>();
        m.put("type", type);
        if (name != null) {
            m.put("name", name);
        }
        if (value != null) {
            m.put("value", value);
        }
        m.put("sortNo", sortNo);
        return m;
    }

    @Test
    void createRouteViaApi_thenItIsReachableImmediately_withHeaderActions() throws Exception {
        String upstreamUrl = "http://127.0.0.1:" + upstreamPort;
        Map<String, Object> body = Map.of(
                "routeNo", "it-order",
                "name", "集成测试路由",
                "upstream", upstreamUrl,
                "enabled", 1,
                "conditions", List.of(rule("PATH_PREFIX", null, "/order/", 1)),
                "actions", List.of(
                        rule("REQ_ADD_HEADER", "X-Gw", "gw-value", 1),
                        rule("REQ_REMOVE_HEADER", "X-Internal", null, 2),
                        rule("RESP_ADD_HEADER", "X-Trace", "resp-trace", 3)));

        // 1. 经管理接口建路由
        web.post().uri("/api/gateway/routes")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.code").isEqualTo(0);

        // 2. 不重启，立刻转发：请求补/删头生效，响应补头生效
        byte[] respBytes = web.get().uri("/order/it?x=1")
                .header("X-Internal", "should-be-removed")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Trace", "resp-trace")
                .expectHeader().exists("X-Gateway-Trace-Id")
                .expectBody().returnResult().getResponseBody();

        JsonNode node = json.readTree(respBytes);
        org.assertj.core.api.Assertions.assertThat(node.get("gw").asText()).isEqualTo("gw-value");
        org.assertj.core.api.Assertions.assertThat(node.get("leaked").isNull()
                || node.get("leaked").asText().equals("null")).isTrue();

        // 3. 没配的路径：404 且明确是网关没找到路
        web.get().uri("/not-configured").exchange()
                .expectStatus().isNotFound()
                .expectHeader().valueEquals("X-Gateway-Error", "NO_ROUTE")
                .expectBody().jsonPath("$.error").isEqualTo("NO_ROUTE");

        // 4. 删除路由后立刻失效（同样不重启）
        web.delete().uri("/api/gateway/routes/it-order?expectVersion=0").exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.code").isEqualTo(0);
        web.get().uri("/order/it").exchange()
                .expectStatus().isNotFound()
                .expectHeader().valueEquals("X-Gateway-Error", "NO_ROUTE");
    }
}
