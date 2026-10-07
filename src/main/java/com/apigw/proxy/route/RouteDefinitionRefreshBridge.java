package com.apigw.proxy.route;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 把路由增删改事件桥接成 Spring Cloud Gateway 的 {@link RefreshRoutesEvent}。
 *
 * <p>转发流量由 {@code GatewayProxyWebFilter} 基于不可变快照短路处理，并不依赖 SCG 自己的
 * CachingRouteLocator；但 {@code RedisRouteDefinitionRepository} 仍会把路由翻译成 SCG 定义，
 * 那份缓存若不刷新，删除/同编号重建后会长期挂着旧定义（路由 id、谓词、过滤器都是上一任的）。
 * 配置提交成功后让 SCG 重新拉一次定义，两套视图都与权威存储保持一致。
 *
 * <p>多台网关之间的生效一致性不靠它：协调模式下各实例对不可变快照做两阶段切换，
 * 这里只刷新本机的 SCG 定义缓存，失败也不影响转发链路。
 */
@Slf4j
@Component
public class RouteDefinitionRefreshBridge {

    private final ApplicationEventPublisher publisher;

    public RouteDefinitionRefreshBridge(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @EventListener(RoutesChangedEvent.class)
    public void onRoutesChanged(RoutesChangedEvent event) {
        try {
            publisher.publishEvent(new RefreshRoutesEvent(this));
        } catch (Exception e) {
            log.debug("刷新 SCG 路由定义缓存失败（不影响转发快照）routeNo={} reason={}：{}",
                    event.routeNo(), event.reason(), e.toString());
        }
    }
}
