package com.apigw.infrastructure.store;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RuleTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则索引：把每条路由的匹配条件与转发动作单独存一份，匹配装配时以它为准，
 * 省掉每次刷新都要把整条路由 JSON 解一遍的开销。
 *
 * <p>写入：路由创建/修改提交成功之后；读取：{@code RouteCatalog} 装配可用路由时。
 */
@Component
public class RouteRuleIndex {

    public static final String RULES_KEY = "apigw:route:rules";

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RouteRuleIndex(ReactiveStringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    /** 覆盖写一条路由的规则。 */
    public Mono<Void> write(String routeNo, Rules rules) {
        return redis.opsForHash().put(RULES_KEY, routeNo, serialize(rules)).then();
    }

    /** 读一条路由的规则；没有则为空。 */
    public Mono<Rules> read(String routeNo) {
        return redis.opsForHash().get(RULES_KEY, routeNo)
                .map(v -> deserialize(String.valueOf(v)));
    }

    /** 一次读全部规则（装配时一次 HGETALL）。 */
    public Mono<Map<String, Rules>> readAll() {
        return redis.opsForHash().entries(RULES_KEY)
                .collectMap(e -> String.valueOf(e.getKey()),
                        e -> deserialize(String.valueOf(e.getValue())),
                        LinkedHashMap::new);
    }

    /** 清掉一条路由的规则（删除路由时应一并调用）。 */
    public Mono<Void> remove(String routeNo) {
        return redis.opsForHash().remove(RULES_KEY, routeNo).then();
    }

    private String serialize(Rules rules) {
        try {
            return objectMapper.writeValueAsString(rules);
        } catch (Exception e) {
            throw new BizException("规则索引序列化失败：" + e.getMessage());
        }
    }

    private Rules deserialize(String json) {
        try {
            Rules r = objectMapper.readValue(json, Rules.class);
            return r == null ? new Rules() : r;
        } catch (Exception e) {
            throw new BizException("规则索引反序列化失败：" + e.getMessage());
        }
    }

    /** 索引里存的形状：匹配条件 + 转发动作。 */
    public static class Rules {
        public List<RuleItem> conditions = new ArrayList<>();
        public List<RuleItem> actions = new ArrayList<>();

        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean isEmpty() {
            return conditions.isEmpty() && actions.isEmpty();
        }

        /** 从一条路由抽出规则。 */
        public static Rules of(GatewayRoute route) {
            Rules r = new Rules();
            for (GatewayRule c : route.getConditions()) {
                r.conditions.add(RuleItem.from(c));
            }
            for (GatewayRule a : route.getActions()) {
                r.actions.add(RuleItem.from(a));
            }
            return r;
        }

        /** 把另一份规则接到这一份后面。 */
        public Rules plus(Rules other) {
            Rules r = new Rules();
            r.conditions.addAll(this.conditions);
            r.actions.addAll(this.actions);
            if (other != null) {
                r.conditions.addAll(other.conditions);
                r.actions.addAll(other.actions);
            }
            return r;
        }

        public List<GatewayRule> conditionRules() {
            List<GatewayRule> out = new ArrayList<>();
            for (RuleItem i : conditions) {
                GatewayRule g = i.toDomain();
                g.setRuleKind(RuleTypes.KIND_CONDITION);
                out.add(g);
            }
            return out;
        }

        public List<GatewayRule> actionRules() {
            List<GatewayRule> out = new ArrayList<>();
            for (RuleItem i : actions) {
                GatewayRule g = i.toDomain();
                g.setRuleKind(RuleTypes.KIND_ACTION);
                out.add(g);
            }
            return out;
        }
    }

    /** 单条规则的落盘形状（与路由 JSON 里的子项字段一致）。 */
    public static class RuleItem {
        public String id;
        public String stage;
        public String type;
        public String name;
        public String value;
        public Integer sortNo;

        static RuleItem from(GatewayRule g) {
            RuleItem d = new RuleItem();
            d.id = g.getId();
            d.stage = g.getStage();
            d.type = g.getType();
            d.name = g.getName();
            d.value = g.getValue();
            d.sortNo = g.getSortNo();
            return d;
        }

        GatewayRule toDomain() {
            return GatewayRule.create(stage, type, name, value, sortNo);
        }
    }
}
