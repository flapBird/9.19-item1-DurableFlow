package com.brixdata.durableflow.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 步骤条件表达式，用于条件分支。语法：
 * <pre>
 *   input.&lt;path&gt; &lt;op&gt; &lt;literal&gt;
 *   steps.&lt;stepId&gt;.&lt;path&gt; &lt;op&gt; &lt;literal&gt;
 * </pre>
 * 其中 op 为 ==、!=、&gt;、&gt;=、&lt;、&lt;=；literal 支持数字、true/false、null、
 * 单/双引号字符串或裸字符串。示例：{@code input.vip == true}、{@code steps.check.stock > 0}。
 */
public final class Condition {

    /** 比较运算符。 */
    public enum Op {
        EQ, NE, GT, GE, LT, LE
    }

    private final String source;
    private final List<String> path;
    private final Op op;
    private final Object literal;

    private Condition(String source, List<String> path, Op op, Object literal) {
        this.source = source;
        this.path = path;
        this.op = op;
        this.literal = literal;
    }

    /**
     * 解析条件表达式。
     *
     * @throws IllegalArgumentException 表达式非法时抛出
     */
    public static Condition parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("条件表达式不能为空");
        }
        String[] tokens = expression.trim().split("\\s+");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("条件表达式格式应为 '<path> <op> <literal>': " + expression);
        }
        String[] segments = tokens[0].split("\\.");
        if (segments.length < 2
                || !(segments[0].equals("input") || segments[0].equals("steps"))) {
            throw new IllegalArgumentException("条件路径必须以 input. 或 steps. 开头: " + tokens[0]);
        }
        Op op = switch (tokens[1]) {
            case "==" -> Op.EQ;
            case "!=" -> Op.NE;
            case ">" -> Op.GT;
            case ">=" -> Op.GE;
            case "<" -> Op.LT;
            case "<=" -> Op.LE;
            default -> throw new IllegalArgumentException("不支持的运算符: " + tokens[1]);
        };
        return new Condition(segments[0], List.of(segments).subList(1, segments.length),
                op, parseLiteral(tokens[2]));
    }

    private static Object parseLiteral(String token) {
        if (token.equals("true") || token.equals("false")) {
            return Boolean.valueOf(token);
        }
        if (token.equals("null")) {
            return null;
        }
        if ((token.startsWith("'") && token.endsWith("'") && token.length() >= 2)
                || (token.startsWith("\"") && token.endsWith("\"") && token.length() >= 2)) {
            return token.substring(1, token.length() - 1);
        }
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException ignored) {
            // 继续尝试浮点
        }
        try {
            return Double.parseDouble(token);
        } catch (NumberFormatException ignored) {
            return token;
        }
    }

    /**
     * 对工作流输入与已完成步骤的结果求值。
     *
     * @param input       工作流输入
     * @param stepResults 步骤 ID 到其结果的映射
     */
    public boolean evaluate(Map<String, Object> input, Map<String, Object> stepResults) {
        Object root = source.equals("input") ? input : stepResults;
        Object actual = resolvePath(root, path);
        return compare(actual, literal, op);
    }

    @SuppressWarnings("unchecked")
    private static Object resolvePath(Object root, List<String> path) {
        Object current = root;
        for (String segment : path) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<String, Object>) current).get(segment);
        }
        return current;
    }

    private static boolean compare(Object actual, Object expected, Op op) {
        if (actual instanceof Number a && expected instanceof Number b) {
            double diff = a.doubleValue() - b.doubleValue();
            return switch (op) {
                case EQ -> diff == 0;
                case NE -> diff != 0;
                case GT -> diff > 0;
                case GE -> diff >= 0;
                case LT -> diff < 0;
                case LE -> diff <= 0;
            };
        }
        int eq = Objects.equals(actual, expected) ? 0 : 1;
        if (actual instanceof Comparable<?> c && expected != null
                && actual.getClass().isInstance(expected)) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            int cmp = ((Comparable) c).compareTo(expected);
            return switch (op) {
                case EQ -> cmp == 0;
                case NE -> cmp != 0;
                case GT -> cmp > 0;
                case GE -> cmp >= 0;
                case LT -> cmp < 0;
                case LE -> cmp <= 0;
            };
        }
        return switch (op) {
            case EQ -> eq == 0;
            case NE -> eq != 0;
            default -> false;
        };
    }
}
