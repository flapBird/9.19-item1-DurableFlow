package com.brixdata.durableflow.engine;

import java.util.Map;

/**
 * 条件表达式求值器，用于条件分支。
 *
 * <p>支持的语法（空白不敏感）：</p>
 * <ul>
 *   <li>{@code input.<key>} — 输入值真值判断（非 null、非 "false"、非空串）</li>
 *   <li>{@code input.<key> == <literal>} / {@code !=} — 字符串相等比较，
 *       literal 可用单/双引号包裹</li>
 *   <li>{@code steps.<stepId>.result == <literal>} — 与已完成步骤的结果比较</li>
 *   <li>{@code !<expr>} — 取反</li>
 *   <li>{@code true} / {@code false} — 字面量</li>
 * </ul>
 */
public final class ConditionEvaluator {

    private ConditionEvaluator() {
    }

    public static boolean evaluate(String expression, Map<String, Object> input,
                                   Map<String, String> stepResults) {
        if (expression == null || expression.isBlank()) {
            return true;
        }
        String expr = expression.trim();
        if (expr.startsWith("!")) {
            return !evaluate(expr.substring(1), input, stepResults);
        }
        if (expr.equalsIgnoreCase("true")) {
            return true;
        }
        if (expr.equalsIgnoreCase("false")) {
            return false;
        }
        int eq = findOperator(expr, "==");
        if (eq >= 0) {
            return resolve(expr.substring(0, eq), input, stepResults)
                    .equals(unquote(expr.substring(eq + 2)));
        }
        int ne = findOperator(expr, "!=");
        if (ne >= 0) {
            return !resolve(expr.substring(0, ne), input, stepResults)
                    .equals(unquote(expr.substring(ne + 2)));
        }
        String value = resolve(expr, input, stepResults);
        return value != null && !value.isEmpty() && !value.equalsIgnoreCase("false");
    }

    private static int findOperator(String expr, String op) {
        return expr.indexOf(op);
    }

    private static String resolve(String pathExpr, Map<String, Object> input,
                                  Map<String, String> stepResults) {
        String path = pathExpr.trim();
        if (path.startsWith("input.")) {
            Object v = input.get(path.substring("input.".length()));
            return v == null ? null : String.valueOf(v);
        }
        if (path.startsWith("steps.") && path.endsWith(".result")) {
            String stepId = path.substring("steps.".length(), path.length() - ".result".length());
            return stepResults.get(stepId);
        }
        throw new IllegalArgumentException("unsupported condition path: " + path);
    }

    private static String unquote(String literalExpr) {
        String literal = literalExpr.trim();
        if (literal.length() >= 2
                && ((literal.startsWith("\"") && literal.endsWith("\""))
                || (literal.startsWith("'") && literal.endsWith("'")))) {
            return literal.substring(1, literal.length() - 1);
        }
        return literal;
    }
}
