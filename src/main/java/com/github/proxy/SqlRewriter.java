package com.github.proxy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlRewriter {
    // 匹配双引号内的内容
    private static final Pattern QUOTE_PATTERN = Pattern.compile("\"([^\"]+)\"");

    public static String rewrite(String sql) {
        if (sql == null) return null;
        
        Matcher matcher = QUOTE_PATTERN.matcher(sql);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            // 提取内容 -> 转小写 -> 加上反引号
            String replacement = "`" + matcher.group(1).toLowerCase() + "`";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
