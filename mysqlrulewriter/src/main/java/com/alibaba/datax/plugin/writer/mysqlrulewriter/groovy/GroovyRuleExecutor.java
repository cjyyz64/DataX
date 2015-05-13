package com.alibaba.datax.plugin.writer.mysqlrulewriter.groovy;

import com.taobao.tddl.rule.impl.WrappedGroovyRule;
import com.taobao.tddl.rule.utils.AdvancedParameterParser;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Date: 15/5/8 下午3:15
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class GroovyRuleExecutor extends WrappedGroovyRule {

    private String pattern = "";

    public GroovyRuleExecutor(String expression, String pattern) {
        super(expression, pattern, false);
        this.pattern = pattern;
    }

    public GroovyRuleExecutor(String expression, String pattern, boolean lazyInit) {
        super(expression, pattern, lazyInit);
    }

    @Override
    @SuppressWarnings("unchecked")
    public String parseParam(String paramInDoller, Map parameters) {
        RuleColumn ruleColumn = null;
        if (!paramInDoller.contains(",")) {
            // 如果没有其他参数，直接从parameters中取
            // 这里必需RuleColumn的对于Key的处理方式一致，否则将取不到，这是一个风险点
            ruleColumn = (RuleColumn) parameters.get(paramInDoller.trim());
        }

        if (ruleColumn == null) {
            ruleColumn = AdvancedParameterParser.getAdvancedParamByParamTokenNew(paramInDoller, false);
            parameters.put(ruleColumn.key, ruleColumn);
        }
        return replace(ruleColumn);
    }

    public String executeRule(Map<String, Object> recordMap) {
        if(StringUtils.isBlank(getExpression())) {
            return this.pattern;
        }
        return eval(recordMap, null);
    }

}
