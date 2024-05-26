package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.structure.Atom;
import uk.ac.ncl.utils.IO;
import com.google.common.collect.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Rule;
import uk.ac.ncl.structure.Template;

import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Context {
    public static BiMap<Integer, Rule> indexRule = HashBiMap.create();
    public Map<Rule, Integer> ruleFrequency = new HashMap<>();

    private List<Rule> refinedRules = new ArrayList<>();
    private List<Rule> specializedRules = new ArrayList<>();
    private List<Rule> appliedRules = new ArrayList<>();
    private ConcurrentHashMap<Pair, List<Rule>> predictionMap;

    private static int index = 0;
    private int totalInsRules = 0;
    private int essentialRules = 0;

    Multimap<Pair, Rule> getPredictionMultiMap() {
        Multimap<Pair, Rule> candidateMap = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Map.Entry<Pair, List<Rule>> entry : predictionMap.entrySet()) {
            candidateMap.putAll(entry.getKey(), entry.getValue());
        }
        return candidateMap;
    }

    public void initPredictionMap() {
        predictionMap = new ConcurrentHashMap<>((int) (Settings.SUGGESTION_CAP * 0.3), 0.8f, Settings.THREAD_NUMBER);
    }

    public boolean checkInsRuleCap() {
        return totalInsRules > Settings.INS_RULE_CAP;
    }

    public boolean checkSuggestionCap() {
        return predictionMap.size() > Settings.SUGGESTION_CAP;
    }

    public synchronized void putInPredictionMap(Pair pair, Rule rule) {
        /*
         * 如果这个答案有多条规则，直接存放到map中
         */
        if(!predictionMap.containsKey(pair)) {
            predictionMap.put(pair, new ArrayList<>());
            predictionMap.get(pair).add(rule);
        } else {
            List<Rule> rules = predictionMap.get(pair);
            /*
             * 限制一个答案，对应的支撑规则数量
             * public static int PREDICTION_RULE_CAP = 20;
             */
            if(rules.size() >= Settings.PREDICTION_RULE_CAP) {
                /*
                 * 假设这条规则的分数比当前存储最后一名的规则分数要高，则要remove这条规则
                 * 并添加这条规则进map当中
                 * 并且重新排序
                 */
                if(rules.get(rules.size() - 1).getQuality() < rule.getQuality()) {
                    rules.remove(rules.size() - 1);
                    rules.add(rule);
                    rules.sort(IO.ruleComparatorBySC());
                }
            } else {
                /*
                 * 如果有多于2条规则，则需要用smoothedConf对这些规则降序排序
                 */
                rules.add(rule);
                rules.sort(IO.ruleComparatorBySC());
            }
        }
    }

    public synchronized void addSpecializedRules(Rule rule) {
        specializedRules.add(rule);
    }

    public synchronized List<Rule> getSpecializedRules() {
        return specializedRules;
    }

    public int predictionMapSize() {
        return predictionMap.keySet().size();
    }

    public synchronized int getTotalInsRules() {
        return totalInsRules;
    }

    public synchronized void updateTotalInsRules() {
        totalInsRules += 1;
    }

    public synchronized void removeRefinedRule(Rule rule) {
        refinedRules.remove(rule);
    }

    public synchronized void addAppliedRule(Rule rule) {
        appliedRules.add(rule);
    }

    public synchronized List<Rule> getAppliedRules() {
        return appliedRules;
    }

    public List<Rule> getRefinedRules() {
        return refinedRules;
    }

    public Rule abstraction(Path path, Pair pair) {
        List<Atom> bodyAtoms = buildBodyAtoms(path);
        Atom head = new Atom(pair);
        Rule rule = new Template(head, bodyAtoms);
        updateFreqAndIndex(rule);
        return rule;
    }

    public static Rule createTemplate(Path path, Pair pair) {
        List<Atom> bodyAtoms = buildBodyAtoms(path);
        Atom head = new Atom(pair);
        return new Template(head, bodyAtoms);
    }

    public synchronized void updateFreqAndIndex(Rule rule) {
        /*
         * ruleFrequency，是用于方便按频率排序，然后生成HAR或者BAR,形式如下
         * rule，frequency
         * rule1，frequency1
         * rule2，frequency2
         * rule3，frequency3
         * indexRule是按照先后顺序生成的，包含了所有的closed以及open rules,形式如下
         * index，rule
         * 1，rule1
         * 2，rule2
         * 3，rule3
         */
        if(ruleFrequency.containsKey(rule))
            ruleFrequency.put(rule, ruleFrequency.get(rule) + 1);
        else {
            ruleFrequency.put(rule, 1);
            indexRule.put(index++, rule);
        }
    }

    public Integer getIndex(Rule rule) {
        Integer r = indexRule.inverse().get(rule);
        if(r == null) {
            System.err.println("# Query for non-existent rule.");
            System.exit(-1);
        } else
            return r;
        return -1;
    }

    public Rule getRule(Integer index) {
        return indexRule.get(index);
    }

    public List<Rule> sortTemplates() {
        Multimap<Integer, Rule> openRuleLengthMap = MultimapBuilder.treeKeys().hashSetValues().build();
        List<Rule> closedRules = new ArrayList<>();
        /*
         * 从ruleFrequency中获前面还没被生成HAR或者BAR的规则
         * 分开统计，先统计closedRules，再统计openRule
         * 这里的len是指bodyatom的数量
         * closedRules包含len=1,2,3的规则
         * openRule按长度进行统计，包含len=2,3的规则，len=1的在前面generateEssentialRules当中已经生成了规则
         * 其中openRule在len=2的时候降序排序，在len等3的时候也降序排序，最终，refinedRules的格式如下
         * closedRules len=1,2,3的规则，按frequency降序排序
         * openRule len=2，按frequency降序排序
         * openRule len=3，按frequency降序排序
         */
        for (Rule rule : getAbstractRules()) {
            if(rule.isClosed())
                closedRules.add(rule);
            else
                openRuleLengthMap.put(rule.length(), rule);
        }
        /*
         * 对closedRules降序排序
         */
        closedRules.sort((o1, o2) -> ruleFrequency.get(o2) - ruleFrequency.get(o1));
        refinedRules.addAll(closedRules);
        for (Integer length : openRuleLengthMap.keySet()) {
            List<Rule> openRules = new ArrayList<>(openRuleLengthMap.get(length));
            openRules.sort((o1, o2) -> ruleFrequency.get(o2) - ruleFrequency.get(o1));
            refinedRules.addAll(openRules);
        }
        return refinedRules;
    }

    private static List<Atom> buildBodyAtoms(Path path) {
        List<Atom> bodyAtoms = Lists.newArrayList();
        List<Relationship> relationships = Lists.newArrayList( path.relationships() );
        List<Node> nodes = Lists.newArrayList( path.nodes() );
        for( int i = 0; i < relationships.size(); i++ ) {
            bodyAtoms.add( new Atom( nodes.get( i ), relationships.get( i ) ) );
        }
        return bodyAtoms;
    }

    public Set<Rule> getAbstractRules() {
        return ruleFrequency.keySet();
    }

    public static String analyzeRuleComposition(String header, Collection<Rule> rules) {
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        int closedRules = 0;
        int openRules = 0;
        Map<Integer, Integer> lengthMap = new TreeMap<>();

        for (Rule rule : rules) {
            if(rule.isClosed())
                closedRules++;
            else {
                openRules++;
                if (lengthMap.containsKey(rule.length())) {
                    lengthMap.put(rule.length(), lengthMap.get(rule.length()) + 1);
                } else {
                    lengthMap.put(rule.length(), 1);
                }
            }
        }

        String content = MessageFormat.format("{0}: {1} | ClosedRules: {2} | OpenRules: {3} | "
                , header, rules.size(), closedRules, openRules);
        List<String> words = new ArrayList<>();
        for (Integer length : lengthMap.keySet()) {
            words.add("len=" + length + ": " + f.format(lengthMap.get(length)));
        }
        content += String.join(" | ", words);
        return content;
    }

    public synchronized void updateEssentialRules() {
        essentialRules++;
    }

    public synchronized int getEssentialRules() {
        return essentialRules;
    }

    public NavigableSet<Rule> topRules = Collections.synchronizedNavigableSet(
            new TreeSet<>(IO.treeSetRuleQualityComparator(Settings.QUALITY_MEASURE)));

    public synchronized void addTopRules(Rule rule) {
        String metric = Settings.QUALITY_MEASURE;
        if(topRules.size() < Settings.TOP_RULES)
            topRules.add(rule);
        else {
            if(topRules.last().getQuality(metric) < rule.getQuality(metric)) {
                topRules.pollLast();
                topRules.add(rule);
            }
        }
    }
}
