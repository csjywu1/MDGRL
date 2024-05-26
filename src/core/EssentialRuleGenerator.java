package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.structure.Rule;
import uk.ac.ncl.structure.Template;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.Logger;
import uk.ac.ncl.utils.SemaphoredThreadPool;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.structure.InstantiatedRule;
import uk.ac.ncl.structure.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.*;

public class EssentialRuleGenerator {

    public static void generateEssentialRules(Set<Pair> trainPairs, Set<Pair> validPairs
            , Context context, GraphDatabaseService graph
            , File tempFile, File ruleFile) throws IOException {
        long s = System.currentTimeMillis();
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        GlobalTimer.setEssentialStartTime(System.currentTimeMillis());

        Set<Rule> essentialRules = new HashSet<>();
        BlockingQueue<String> tempFileContents = new LinkedBlockingDeque<>(10000000);
        BlockingQueue<String> ruleFileContents = new LinkedBlockingDeque<>(10000000);
        /*
         * essentialRules，处理length==1，并且是非closed的规则
         */
        for (Rule rule : context.getAbstractRules()) {
            if(!rule.isClosed() && rule.length() == 1)
                essentialRules.add(rule);
        }

        Multimap<Long, Long> trainObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> trainSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();

        for (Pair trainPair : trainPairs) {
            trainObjToSub.put(trainPair.objId, trainPair.subId);
            trainSubToObj.put(trainPair.subId, trainPair.objId);
        }

        for (Pair validPair : validPairs) {
            validObjToSub.put(validPair.objId, validPair.subId);
            validSubToObj.put(validPair.subId, validPair.objId);
        }

        /*
         * 定义规则的读写类
         * tempFiltercontents只写入ABS:instantiated开头的，后接HAR或者BAR，对应的是各种类型的文件夹
         * ruleFilecontens只写入HAR或者BAR，对应只有一个rules.txt文件夹
         * tempFilewriter相当于一个监听器，writer写入tempFilewriter监听到的结果，writer是最后整体写入
         * ruleFileWriter相当于一个监听器，writer1写入ruleFileWriter监听到的结果，而writer1是每接收一个就直接写入
         */
        RuleWriter tempFileWriter = new RuleWriter(0, tempFile, tempFileContents, true);
        PrintWriter writer = new PrintWriter(new FileWriter(tempFile, true));
        RuleWriter ruleFileWriter = new RuleWriter(0, ruleFile, ruleFileContents, true);
        PrintWriter writer1 = new PrintWriter(new FileWriter(ruleFile, true));
        Set<Rule> specializedRules = new HashSet<>();

        try {
            for (Rule rule : essentialRules) {
                if(GlobalTimer.stopEssential()) break;
                BlockingQueue<String> contents = new LinkedBlockingDeque<>();
                context.ruleFrequency.remove(rule);

                Set<Pair> groundings = generateBodyGrounding(rule, graph);
                Multimap<Long, Long> originalToTails = MultimapBuilder.hashKeys().hashSetValues().build();
                Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();

                for (Pair grounding : groundings) {
                    originalToTails.put(grounding.subId, grounding.objId);
                    tailToOriginals.put(grounding.objId, grounding.subId);
                }

                Multimap<Long, Long> trainAnchoringToOriginals = rule.isFromSubject() ? trainObjToSub : trainSubToObj;
                Multimap<Long, Long> validAnchoringToOriginals = rule.isFromSubject() ? validObjToSub : validSubToObj;
                for (Long anchoring : trainAnchoringToOriginals.keySet()) {
                    if(GlobalTimer.stopEssential()) break;
                    Collection<Long> validOriginals = validAnchoringToOriginals.get(anchoring);
                    /*
                     * 修改Head中的一个值为Headanchoring！！！！！！！！！限定Head中一个值的情况下
                     * 如果是fromSource，就修改Head中的Y
                     * 如果不是fromSource，就修改Head中的X
                     * 举例
                     * fromsource的时候
                     * Head：X->Y  bodyatom：X<-V0 修改为Head：X->HeadAnchoring bodyatom：X<-V0
                     *查找的过程Head:HeadAnchoring<-Original (从trainPair中获取),Bodyatom:groundingOriginal<-V0(从grounding中获取，与bodyatom同向)
                     * 二者满足，就是计算Original与groundingOriginal中重复的情况
                     * 非fromsource的时候
                     * Head：X->Y  bodyatom：Y<-V0 修改为Head：HeadAnchoring->Y  bodyatom：Y<-V0
                     *查找的过程Head:HeadAnchoring->Original (从trainPair中获取),Bodyatom:groundingOriginal<-V0(从grounding中获取，与bodyatom同向)
                     * 二者满足，就是计算Original与groundingOriginal中重复的情况
                     */
                    new CreateHAR(rule, anchoring, trainAnchoringToOriginals.get(anchoring)
                             , validOriginals, originalToTails.keySet()
                             , graph, contents, ruleFileContents, context);
                    while( !ruleFileWriter.contents.isEmpty()) {
                        String line = ruleFileWriter.contents.poll();
                        if(line != null) {
                            writer1.println(line);
                            writer1.flush();
                        }
                    }
                    /*
                     * 修改Head中的，一个值为Headanchoring,一个值为tail！！！！！！！！！限定Head中一个值，以及限定lastatom中一个值的情况下
                     * 如果是fromSource，就修改Head中的Y，并且要修改atom中最后一个值
                     * 如果不是fromSource，就修改Head中的X，并且要修改atom中最后一个值
                     * 举例
                     * fromsource的时候
                     * Head：X->Y  bodyatom：X<-V0 修改为Head：X->HeadAnchoring bodyatom：X<-tail
                     *查找的过程Head:HeadAnchoring<-Original (从trainPair中获取),Bodyatom:groundingOriginal<-tail(从grounding中获取，与bodyatom反向)
                     * 非fromsource的时候
                     * Head：X->Y  bodyatom：Y<-V0 修改为Head：HeadAnchoring->Y  bodyatom：Y<-tail
                     *查找的过程Head:HeadAnchoring->Original (从trainPair中获取),Bodyatom:groundingOriginalOriginal->tail(从grounding中获取，与bodyatom反向)
                     */
                    for (Long original : trainAnchoringToOriginals.get(anchoring)) {
                        if(GlobalTimer.stopEssential()) break;
                        for (Long tail : originalToTails.get(original)) {
                            if(GlobalTimer.stopEssential()) break;
                            Pair candidate = new Pair(anchoring, tail);
                            /*
                             *trivialCheck，不对length=1且headanchoring=tail的规则生成BAR
                             * 举例
                             * head：128->129
                             * bodyatom：129->128
                             * head：128->129
                             * bodyatom：129<-128
                             */
                            if (!trivialCheck(rule, anchoring, tail)) {
                                new CreateBAR(rule, candidate, trainAnchoringToOriginals.get(anchoring)
                                        , validOriginals, tailToOriginals.get(tail)
                                        , graph, contents, ruleFileContents, context);
                                while( !ruleFileWriter.contents.isEmpty()) {
                                    String line = ruleFileWriter.contents.poll();
                                    if(line != null) {
                                        writer1.println(line);
                                        writer1.flush();
                                    }
                                }
                            }
                        }
                    }
                }


                if(!contents.isEmpty()) {
                    specializedRules.add(rule);
                    /*
                     * 对instantiatedRule的累加值
                     * 计算validPrecision，apcaConf，headCoverage，pcaConfstandardConf，standardConf，smoothedConf
                     */
                    rule.stats.compute();
                    /*
                     * 写入ABS开头的规则，写入各类型的文件当中
                     * 后接各种HAR或者BAR的规则，String.join("\t", contents)，contents存储了对应的HAR或者BAR
                     * 获取在indexRule中的位置，indexRule是按照先后顺序生成的，包含了所有的closed以及open rules
                     * context.getIndex(rule)：获取序号
                     * ((Template) rule).toRuleIndexString()：获取Head<-bodyatoms
                     */
                    tempFileContents.put("ABS: " + context.getIndex(rule) + "\t"
                            + ((Template) rule).toRuleIndexString() + "\t"
                            + f.format(rule.getStandardConf()) + "\t"
                            + f.format(rule.getQuality()) + "\t"
                            + f.format(rule.getPcaConf()) + "\t"
                            + f.format(rule.getApcaConf()) + "\t"
                            + f.format(rule.getHeadCoverage()) + "\t"
                            + f.format(rule.getValidPrecision()) + "\n"
                            + String.join("\t", contents) + "\n");
                    while( !tempFileWriter.contents.isEmpty()) {
                        String line = tempFileWriter.contents.poll();
                        if(line != null) {
                            writer.println(line);
                            writer.flush();
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateGenEssentialStats(Helpers.timerAndMemory(s, "# Generate Essentials"));
        Logger.println("# Specialized Essential Templates: " + f.format(specializedRules.size()) + " | " +
                "Generated Essential Rules: " + f.format(context.getEssentialRules()), 1);
    }

    private static class RuleWriter extends Thread {
        int id;
        ExecutorService service;
        File file;
        BlockingQueue<String> contents;
        boolean append;

        RuleWriter(int id, File file, BlockingQueue<String> contents, boolean append) {
            super("EssentialRule-RuleWriter-" + id);
            this.id = id;
            this.file = file;
            this.contents = contents;
            this.append = append;
    }
    }

    private static class CreateBAR implements Runnable {
        Rule base;
        Pair candidate;
        Collection<Long> originals;
        Collection<Long> groundingOriginals;
        GraphDatabaseService graph;
        BlockingQueue<String> Contents;
        BlockingQueue<String> ruleFileContents;
        Context context;
        Collection<Long> validOriginals;

        CreateBAR(Rule base, Pair candidate
                , Collection<Long> originals
                , Collection<Long> validAnchoringToOriginals
                , Collection<Long> groundingOriginals
                , GraphDatabaseService graph
                , BlockingQueue<String> Contents
                , BlockingQueue<String> ruleFileContents
                , Context context) {
            this.base = base;
            this.candidate = candidate;
            this.originals = originals;
            this.groundingOriginals = groundingOriginals;
            this.graph = graph;
            this.Contents = Contents;
            this.ruleFileContents = ruleFileContents;
            this.context = context;
            this.validOriginals = validAnchoringToOriginals;
        }

        @Override
        public void run() {
            DecimalFormat f = new DecimalFormat("####.#####");
            try(Transaction tx = graph.beginTx()) {
                int totalPredictions = 0, support = 0, groundTruth = originals.size()
                        , validTotalPredictions = 0, validPredictions = 0;

                candidate.subName = (String) graph.getNodeById(candidate.subId).getProperty(Settings.NEO4J_IDENTIFIER);
                candidate.objName = (String) graph.getNodeById(candidate.objId).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule rule = new InstantiatedRule(base, candidate);
                /*
                 * ！！！！！！！！！！！！！
                 * 在限定情况下进行统计，限定Head中一个值，以及限定lastatom中一个值的情况下计算
                 * 统计限定了headanchoring与tail值以后，在train与valid当中有多少数据满足这条规则
                 * totalPredictions （standard），满足bodyatom格式grounding的数量，grounding的数据来源于图（由train生成的图）
                 * support在代入HeadAnchoring值进trainPair以后，代入tail值进lastatom中，训练集中符合规则，既满足head也满足bodyatom的的数量
                 * ！！！！！！！！！！！！！
                 * 训练集中不满足的放到验证集中完成预测
                 * validTotalPredictions=训练集中，限定headanchoring值和tail值以后，不满足规则的数量，
                 * validPredictions在代入HeadAnchoring值进validPair以后，代入tail值进lastatom中，验证集中符合规则，既满足head也满足bodyatom的的数量
                 * ！！！！！！！！！！！！！！！！！！！！
                 * pcaTotalPredictions （pca）参考AMIE+的理解，使用重合的X值的数量
                 * 举例
                 * fromsource的时候
                 * Head：X->Y  bodyatom：X<-V0 修改为Head：X->HeadAnchoring bodyatom：X<-tail
                 *查找的过程Head:HeadAnchoring<-Original (从trainPair中获取),Bodyatom:GroudingOriginal<-tail(从grounding中获取)
                 * 在限定HeadAnchoring与tail的情况下，计算满足两者，满足Head，满足bodyatom的情况
                 * 非fromsource的时候
                 * Head：X->Y  bodyatom：Y<-V0 修改为Head：HeadAnchoring->Y  bodyatom：Y<-tail
                 *查找的过程Head:HeadAnchoring->Original (从trainPair中获取),Bodyatom:GroudingOriginal->tail(从grounding中获取)
                 * 在限定HeadAnchoring与tail的情况下，计算满足两者，满足Head，满足bodyatom的情况
                 */
                for (Long groundingOriginal : groundingOriginals) {
                    totalPredictions++;
                    if(originals.contains(groundingOriginal))
                        support++;
                    else {
                        validTotalPredictions++;
                        if(validOriginals.contains(groundingOriginal))
                            validPredictions++;
                    }
                }
                /*
                 * 与AMIE+的定义有出入，使用重合的X值的数量
                 * Head：X->Y  bodyatom：X<-V0 修改为Head：X->HeadAnchoring bodyatom：X<-tail，也就是support值的数量
                 * Head：X->Y  bodyatom：Y<-V0 修改为Head：HeadAnchoring->Y  bodyatom：Y<-tail， 由于没有X值，因此直接使用totalPredictions
                 */
                int pcaTotalPredictions = base.isFromSubject() ? support : totalPredictions;
                rule.setStats(support, totalPredictions, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
                /*
                 * 检查三个阈值是否满足最低标准
                 * 1.SUPPORT,二者都满足,既满足head,也满足bodyatoms的
                 * 2.CONFIDENCE,二者都满足/只满足bodyatoms的
                 * 3.HEAD_COVERAGE，二者都满足/只满足head的
                 */
                if(Template.qualityCheck(rule)) {
                    try {
                        /*
                         * 统计EssentialRules，也就是path=1的instantiated生成HAR或者BAR的数量
                         */
                        context.updateEssentialRules();
                        /*
                         *将HAR的值，累加到instantiatedrule的参数中
                         * support，totalPredictions，pcaTotalPredictions，groundTruth，validTotalPredictions，validPredictions
                         */
                        Contents.put("2" + ","
                                + rule.getHeadAnchoring() + ","
                                + rule.getTailAnchoring() + ","
                                + f.format(rule.getStandardConf()) + ","
                                + f.format(rule.getQuality()) + ","
                                + f.format(rule.getPcaConf()) + ","
                                + f.format(rule.getApcaConf()) + ","
                                + f.format(rule.getHeadCoverage()) + ","
                                + f.format(rule.getValidPrecision()));
                        /*
                         * 将BAR,写入rules.txt当中
                         * 统计Quality是可选的pcaConf，standardConf，smoothedConf,这里选择的是smoothedConf
                         */
                        ruleFileContents.put(rule.toString() + "\t"
                                + f.format(rule.getQuality()) + "\t"
                                + f.format(rule.getHeadCoverage()) + "\t"
                                + f.format(rule.getValidPrecision()) + "\t"
                                + (int) rule.stats.support + "\t"
                                + (int) rule.stats.totalPredictions);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
                tx.success();
            }
        }
    }

    private static class CreateHAR implements Runnable {
        Rule base;
        long anchoring;
        Collection<Long> originals;
        Collection<Long> groundingOriginals;
        GraphDatabaseService graph;
        BlockingQueue<String> Contents;
        BlockingQueue<String> ruleFileContents;
        Context context;
        Collection<Long> validOriginals;

                                            CreateHAR(Rule base, long anchoring
                , Collection<Long> originals
                , Collection<Long> validOriginals
                , Collection<Long> groundingOriginals
                , GraphDatabaseService graph
                , BlockingQueue<String> Contents
                , BlockingQueue<String> ruleFileContents
                , Context context) {
            this.base = base;
            this.anchoring = anchoring;
            this.originals = originals;
            this.validOriginals = validOriginals;
            this.groundingOriginals = groundingOriginals;
            this.graph = graph;
            this.Contents = Contents;
            this.ruleFileContents = ruleFileContents;
            this.context = context;
            run();
        }

        @Override
        public void run() {
            DecimalFormat f = new DecimalFormat("####.#####");
            try(Transaction tx = graph.beginTx()) {
                int totalPredictions = 0, support = 0, groundTruth = originals.size()
                        , validTotalPredictions = 0, validPredictions = 0;
                String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                /*
                 * base值用于记录规则原来的值，没被替代前的值
                 */
                Rule rule = new InstantiatedRule(base, headName, anchoring);
                /*
                 * ！！！！！！！！！！！！！
                 * 在限定情况下进行统计，限定Head中一个值的情况下计算
                 * 统计限定了headanchoring值以后，在train与valid当中有多少数据满足这条规则
                 * totalPredictions （standard），满足bodyatom格式grounding的数量，grounding的数据来源于图（由train生成的图）
                 * support在代入HeadAnchoring值进trainPair以后，训练集中符合规则，既满足head也满足bodyatom的的数量
                 * ！！！！！！！！！！！！！
                 * 训练集中不满足的放到验证集中完成预测
                 * validTotalPredictions=训练集中，限定headanchoring值以后，不满足规则的数量，
                 * validPredictions在代入HeadAnchoring值进validPair以后，验证集中符合规则，既满足head也满足bodyatom的的数量
                 * ！！！！！！！！！！！！！！！！！！！！
                 * pcaTotalPredictions （pca）参考AMIE+的理解，使用重合的X值的数量
                 * 举例
                 * fromsource的时候
                 * Head：X->Y  bodyatom：X<-V0 修改为Head：X->HeadAnchoring bodyatom：X<-V0
                 *查找的过程Head:HeadAnchoring<-Original (从trainPair中获取),Bodyatom:groundingOriginal<-V0(从grounding中获取，与bodyatom同向)
                 * 二者满足，就是计算Original与groundingOriginal中重复的情况
                 * 非fromsource的时候
                 * Head：X->Y  bodyatom：Y<-V0 修改为Head：HeadAnchoring->Y  bodyatom：Y<-V0
                 *查找的过程Head:HeadAnchoring->Original (从trainPair中获取),Bodyatom:groundingOriginal<-V0(从grounding中获取，与bodyatom同向)
                 * 二者满足，就是计算Original与groundingOriginal中重复的情况
                 */
                for (Long groundingOriginal : groundingOriginals) {
                    totalPredictions++;
                    if(originals.contains(groundingOriginal)) {
                        support++;
                    } else {
                        validTotalPredictions++;
                        if(validOriginals.contains(groundingOriginal))
                            validPredictions++;
                    }
                }
                /*
                 * 与AMIE+的定义有出入，使用重合的X值的数量
                 * Head：X->Y  bodyatom：X<-V0 fromsource，修改为Head：X->HeadAnchoring bodyatom：X<-V0，也就是support值
                 * Head：X->Y  bodyatom：Y<-V0 非fromsource，修改为Head：HeadAnchoring->Y bodyatom：Y<-V0， 由于没有X值，因此直接使用totalPredictions
                 */
                int pcaTotalPredictions = base.isFromSubject() ? support : totalPredictions;
                /*
                 * groundTruth只满足head的数量
                 */
                rule.setStats(support, totalPredictions, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);
                /*
                 * 检查三个阈值是否满足最低标准
                 * 1.SUPPORT,二者都满足,既满足head,也满足bodyatoms的
                 * 2.CONFIDENCE,二者都满足/只满足bodyatoms的
                 * 3.HEAD_COVERAGE，二者都满足/只满足head的
                 */
                if(Template.qualityCheck(rule)) {
                    try {
                        /*
                         * 统计EssentialRules，也就是path=1的instantiated生成HAR或者BAR的数量
                         */
                        context.updateEssentialRules();
                        /*
                         *将HAR的值，累加到instantiatedrule的参数中
                         * support，totalPredictions，pcaTotalPredictions，groundTruth，validTotalPredictions，validPredictions
                         */
                        updateBaseStats(rule);
                        Contents.put("0" + ","
                                + rule.getHeadAnchoring() + ","
                                + f.format(rule.getStandardConf()) + ","
                                + f.format(rule.getQuality()) + ","
                                + f.format(rule.getPcaConf()) + ","
                                + f.format(rule.getApcaConf()) + ","
                                + f.format(rule.getHeadCoverage()) + ","
                                + f.format(rule.getValidPrecision()));
                        /*
                         * 将HAR或者BAR,写入rules.txt当中
                         * 统计Quality是可选的pcaConf，standardConf，smoothedConf,这里选择的是smoothedConf
                         */
                        ruleFileContents.put(rule.toString() + "\t"
                                + f.format(rule.getQuality()) + "\t"
                                + f.format(rule.getHeadCoverage()) + "\t"
                                + f.format(rule.getValidPrecision()) + "\t"
                                + (int) rule.stats.support + "\t"
                                + (int) rule.stats.totalPredictions);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
                tx.success();
            }
        }

        private synchronized void updateBaseStats(Rule rule) {
            base.stats.support += rule.stats.support;
            base.stats.totalPredictions += rule.stats.totalPredictions;
            base.stats.pcaTotalPredictions += rule.stats.pcaTotalPredictions;
            base.stats.groundTruth += rule.stats.groundTruth;
            base.stats.validTotalPredictions += rule.stats.validTotalPredictions;
            base.stats.validPredictions += rule.stats.validPredictions;
        }
    }

    private static boolean trivialCheck(Rule rule, long head, long tail) {
        if(Settings.ALLOW_INS_REVERSE)
            return false;
        return head == tail && rule.length() == 1 && rule.head.predicate.equals(rule.bodyAtoms.get(0).predicate);
    }

    private static Set<Pair> generateBodyGrounding(Rule rule, GraphDatabaseService graph) {
        Set<Pair> groundings = new HashSet<>();
        String predicate = rule.bodyAtoms.get(0).predicate;
        boolean outgoing = rule.bodyAtoms.get(0).direction.equals(Direction.OUTGOING);
        try(Transaction tx = graph.beginTx()) {
            for (Relationship relationship : graph.getAllRelationships()) {
//                if(GlobalTimer.stopEssential() || groundings.size() > Settings.LEARN_GROUNDINGS) break;
                if(relationship.getType().name().equals(predicate)) {
                    groundings.add(outgoing ? new Pair(relationship.getStartNodeId(), relationship.getEndNodeId())
                            : new Pair(relationship.getEndNodeId(), relationship.getStartNodeId()));
                }
            }
            tx.success();
        }
        return groundings;
    }


}
