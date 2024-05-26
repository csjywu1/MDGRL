package uk.ac.ncl.structure;

import uk.ac.ncl.Caculate;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Context;
import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.GlobalTimer;
import uk.ac.ncl.core.GraphOps;
import uk.ac.ncl.utils.IO;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Template extends Rule {
    public List<SimpleInsRule> insRules = new ArrayList<>();

    public Template() {}

    public Template(String line) {
        String[] words = line.split("\t");
        closed = words[1].equals("CAR");
        String headline = words[2].split(" <- ")[0];
        String[] bodyLines = words[2].split(" <- ")[1].split(", ");
        head = new Atom(headline, true);
        bodyAtoms = new ArrayList<>();
        for (String bodyLine : bodyLines) {
            bodyAtoms.add(new Atom(bodyLine, false));
        }
        Atom firstAtom = bodyAtoms.get(0);
        fromSubject = firstAtom.subject.equals("X");
    }

    public Template(Atom h, List<Atom> b) {
        super( h, b );
        Atom firstAtom = bodyAtoms.get( 0 );
        Atom lastAtom = bodyAtoms.get( bodyAtoms.size() - 1 );
        head.subject = "X";
        head.object = "Y";

        int variableCount = 0;
        for(Atom atom : bodyAtoms) {
            atom.subject = "V" + variableCount;
            atom.object = "V" + ++variableCount;
        }

        if ( fromSubject ) firstAtom.subject = "X";
        else firstAtom.subject = "Y";

        if ( closed && fromSubject ) lastAtom.object = "Y";
        else if ( closed ) lastAtom.object = "X";
    }

    @Override
    public long getTailAnchoring() {
        return -1;
    }

    @Override
    public long getHeadAnchoring() {
        return -1;
    }

    public void specialization(GraphDatabaseService graph, Set<Pair> trainPairs, Set<Pair> validPair
            , Multimap<Long, Long> trianAnchoringToOriginals, Multimap<Long, Long> validAnchoringToOriginals
            , Context context
            , BlockingQueue<String> ruleFileContents
            , BlockingQueue<String> tempFileContents, Engine.RuleWriter tempFileWriter, Engine.RuleWriter ruleFileWriter, PrintWriter writer, PrintWriter writer1) throws InterruptedException {
        DecimalFormat f = new DecimalFormat("####.#####");
        List<String> contents = new ArrayList<>();
        /*
         *深搜遍历生成length>1的bodygroudings
         */
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this
                );
        /*
         * tempFile,按类型分别写进文件当中，形式如下所示,CAR是closdRule，OAR是instantiatedRule
         * ABS: 26409	CAR	CanFollow(X,Y) <- CanPrecede(Y,X,1)	0.65854	0.62069	0.76056	0.62069	0.65854	0.53571
         * ABS: 24574	OAR	CanFollow(X,Y) <- CanFollow(V1,Y,1), targetof(V1,V2,0), PeerOf(V3,V2,1)	0.75	0.46154	0.75	0.46154	0.07317	0	0.51748
         * 0,38,0.75,0.46154,0.75,0.46154,1,0	2,38,1186,1,0.54545,1,0.54545,1,0	2,38,1142,1,0.54545,1,0.54545,1,0 （后接各种HAR或者BAR）
         * ruleFile （rule.txt）,将所有类型的规则写进文件当，写入Closed的规则 (len=1,len=2,len=3)，以及
         * 所有的HAR或者BAR的规则 (len=2,len=3)，len=1的HAR或者BAR在generateEssentialRules当中已经写入
         */
        if(closed) {
            /*
             * evalClosedRule，统计指标
             *              * standardConf:两者都满足/所有与bodyatom形式相关的值
             *              * pcaConf:fromSource的时候pcaConf=1，非fromSource的时候与standardConf的值相同
             *              * headCoverage:两者都满足/满足头的
             *              * 非常用的三个指标！！！！！！！！！！！！！！！！
             *              * smoothedConf：在standardConf的基础上加了一个偏移的变量
             *              * apcaConf：如果一对一比较多，就选择使用pcaConf，否则使用smoothedConf
             *              * validPrecision：
             *              * 训练集中不满足的放到验证集中完成预测
             *              * validTotalPredictions=！！！训练集中，限定headanchoring值以后，不满足规则的数量
             *              * validPredictions在代入HeadAnchoring值进validPair以后，验证集中符合规则，既满足head也满足bodyatom的的数量
             * closedRule的bodyGrounding
             * context.getIndex(this),从indexRule当中获取index
             * this.toRuleIndexString()，规则的内容
             * closedRule同时写入tempFile，以及ruleFile当中
             */
             if(evalClosedRule(bodyGroundings, trainPairs, validPair,graph)) {
                 /*
                  * 统计Specialized过的规则
                  */
                 context.addSpecializedRules(this);
                 tempFileContents.put("ABS: " + context.getIndex(this) + "\t"
                         + this.toRuleIndexString() + "\t"
                         + f.format(getStandardConf()) + "\t"
                         + f.format(getQuality()) + "\t"
                         + f.format(getPcaConf()) + "\t"
                         + f.format(getApcaConf()) + "\t"
                         + f.format(getHeadCoverage()) + "\t"
                         + f.format(getValidPrecision()) + "\n");
                 while( !tempFileWriter.contentQueue.isEmpty()) {
                     String line = tempFileWriter.contentQueue.poll();
                     if(line != null) {
                         writer.println(line);
                     }
                 }
                 ruleFileContents.put(this.toString() + "\t"
                         + f.format(getQuality()) + "\t"
                         + f.format(getHeadCoverage()) + "\t"
                         + f.format(getValidPrecision()) + "\t"
                         + (int) stats.support + "\t"
                         + (int) stats.totalPredictions);
                 while( !ruleFileWriter.contentQueue.isEmpty()) {
                     String line = ruleFileWriter.contentQueue.poll();
                     if(line != null) {
                         writer1.println(line);
                         writer1.flush();
                     }
                 }
             }
        }
        else {
            /*
             * ！！！！！！！！groundtruth在instantiatedRule,HAR,BAR设置的不同
             * 这里是设置instantiatedRule的groundTruth，在没限制HeadAnchoring或者tail的情况下，满足头的数量
             * 下面有HAR设置了HeadAnchoring的情况下，满足头的数量
             * 下面有BAR设置了HeadAnchoring与tail的情况下，满足头的数量!!!!!!!
             * 由于tail不影响Head，因此HAR满足头的数量与BAR满足头的数量相同
             */
            stats.groundTruth = trainPairs.size();
            Multimap<Long, Long> originalToTail = MultimapBuilder.hashKeys().hashSetValues().build();
            Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
            for (Pair bodyGrounding : bodyGroundings) {
                originalToTail.put(bodyGrounding.subId, bodyGrounding.objId);
                tailToOriginal.put(bodyGrounding.objId, bodyGrounding.subId);
            }

            /*
             * 第一层循环，限定Head再进行计算
             */
            for (Long anchoring : trianAnchoringToOriginals.keySet()) {


                String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule HAR = new InstantiatedRule(this, headName, anchoring);
                /*
                 * 在限定HeadAnchoring的条件下，统计各种指标
                 *  evaluateRule函数既可以统计HAR也可以统计BAR
                 */
                Set<Long> groundingOriginals = originalToTail.keySet();
                Collection<Long> validOriginals = validAnchoringToOriginals.get(anchoring);
                Collection<Long> originals= trianAnchoringToOriginals.get(anchoring);
                if(evaluateRule(HAR, originals ,validOriginals, groundingOriginals,graph)) {
                    /*
                     * 统计到instantiatedRule里面
                     */
                    stats.support += HAR.stats.support;
                    stats.totalPredictions += HAR.stats.totalPredictions;
                    stats.pcaTotalPredictions += HAR.stats.pcaTotalPredictions;
                    /*
                     * 统计HAR和BAR的总数量
                     */
                    context.updateTotalInsRules();
                    contents.add("0" + ","
                            + HAR.getHeadAnchoring() + ","
                            + f.format(HAR.getStandardConf()) + ","
                            + f.format(HAR.getQuality()) + ","
                            + f.format(HAR.getPcaConf()) + ","
                            + f.format(HAR.getApcaConf()) + ","
                            + f.format(HAR.getHeadCoverage()) + ","
                            + f.format(HAR.getValidPrecision()));
                    ruleFileContents.put(HAR.toString() + "\t"
                            + f.format(HAR.getQuality()) + "\t"
                            + f.format(HAR.getHeadCoverage()) + "\t"
                            + f.format(HAR.getValidPrecision()) + "\t"
                            + (int) HAR.stats.support + "\t"
                            + (int) HAR.stats.totalPredictions);
                    while( !ruleFileWriter.contentQueue.isEmpty()) {
                        String line = ruleFileWriter.contentQueue.poll();
                        if(line != null) {
                            writer1.println(line);
                            writer1.flush();
                        }
                    }
                }
                /*
                 * 第二层循环，限定Head与tail，再进行计算
                 */
                for (Long original : trianAnchoringToOriginals.get(anchoring)) {

                    for (Long tail : originalToTail.get(original)) {
                        Pair candidate = new Pair(anchoring, tail);
                        if(!trivialCheck(anchoring, tail)) {
                            candidate.subName = headName;
                            candidate.objName = (String) graph.getNodeById(tail).getProperty(Settings.NEO4J_IDENTIFIER);
                            Rule BAR = new InstantiatedRule(this, candidate);
                            /*
                             * 在限定HeadAnchoring与tail的条件下，统计各种指标
                             */
                            groundingOriginals= (Set<Long>) tailToOriginal.get(tail);
                            if (evaluateRule(BAR, originals, validOriginals,groundingOriginals,graph)) {
                                /*
                                 * 统计HAR和BAR的总数量
                                 */
                                context.updateTotalInsRules();
                                contents.add("2" + ","
                                        + BAR.getHeadAnchoring() + ","
                                        + BAR.getTailAnchoring() + ","
                                        + f.format(BAR.getStandardConf()) + ","
                                        + f.format(BAR.getQuality()) + ","
                                        + f.format(BAR.getPcaConf()) + ","
                                        + f.format(BAR.getApcaConf()) + ","
                                        + f.format(BAR.getHeadCoverage()) + ","
                                        + f.format(BAR.getValidPrecision()));
                                ruleFileContents.put(BAR.toString() + "\t"
                                        + f.format(BAR.getQuality()) + "\t"
                                        + f.format(BAR.getHeadCoverage()) + "\t"
                                        + f.format(BAR.getValidPrecision()) + "\t"
                                        + (int) BAR.stats.support + "\t"
                                        + (int) BAR.stats.totalPredictions);
                            }
                            while( !ruleFileWriter.contentQueue.isEmpty()) {
                                String line = ruleFileWriter.contentQueue.poll();
                                if(line != null) {
                                    writer1.println(line);
                                }
                            }
                        }
                    }
                }
            }
            /*
             * 对instantiatedRule进行分数的计算，前面已经把各种HAR或者BAR的值附加上去
             */
            stats.compute();
            if(!contents.isEmpty()) {
                /*
                 * 统计instantiatedRule是否可以生成HAR或者BAR，如果可以生成则存放到SpecializedRules当中
                 */
                context.addSpecializedRules(this);
                tempFileContents.put("ABS: " + context.getIndex(this) + "\t"
                        + this.toRuleIndexString() + "\t"
                        + f.format(getStandardConf()) + "\t"
                        + f.format(getSmoothedConf()) + "\t"
                        + f.format(getPcaConf()) + "\t"
                        + f.format(getApcaConf()) + "\t"
                        + f.format(getHeadCoverage()) + "\t"
                        + f.format(getValidPrecision()) + "\n"
                        + String.join("\t", contents) + "\n");
            }
            while( !tempFileWriter.contentQueue.isEmpty()) {
                String line = tempFileWriter.contentQueue.poll();
                if(line != null) {
                    writer.println(line);
                    writer.flush();
                }
            }
        }
    }

    public void applyRule(GraphDatabaseService graph, Context context) {
        /*
         * 找到这条rule的所有bodygrounding，要执行checktail
         * checktail的作用，限制grounding的值，grounding的lastatom的tail值相同
         * ！！！！！！！！！！！！！！！！！！！！！！！
         * 而在specialization与generalization生成grounding是只限制形式，而在application生成grounding限制了tail值
         * ！！！！！！！！！！！！！！！！
         * 用的是OAR本来的tail值来限制
         */
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this);
        Set<Long> originals = Sets.newHashSet();
        Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair grounding : bodyGroundings) {
            originals.add(grounding.subId);
            tailToOriginals.put(grounding.objId, grounding.subId);
        }
        /*
         * ！！！！！！！！！！！！！！
         * 会生成new triple
         * 用closedRule生成答案
         */
        if (closed) applyClosedRule(bodyGroundings, context);
        else {
            for (SimpleInsRule rule : insRules) {

                /*
                 * 用HAR生成答案
                 */
                if (rule.type == 0)
                    applyHeadAnchoredRules(rule, originals, context);
                /*
                 * 用BAR生成答案
                 */
                else if (rule.type == 2)
                    applyBothAnchoredRules(rule, tailToOriginals, context);
            }
            /*
             * 在RuleReader reader = new RuleReader中重新读新的规则
             */
            insRules.clear();
        }
    }

    private boolean evalClosedRule(CountedSet<Pair> bodyGroundings, Set<Pair> trainPairs, Set<Pair> validPair,GraphDatabaseService graph) {
        double totalPrediction = 0, support = 0, pcaTotalPrediction = 0
                , validTotalPredictions = 0, validPredictions = 0;
        /*
         * 统计所有TrainPairs中的X值
         */
        Set<Long> subs = new HashSet<>();
        for (Pair pair : trainPairs)
            subs.add(pair.subId);
        /*
         * totalPrediction，满足bodyatom的数量
         * correctPrediction，两者都满足的数量
         *
         **/
        /*
         * ！！！！！！！！！！！！！
         * 在无限定情况下
         * 统计在train与valid当中有多少数据满足这条规则
         * totalPredictions （standard），满足bodyatom格式grounding的数量，grounding的数据来源于图（由train生成的图）
         * support既满足head也满足bodyatom的的数量
         * ！！！！！！！！！！！！！
         * 训练集中不满足的放到验证集中完成预测
         * validTotalPredictions=训练集中，不满足规则的数量，
         * validPredictions验证集中既满足head也满足bodyatom的的数量
         * ！！！！！！！！！！！！！！！！！！！！
         * pcaTotalPredictions （pca）参考AMIE+的理解，使用重合的X值的数量
         * 举例
         * fromsource的时候
         * Head：X->Y  bodyatom：X->V0->Y
         * 找的过程Head:X->Y (从trainPair中获取),Bodyatom:X->V0->Y(从grounding中获取)
         * 计算满足两者，满足Head，满足bodyatom的情况
         * 非fromsource的时候
         * Head：X->Y  bodyatom：Y<-X
         * 查找的过程Head:X->Y (从trainPair中获取),Bodyatom:X->Y(从grounding中获取)
         * 计算满足两者，满足Head，满足bodyatom的情况
         */
        for (Pair grounding : bodyGroundings) {
            long sub = fromSubject ? grounding.subId : grounding.objId;
            /*
             * 统计Head与BodyAtom中，有多少X值是重合的
             * ！！！！！！！！！！！！！！！！！！！！
             * 在这里与AMIE+的定义相同
             */
            if(subs.contains(sub))
                pcaTotalPrediction++;

            Pair prediction = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);

            if(trainPairs.contains(prediction))
                support++;
            else {
                validTotalPredictions++;
                if(validPair.contains(prediction))
                    validPredictions++;
            }

            totalPrediction++;
        }
        /*
         * 满足head的数量
         */
        int groundtruth = trainPairs.size();
        setStats(support, totalPrediction, pcaTotalPrediction
                , groundtruth, validTotalPredictions, validPredictions);

        if(!qualityCheck(this)){
            return false;
        }
        /*
         * 检查三个阈值是否满足最低标准
         * 1.SUPPORT,二者都满足,既满足head,也满足bodyatoms的
         * 2.CONFIDENCE,二者都满足/只满足bodyatoms的
         * 3.HEAD_COVERAGE，二者都满足/只满足head的
         */

        double sum=0;
        Caculate cal=new Caculate();
        for (Pair grounding : bodyGroundings) {
            long sub = fromSubject ? grounding.subId : grounding.objId;
            /*
             * 统计Head与BodyAtom中，有多少X值是重合的
             * ！！！！！！！！！！！！！！！！！！！！
             * 在这里与AMIE+的定义相同
             */
            if(subs.contains(sub))
                pcaTotalPrediction++;

            Pair prediction = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);

            //如果有相同的，则计算这个trainPairs的分数
            if(trainPairs.contains(prediction)) {
                support++;
                double[] entity1=new double[100];
                double[] entity2=new double[100];
                double[] relation=new double[100];
                int c = Engine.relation2id.get(Engine.r);
                relation=Arrays.copyOf(Engine.relation_vec[c],Engine.relation_vec[c].length);
                if(fromSubject){
                    entity1=Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.subId).getProperty("name"))],Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.subId).getProperty("name"))].length);
                    entity2=Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.objId).getProperty("name"))],Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.objId).getProperty("name"))].length);
                    sum=sum+cal.calc_sum(entity1,entity2,relation);
                }
                else{
                    entity2=Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.subId).getProperty("name"))],Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.subId).getProperty("name"))].length);
                    entity1=Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.objId).getProperty("name"))],Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(grounding.objId).getProperty("name"))].length);
                    sum=sum+cal.calc_sum(entity1,entity2,relation);
                }
            }
            else {
                validTotalPredictions++;
                if(validPair.contains(prediction))
                    validPredictions++;
            }

            totalPrediction++;
        }
        if(support==0){
            return false;
        }
        double score=sum/support;
        score= 1.0 / (1.0 + Math.exp(-score));
        this.stats.Jaccard=score;
        return true;
    }

    private boolean evaluateRule(Rule rule, Collection<Long> originals, Collection<Long> validOriginals, Collection<Long> groundingOriginals,GraphDatabaseService graph) {
        int totalPrediction = 0, support = 0, groundTruth = originals.size()
                , validTotalPredictions = 0, validPredictions = 0;
        for (Long groundingOriginal : groundingOriginals) {
            totalPrediction++;
            if(originals.contains(groundingOriginal))
                support++;
            else {
                validTotalPredictions++;
                if(validOriginals.contains(groundingOriginal))
                    validPredictions++;
            }
        }
        int pcaTotalPredictions = isFromSubject() ? support : totalPrediction;
        rule.setStats(support, totalPrediction, pcaTotalPredictions, groundTruth, validTotalPredictions, validPredictions);

        if(!qualityCheck(rule)){
            return false;
        }

        double sum=0;
        Caculate cal=new Caculate();
        for (Long groundingOriginal : groundingOriginals) {
            totalPrediction++;
            if(originals.contains(groundingOriginal)) {
                support++;
                double[] entity1 = new double[100];
                double[] entity2 = new double[100];
                double[] relation = new double[100];
                int c = Engine.relation2id.get(Engine.r);
                relation = Arrays.copyOf(Engine.relation_vec[c], Engine.relation_vec[c].length);
                if (fromSubject) {
                    entity1 = Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(groundingOriginal).getProperty("name"))], Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(groundingOriginal).getProperty("name"))].length);
                    entity2 = Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(rule.getHeadAnchoring()).getProperty("name"))], Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(rule.getHeadAnchoring()).getProperty("name"))].length);
                    sum = sum + cal.calc_sum(entity1, entity2, relation);
                } else {
                    entity1 = Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(rule.getHeadAnchoring()).getProperty("name"))], Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(rule.getHeadAnchoring()).getProperty("name"))].length);
                    entity2 = Arrays.copyOf(Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(groundingOriginal).getProperty("name"))], Engine.entity_vec[(int) Engine.entity2id.get(graph.getNodeById(groundingOriginal).getProperty("name"))].length);
                    sum = sum + cal.calc_sum(entity1, entity2, relation);
                }
            }
            else {
                validTotalPredictions++;
                if(validOriginals.contains(groundingOriginal))
                    validPredictions++;
            }
        }
        pcaTotalPredictions = isFromSubject() ? support : totalPrediction;
        if(support==0){
            return false;
        }
        double score=sum/support;
        score= 1.0 / (1.0 + Math.exp(-score));

        rule.stats.Jaccard=score;

        /*
         * 检查三个阈值是否满足最低标准
         * 1.SUPPORT,二者都满足,既满足head,也满足bodyatoms的
         * 2.CONFIDENCE,二者都满足/只满足bodyatoms的
         * 3.HEAD_COVERAGE，二者都满足/只满足head的
         */
        return true;
    }

    public static boolean qualityCheck(Rule rule) {
        return (rule.stats.support >= Settings.SUPPORT)
                && (rule.getQuality("smoothedConf") >= Settings.CONF)
                && (rule.getHeadCoverage() >= Settings.HEAD_COVERAGE);
    }

    /*
     * Remove the both anchored trivial rule taking the form R(e,Y) <- R(Y,e).
     */
    private boolean trivialCheck(long head, long tail) {
        if(Settings.ALLOW_INS_REVERSE)
            return false;
        return head == tail && length() == 1 && this.head.predicate.equals(bodyAtoms.get(0).predicate);
    }

    private void applyHeadAnchoredRules(SimpleInsRule rule, Set<Long> groundingoriginals, Context context) {
        /*
         * !!!!!!!!!!!!!!!!!!!!在限制lastatom中的tail值的情况下生成答案
         * 确保路径的首尾不是相同的
         * 举例
         * 128->129<-128,虽然可能找到这样的路径
         * 但是这个grounding 128,128不能够作为一个答案，对预测无意义
         * ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
         * 利用找到的grounding生成答案，有可能生成new triple
         * target！！！！！！！！！！！是for循环中设置的，Settings.TARGET = target;
         * 方向！！！！！！！！！！！！是利用fromSubject进行了转换，都是单向，从左往右，与数据集的顺序相同
         * 举例
         * fromSource的HAR
         * Head:X->Y
         * Bodyatom:X->V0,假设Bodyatom中的V0 (tail)值为129,Head中的Y (HeadAnchoring)的值为128
         * 则找到的grounding可以为
         * groundingoriginals1,129
         * groundingoriginals2,129
         * groundingoriginals3,129
         * 生成的答案就是
         * groundingoriginals1，128
         * groundingoriginals2, 128
         * groundingoriginals3, 128
         * 非fromSource的HAR
         * Head:X->Y
         * Bodyatom:Y->V0,假设Bodyatom中的V0 (tail)值为130,Head中的Y (HeadAnchoring)的值为137
         * 则找到的grounding可以为
         * groundingoriginals1,130
         * groundingoriginals2,130
         * groundingoriginals3,130
         * 生成的答案就是
         * 137,groundingoriginals1
         * 137,groundingoriginals2
         * 137,groundingoriginals3
         */
        for (Long groundingoriginal : groundingoriginals) {
            Pair pair = fromSubject ? new Pair(groundingoriginal, rule.headAnchoringId) : new Pair(rule.headAnchoringId, groundingoriginal);
            if(!pair.isSelfloop()) {
                /*
                 * 存放生成答案
                 */
                context.putInPredictionMap(pair, rule);
            }
        }
    }

    private void applyBothAnchoredRules(SimpleInsRule rule, Multimap<Long, Long> tailToOriginals, Context context) {
        /*
         * !!!!!!!!!!!!!!!!!!!!在限制lastatom中的tail值的情况下生成答案
         * 确保路径的首尾不是相同的
         * 举例
         * 128->129<-128,虽然可能找到这样的路径
         * 但是这个grounding 128,128不能够作为一个答案，对预测无意义
         * ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
         * 利用找到的grounding生成答案，有可能生成new triple
         * target！！！！！！！！！！！是for循环中设置的，Settings.TARGET = target;
         * 方向！！！！！！！！！！！！是利用fromSubject进行了转换，都是单向，从左往右，与数据集的顺序相同
         * 举例
         * fromSource的BAR
         * Head:X->Y
         * Bodyatom:X->V0,假设Bodyatom中的V0 (tail)值为129!!!!!!!!!!这个值来源于base,Head中的Y (HeadAnchoring)的值为128
         * 则找到的grounding可以为
         * groundingoriginals1,129
         * groundingoriginals2,129
         * groundingoriginals3,129
         * 生成的答案就是
         * groundingoriginals1，128
         * groundingoriginals2, 128
         * groundingoriginals3, 128
         * 非fromSource的BAR
         * Head:X->Y
         * Bodyatom:Y->V0,假设Bodyatom中的V0 (tail)值为130!!!!!!!!!!这个值来源于base,Head中的Y (HeadAnchoring)的值为137
         * 则找到的grounding可以为
         * groundingoriginals1,130
         * groundingoriginals2,130
         * groundingoriginals3,130
         * 生成的答案就是
         * 137,groundingoriginals1
         * 137,groundingoriginals2
         * 137,groundingoriginals3
         * ！！！！！！！！！！！！！！！！！！！！！
         * 这里的tailAnchoringId是生成这条BAR规则时候设定的值,而前面在checktail当中也有限制tail，那个是限制lastatom的tail值
         */
        for (Long groundingoriginal : tailToOriginals.get(rule.tailAnchoringId)) {
            Pair pair = fromSubject ? new Pair(groundingoriginal, rule.headAnchoringId) : new Pair(rule.headAnchoringId, groundingoriginal);
            if(!pair.isSelfloop()) {
                context.putInPredictionMap(pair, rule);
            }
        }
    }

    private void applyClosedRule(CountedSet<Pair> bodyGroundings, Context context) {
        for (Pair grounding : bodyGroundings) {
            Pair pair = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);
            /*
             * !!!!!!!!!!!!!!!!!!!!在限制lastatom中的tail值的情况下生成答案
             * 确保路径的首尾不是相同的
             * 举例
             * 128->129<-128,虽然可能找到这样的路径
             * 但是这个grounding 128,128不能够作为一个答案，对预测无意义
             * ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
             * 利用找到的grounding生成答案，有可能生成new triple
             * target！！！！！！！！！！！是for循环中设置的，Settings.TARGET = target;
             * 方向！！！！！！！！！！！！是利用fromSubject进行了转换，都是单向，从左往右，与数据集的顺序相同
             * 举例
             * fromSource的closedRule
             * Head:X->Y
             * Bodyatom:X->V0<-Y,假设Bodyatom中的Y值 （tail）为129！！！！！！！！！这个值来源于BAR
             * 则找到的grounding可以为
             * groundingoriginals1,129
             * groundingoriginals2,129
             * groundingoriginals3,129
             * 生成的答案就是
             * groundingoriginals1，129
             * groundingoriginals2, 129
             * groundingoriginals3, 129
             * 非fromSource的closedRule
             * Head:X->Y Bodyatom:Y<-X,假设Bodyatom中的X值 (tail)为128！！！！！！！！！这个值来源于BAR
             * 则找到的grounding可以为
             * groundingoriginals1,128
             * groundingoriginals2,128
             * groundingoriginals3,128
             * 生成的答案就是
             * 128,groundingoriginals1
             * 128,groundingoriginals2
             * 128,groundingoriginals3
             */
            if(!pair.isSelfloop()) {
                /*
                 * 存放生成答案
                 */
                context.putInPredictionMap(grounding, this);
            }
        }
    }

    @Override
    public String toString() {
        String header = isClosed() ? "CAR\t" : "OAR\t";
        return header + super.toString();
    }

    public String toRuleIndexString() {
        String str = isClosed() ? "CAR\t" : "OAR\t";
        str += head + " <- ";
        List<String> atoms = new ArrayList<>();
        bodyAtoms.forEach( atom -> atoms.add(atom.toRuleIndexString()));
        return str + String.join(", ", atoms);
    }
}