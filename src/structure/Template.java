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
         *���ѱ�������length>1��bodygroudings
         */
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this
                );
        /*
         * tempFile,�����ͷֱ�д���ļ����У���ʽ������ʾ,CAR��closdRule��OAR��instantiatedRule
         * ABS: 26409	CAR	CanFollow(X,Y) <- CanPrecede(Y,X,1)	0.65854	0.62069	0.76056	0.62069	0.65854	0.53571
         * ABS: 24574	OAR	CanFollow(X,Y) <- CanFollow(V1,Y,1), targetof(V1,V2,0), PeerOf(V3,V2,1)	0.75	0.46154	0.75	0.46154	0.07317	0	0.51748
         * 0,38,0.75,0.46154,0.75,0.46154,1,0	2,38,1186,1,0.54545,1,0.54545,1,0	2,38,1142,1,0.54545,1,0.54545,1,0 ����Ӹ���HAR����BAR��
         * ruleFile ��rule.txt��,���������͵Ĺ���д���ļ�����д��Closed�Ĺ��� (len=1,len=2,len=3)���Լ�
         * ���е�HAR����BAR�Ĺ��� (len=2,len=3)��len=1��HAR����BAR��generateEssentialRules�����Ѿ�д��
         */
        if(closed) {
            /*
             * evalClosedRule��ͳ��ָ��
             *              * standardConf:���߶�����/������bodyatom��ʽ��ص�ֵ
             *              * pcaConf:fromSource��ʱ��pcaConf=1����fromSource��ʱ����standardConf��ֵ��ͬ
             *              * headCoverage:���߶�����/����ͷ��
             *              * �ǳ��õ�����ָ�꣡������������������������������
             *              * smoothedConf����standardConf�Ļ����ϼ���һ��ƫ�Ƶı���
             *              * apcaConf�����һ��һ�Ƚ϶࣬��ѡ��ʹ��pcaConf������ʹ��smoothedConf
             *              * validPrecision��
             *              * ѵ�����в�����ķŵ���֤�������Ԥ��
             *              * validTotalPredictions=������ѵ�����У��޶�headanchoringֵ�Ժ󣬲�������������
             *              * validPredictions�ڴ���HeadAnchoringֵ��validPair�Ժ���֤���з��Ϲ��򣬼�����headҲ����bodyatom�ĵ�����
             * closedRule��bodyGrounding
             * context.getIndex(this),��indexRule���л�ȡindex
             * this.toRuleIndexString()�����������
             * closedRuleͬʱд��tempFile���Լ�ruleFile����
             */
             if(evalClosedRule(bodyGroundings, trainPairs, validPair,graph)) {
                 /*
                  * ͳ��Specialized���Ĺ���
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
             * ����������������groundtruth��instantiatedRule,HAR,BAR���õĲ�ͬ
             * ����������instantiatedRule��groundTruth����û����HeadAnchoring����tail������£�����ͷ������
             * ������HAR������HeadAnchoring������£�����ͷ������
             * ������BAR������HeadAnchoring��tail������£�����ͷ������!!!!!!!
             * ����tail��Ӱ��Head�����HAR����ͷ��������BAR����ͷ��������ͬ
             */
            stats.groundTruth = trainPairs.size();
            Multimap<Long, Long> originalToTail = MultimapBuilder.hashKeys().hashSetValues().build();
            Multimap<Long, Long> tailToOriginal = MultimapBuilder.hashKeys().hashSetValues().build();
            for (Pair bodyGrounding : bodyGroundings) {
                originalToTail.put(bodyGrounding.subId, bodyGrounding.objId);
                tailToOriginal.put(bodyGrounding.objId, bodyGrounding.subId);
            }

            /*
             * ��һ��ѭ�����޶�Head�ٽ��м���
             */
            for (Long anchoring : trianAnchoringToOriginals.keySet()) {


                String headName = (String) graph.getNodeById(anchoring).getProperty(Settings.NEO4J_IDENTIFIER);
                Rule HAR = new InstantiatedRule(this, headName, anchoring);
                /*
                 * ���޶�HeadAnchoring�������£�ͳ�Ƹ���ָ��
                 *  evaluateRule�����ȿ���ͳ��HARҲ����ͳ��BAR
                 */
                Set<Long> groundingOriginals = originalToTail.keySet();
                Collection<Long> validOriginals = validAnchoringToOriginals.get(anchoring);
                Collection<Long> originals= trianAnchoringToOriginals.get(anchoring);
                if(evaluateRule(HAR, originals ,validOriginals, groundingOriginals,graph)) {
                    /*
                     * ͳ�Ƶ�instantiatedRule����
                     */
                    stats.support += HAR.stats.support;
                    stats.totalPredictions += HAR.stats.totalPredictions;
                    stats.pcaTotalPredictions += HAR.stats.pcaTotalPredictions;
                    /*
                     * ͳ��HAR��BAR��������
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
                 * �ڶ���ѭ�����޶�Head��tail���ٽ��м���
                 */
                for (Long original : trianAnchoringToOriginals.get(anchoring)) {

                    for (Long tail : originalToTail.get(original)) {
                        Pair candidate = new Pair(anchoring, tail);
                        if(!trivialCheck(anchoring, tail)) {
                            candidate.subName = headName;
                            candidate.objName = (String) graph.getNodeById(tail).getProperty(Settings.NEO4J_IDENTIFIER);
                            Rule BAR = new InstantiatedRule(this, candidate);
                            /*
                             * ���޶�HeadAnchoring��tail�������£�ͳ�Ƹ���ָ��
                             */
                            groundingOriginals= (Set<Long>) tailToOriginal.get(tail);
                            if (evaluateRule(BAR, originals, validOriginals,groundingOriginals,graph)) {
                                /*
                                 * ͳ��HAR��BAR��������
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
             * ��instantiatedRule���з����ļ��㣬ǰ���Ѿ��Ѹ���HAR����BAR��ֵ������ȥ
             */
            stats.compute();
            if(!contents.isEmpty()) {
                /*
                 * ͳ��instantiatedRule�Ƿ��������HAR����BAR����������������ŵ�SpecializedRules����
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
         * �ҵ�����rule������bodygrounding��Ҫִ��checktail
         * checktail�����ã�����grounding��ֵ��grounding��lastatom��tailֵ��ͬ
         * ����������������������������������������������
         * ����specialization��generalization����grounding��ֻ������ʽ������application����grounding������tailֵ
         * ��������������������������������
         * �õ���OAR������tailֵ������
         */
        CountedSet<Pair> bodyGroundings = GraphOps.bodyGroundingCoreAPI(graph, this);
        Set<Long> originals = Sets.newHashSet();
        Multimap<Long, Long> tailToOriginals = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair grounding : bodyGroundings) {
            originals.add(grounding.subId);
            tailToOriginals.put(grounding.objId, grounding.subId);
        }
        /*
         * ����������������������������
         * ������new triple
         * ��closedRule���ɴ�
         */
        if (closed) applyClosedRule(bodyGroundings, context);
        else {
            for (SimpleInsRule rule : insRules) {

                /*
                 * ��HAR���ɴ�
                 */
                if (rule.type == 0)
                    applyHeadAnchoredRules(rule, originals, context);
                /*
                 * ��BAR���ɴ�
                 */
                else if (rule.type == 2)
                    applyBothAnchoredRules(rule, tailToOriginals, context);
            }
            /*
             * ��RuleReader reader = new RuleReader�����¶��µĹ���
             */
            insRules.clear();
        }
    }

    private boolean evalClosedRule(CountedSet<Pair> bodyGroundings, Set<Pair> trainPairs, Set<Pair> validPair,GraphDatabaseService graph) {
        double totalPrediction = 0, support = 0, pcaTotalPrediction = 0
                , validTotalPredictions = 0, validPredictions = 0;
        /*
         * ͳ������TrainPairs�е�Xֵ
         */
        Set<Long> subs = new HashSet<>();
        for (Pair pair : trainPairs)
            subs.add(pair.subId);
        /*
         * totalPrediction������bodyatom������
         * correctPrediction�����߶����������
         *
         **/
        /*
         * ��������������������������
         * �����޶������
         * ͳ����train��valid�����ж�������������������
         * totalPredictions ��standard��������bodyatom��ʽgrounding��������grounding��������Դ��ͼ����train���ɵ�ͼ��
         * support������headҲ����bodyatom�ĵ�����
         * ��������������������������
         * ѵ�����в�����ķŵ���֤�������Ԥ��
         * validTotalPredictions=ѵ�����У�����������������
         * validPredictions��֤���м�����headҲ����bodyatom�ĵ�����
         * ����������������������������������������
         * pcaTotalPredictions ��pca���ο�AMIE+����⣬ʹ���غϵ�Xֵ������
         * ����
         * fromsource��ʱ��
         * Head��X->Y  bodyatom��X->V0->Y
         * �ҵĹ���Head:X->Y (��trainPair�л�ȡ),Bodyatom:X->V0->Y(��grounding�л�ȡ)
         * �����������ߣ�����Head������bodyatom�����
         * ��fromsource��ʱ��
         * Head��X->Y  bodyatom��Y<-X
         * ���ҵĹ���Head:X->Y (��trainPair�л�ȡ),Bodyatom:X->Y(��grounding�л�ȡ)
         * �����������ߣ�����Head������bodyatom�����
         */
        for (Pair grounding : bodyGroundings) {
            long sub = fromSubject ? grounding.subId : grounding.objId;
            /*
             * ͳ��Head��BodyAtom�У��ж���Xֵ���غϵ�
             * ����������������������������������������
             * ��������AMIE+�Ķ�����ͬ
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
         * ����head������
         */
        int groundtruth = trainPairs.size();
        setStats(support, totalPrediction, pcaTotalPrediction
                , groundtruth, validTotalPredictions, validPredictions);

        if(!qualityCheck(this)){
            return false;
        }
        /*
         * ���������ֵ�Ƿ�������ͱ�׼
         * 1.SUPPORT,���߶�����,������head,Ҳ����bodyatoms��
         * 2.CONFIDENCE,���߶�����/ֻ����bodyatoms��
         * 3.HEAD_COVERAGE�����߶�����/ֻ����head��
         */

        double sum=0;
        Caculate cal=new Caculate();
        for (Pair grounding : bodyGroundings) {
            long sub = fromSubject ? grounding.subId : grounding.objId;
            /*
             * ͳ��Head��BodyAtom�У��ж���Xֵ���غϵ�
             * ����������������������������������������
             * ��������AMIE+�Ķ�����ͬ
             */
            if(subs.contains(sub))
                pcaTotalPrediction++;

            Pair prediction = fromSubject ? grounding : new Pair(grounding.objId, grounding.subId);

            //�������ͬ�ģ���������trainPairs�ķ���
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
         * ���������ֵ�Ƿ�������ͱ�׼
         * 1.SUPPORT,���߶�����,������head,Ҳ����bodyatoms��
         * 2.CONFIDENCE,���߶�����/ֻ����bodyatoms��
         * 3.HEAD_COVERAGE�����߶�����/ֻ����head��
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
         * !!!!!!!!!!!!!!!!!!!!������lastatom�е�tailֵ����������ɴ�
         * ȷ��·������β������ͬ��
         * ����
         * 128->129<-128,��Ȼ�����ҵ�������·��
         * �������grounding 128,128���ܹ���Ϊһ���𰸣���Ԥ��������
         * ����������������������������������������������������������������������������
         * �����ҵ���grounding���ɴ𰸣��п�������new triple
         * target������������������������forѭ�������õģ�Settings.TARGET = target;
         * ���򣡣���������������������������fromSubject������ת�������ǵ��򣬴������ң������ݼ���˳����ͬ
         * ����
         * fromSource��HAR
         * Head:X->Y
         * Bodyatom:X->V0,����Bodyatom�е�V0 (tail)ֵΪ129,Head�е�Y (HeadAnchoring)��ֵΪ128
         * ���ҵ���grounding����Ϊ
         * groundingoriginals1,129
         * groundingoriginals2,129
         * groundingoriginals3,129
         * ���ɵĴ𰸾���
         * groundingoriginals1��128
         * groundingoriginals2, 128
         * groundingoriginals3, 128
         * ��fromSource��HAR
         * Head:X->Y
         * Bodyatom:Y->V0,����Bodyatom�е�V0 (tail)ֵΪ130,Head�е�Y (HeadAnchoring)��ֵΪ137
         * ���ҵ���grounding����Ϊ
         * groundingoriginals1,130
         * groundingoriginals2,130
         * groundingoriginals3,130
         * ���ɵĴ𰸾���
         * 137,groundingoriginals1
         * 137,groundingoriginals2
         * 137,groundingoriginals3
         */
        for (Long groundingoriginal : groundingoriginals) {
            Pair pair = fromSubject ? new Pair(groundingoriginal, rule.headAnchoringId) : new Pair(rule.headAnchoringId, groundingoriginal);
            if(!pair.isSelfloop()) {
                /*
                 * ������ɴ�
                 */
                context.putInPredictionMap(pair, rule);
            }
        }
    }

    private void applyBothAnchoredRules(SimpleInsRule rule, Multimap<Long, Long> tailToOriginals, Context context) {
        /*
         * !!!!!!!!!!!!!!!!!!!!������lastatom�е�tailֵ����������ɴ�
         * ȷ��·������β������ͬ��
         * ����
         * 128->129<-128,��Ȼ�����ҵ�������·��
         * �������grounding 128,128���ܹ���Ϊһ���𰸣���Ԥ��������
         * ����������������������������������������������������������������������������
         * �����ҵ���grounding���ɴ𰸣��п�������new triple
         * target������������������������forѭ�������õģ�Settings.TARGET = target;
         * ���򣡣���������������������������fromSubject������ת�������ǵ��򣬴������ң������ݼ���˳����ͬ
         * ����
         * fromSource��BAR
         * Head:X->Y
         * Bodyatom:X->V0,����Bodyatom�е�V0 (tail)ֵΪ129!!!!!!!!!!���ֵ��Դ��base,Head�е�Y (HeadAnchoring)��ֵΪ128
         * ���ҵ���grounding����Ϊ
         * groundingoriginals1,129
         * groundingoriginals2,129
         * groundingoriginals3,129
         * ���ɵĴ𰸾���
         * groundingoriginals1��128
         * groundingoriginals2, 128
         * groundingoriginals3, 128
         * ��fromSource��BAR
         * Head:X->Y
         * Bodyatom:Y->V0,����Bodyatom�е�V0 (tail)ֵΪ130!!!!!!!!!!���ֵ��Դ��base,Head�е�Y (HeadAnchoring)��ֵΪ137
         * ���ҵ���grounding����Ϊ
         * groundingoriginals1,130
         * groundingoriginals2,130
         * groundingoriginals3,130
         * ���ɵĴ𰸾���
         * 137,groundingoriginals1
         * 137,groundingoriginals2
         * 137,groundingoriginals3
         * ������������������������������������������
         * �����tailAnchoringId����������BAR����ʱ���趨��ֵ,��ǰ����checktail����Ҳ������tail���Ǹ�������lastatom��tailֵ
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
             * !!!!!!!!!!!!!!!!!!!!������lastatom�е�tailֵ����������ɴ�
             * ȷ��·������β������ͬ��
             * ����
             * 128->129<-128,��Ȼ�����ҵ�������·��
             * �������grounding 128,128���ܹ���Ϊһ���𰸣���Ԥ��������
             * ����������������������������������������������������������������������������
             * �����ҵ���grounding���ɴ𰸣��п�������new triple
             * target������������������������forѭ�������õģ�Settings.TARGET = target;
             * ���򣡣���������������������������fromSubject������ת�������ǵ��򣬴������ң������ݼ���˳����ͬ
             * ����
             * fromSource��closedRule
             * Head:X->Y
             * Bodyatom:X->V0<-Y,����Bodyatom�е�Yֵ ��tail��Ϊ129���������������������ֵ��Դ��BAR
             * ���ҵ���grounding����Ϊ
             * groundingoriginals1,129
             * groundingoriginals2,129
             * groundingoriginals3,129
             * ���ɵĴ𰸾���
             * groundingoriginals1��129
             * groundingoriginals2, 129
             * groundingoriginals3, 129
             * ��fromSource��closedRule
             * Head:X->Y Bodyatom:Y<-X,����Bodyatom�е�Xֵ (tail)Ϊ128���������������������ֵ��Դ��BAR
             * ���ҵ���grounding����Ϊ
             * groundingoriginals1,128
             * groundingoriginals2,128
             * groundingoriginals3,128
             * ���ɵĴ𰸾���
             * 128,groundingoriginals1
             * 128,groundingoriginals2
             * 128,groundingoriginals3
             */
            if(!pair.isSelfloop()) {
                /*
                 * ������ɴ�
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