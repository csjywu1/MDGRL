package uk.ac.ncl.model;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import uk.ac.ncl.Experiment;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.*;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import com.google.common.collect.Multimap;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Triple;

import java.io.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GPFL extends Engine {
    public void close() {
        if (graph != null) {
            graph.shutdown();
        }
    }

    public GPFL(File config, String logName) {
        super(config, logName);
        Logger.println("# Graph Path Feature Learning (GPFL) System\n" +
                "# Version: " + Settings.VERSION +  " | Date: " + Settings.DATE, 1);
        Logger.println(MessageFormat.format("# Cores: {0} | JVM RAM: {1}GB | Physical RAM: {2}GB"
                , runtime.availableProcessors()
                , Helpers.JVMRam()
                , Helpers.systemRAM()), 1);

        Helpers.reportSettings();
    }

    private void Read_Vec_File(String file_name, double[][] vec) throws IOException {
        File f = new File(file_name);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f),"UTF-8"));
        String line;
        for (int i = 0; (line = reader.readLine()) != null; i++) {
            String[] line_split = line.split("\t");
            for (int j = 0; j < vector_len; j++) {
                vec[i][j] = Double.valueOf(line_split[j]);
            }
        }
    }

    public void run() throws IOException {
        /**
         * 读取entity和id
         * 读取relation和id
         */
        entity2id=new HashMap();
        relation2id=new HashMap();

        File relationFile = new File("data/"+ Experiment.D+"/relation2id.txt");
        LineIterator relationid = FileUtils.lineIterator(relationFile);//it save head and tail
        while (relationid.hasNext()) {
            ArrayList<Integer> template=new ArrayList<Integer>();
            String[] words = relationid.next().split(",");
            relation2id.put(words[0],Integer.valueOf(words[1]));
        }

        File entityFile = new File("data/"+Experiment.D+"/entity2id.txt");
        LineIterator entityid = FileUtils.lineIterator(entityFile);//it save head and tail
        while (entityid.hasNext()) {
            ArrayList<Integer> template=new ArrayList<Integer>();
            String[] words = entityid.next().split(",");
            entity2id.put(words[0],Integer.valueOf(words[1]));
        }



        /**
         * 读取relation和entity文件有多少行
         */
        int i=0;
        File f = new File("data/"+Experiment.D+"/"+Experiment.M+"/relation2vec.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f),"UTF-8"));
        String line;
        for (i = 0; (line = reader.readLine()) != null; i++) {
        }

        int j=0;
        f = new File("data/"+Experiment.D+"/"+Experiment.M+"/entity2vec.txt");
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(f),"UTF-8"));
        for (j = 0; (line = reader.readLine()) != null; j++) {
        }


        /**
         * 读取PTransE训练得到的向量
         */
        relation_num=i;
        entity_num=j;

        relation_vec = new double[relation_num][vector_len];
        entity_vec = new double[entity_num][vector_len];

        Read_Vec_File("data/"+Experiment.D+"/"+Experiment.M+"/relation2vec.txt", relation_vec);
        Read_Vec_File("data/"+Experiment.D+"/"+Experiment.M+"/entity2vec.txt", entity_vec);

        /**
         * ===============
         * 前面的都是准备工作
         * ===============
         */


        graphFile = new File(home, "databases/graph.db");
        /*
         * 用来训练的图，只包含train部分的数据集
         */
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();

        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();


        /**
         * 读取所有的target
         */
        Map relation2id=new HashMap();

        relationFile = new File("data/"+Experiment.D+"/"+"relation2id.txt");
        relationid = FileUtils.lineIterator(relationFile);//it save head and tail
        while (relationid.hasNext()) {
            ArrayList<Integer> template=new ArrayList<Integer>();
            String[] words = relationid.next().split(",");
            relation2id.put(words[0],Integer.valueOf(words[1]));
        }

        targets=relation2id.keySet();

        /*
         * Learn rules for every target
         */

        targets.remove("instanceof");

        for (String target : targets) {
            File ruletempFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt"));
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                r=target;

                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                /*
                 * 一对一的节点大于90%，就叫做目标功能是否正常，TARGET_FUNCTIONAL
                 */
                Logger.println(MessageFormat.format("# Functional: {0} | Train Size: {1} | Valid Size: {2} | Test Size: {3}"
                        , Settings.TARGET_FUNCTIONAL, trainPairs.size(), validPairs.size(), testPairs.size()), 1);
                /*
                 * trainPairs, validPairs, testPairs的数据不重复，使用Set数据格式存储
                 */
                Set<Pair> filterSet = Helpers.combine(trainPairs, validPairs, testPairs);
                /*
                 * 生成closed rule以及instantied rule
                 */
                generalization(trainPairs, context);
                /*
                 * 对path=1的instantied rule拓展，生成HAR或者BAR
                 * 定义规则的读写类
                 * tempFiltercontents只写入CAR开头的或者OAR开头的后接HAR或者BAR，对应的是各种类型的文件夹
                 * ruleFilecontens只写入CAR,HAR或者BAR，对应只有一个rules.txt文件夹
                 *
                 */
//                if(Settings.ESSENTIAL_TIME != -1 && Settings.INS_DEPTH != 0)
//                    EssentialRuleGenerator.generateEssentialRules(trainPairs, validPairs
//                            , context, graph, ruletempFile, ruleFile);
                /*
                 * 对path>1的instantied rule拓展，生成HAR或者BAR
                 * ！！！！！！！！！！！！！！！！！！！！！！！！！！！
                 * generalization没把closed的规则写进去，specialization把closed的规则也写进去了
                 * tempFiltercontents两种方式写入，一类是ABS:closed的，另一类是ABS:instantiated开头的，后接HAR或者BAR，对应的是各种类型的文件夹
                 * ruleFilecontens写入HAR或者BAR以及Closed的，对应只有一个rules.txt文件夹
                 */
                specialization(context, trainPairs, validPairs, ruletempFile);
                /*
                 * 对各类target的规则整体排序
                 * 计算平均值，并用整体的分数值，降序排序
                 * ClosedRule，InstantiatedRule（后接HAR,BAR）按分数降序排序
                 */
                IO.orderRuleIndexFile(ruletempFile);
                /*
                 * ！！！！！！！！！！！query中会生成newTriple
                 * 生成predictionMap （答案集合），key是query，value是所有的answer值
                 */
                ruleApplication(context, ruletempFile);
                /*
                 *用train数据集的答案集合，用validation去除过拟合的答案，完成对test的预测
                 */
                Evaluator evaluator = new Evaluator(testPairs, filterSet, context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                context.ruleFrequency.clear();
                context.indexRule.clear();
                tx.success();

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();
        /*
         *记录结果
         * hits@3，hits@5，hits@10，mrr，不需要计算map，在这里的应用，map的值与mrr相同
         * 当POST_FILTERING为true时，filterMap为空。filterMap的值为训练集所有的数据，train，validation，test之间的数据可以出现重复
         * 前面的变量filterSet的值train，validation，test之间的数据没重复
         * ！！！！！！！！！！！！！！！采用两种方式，减少预测的竞争性
         *public static boolean PRIOR_FILTERING = true;答案只考虑已经存在测试集中的答案或未知的答案，排名的时候证明排名！！！！！！！！！目前采用的是这种方式
         *public static boolean POST_FILTERING = false;用的是filterMap是所有数据集，train、validation、test之间会有重复，在排名中不将xxxxxx算进答案的排名当中
         * * filterMap,是所有数据集，train、validation、test之间会有重复
         * * filterSet，获取与test的谓语相同的数据集，一般test中的数据的谓语和前面train、validation都是相同的，因此获取为整个数据集的数据
         ** ！！！！！！！！！！！！!!!!!!!!!
         * 有两个filterSet
         * 1.一个是前面写的定义，用于过滤答案，只添加已经存在测试集中的答案（含正确，也含错误）或未知的答案
         * 2. 另一个是在scoreGPFL(filterMap, predictionFile)函数里用于计算排名的，只考虑未知的答案，以及测试集当中正确的那个答案
         */
        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }

    public void learn() throws IOException {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);

        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();
        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/<>]", "_") + ".txt"));

            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);

                Logger.println(MessageFormat.format("# Train Size: {0}", trainPairs.size()), 1);

                generalization(trainPairs, context);
//                if(Settings.ESSENTIAL_TIME != -1 && Settings.INS_DEPTH != 0)
//                    EssentialRuleGenerator.generateEssentialRules(trainPairs, validPairs, context, graph, ruleIndexFile, ruleFile);
//                specialization(context, trainPairs, validPairs, ruleIndexFile);

                IO.orderRuleIndexFile(ruleIndexFile);
                tx.success();
            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        IO.orderRules(out);
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();
    }

    public void apply() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");

        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        populateTargets();
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt");
            if(!ruleIndexFile.exists())
                continue;

            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Applying Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                Set<Pair> filterSet = Helpers.combine(trainPairs, validPairs, testPairs);
                Logger.println(MessageFormat.format("# Train Size: {0} | " + "Valid Size: {1} | " + "Test Size: {2}"
                        , trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                ruleApplication(context, ruleIndexFile);
                Evaluator evaluator = new Evaluator(testPairs, filterSet, context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                tx.success();
            }
        }

        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();

        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }
}
