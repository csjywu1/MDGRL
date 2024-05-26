package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Rule;
import uk.ac.ncl.structure.Triple;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/*
 * A new AnyBURL style evaluator replacing the original one.
 */
public class Evaluator {
    final private BlockingQueue<Pair> testPairs;
    Multimap<Long, Pair> subIndex = MultimapBuilder.hashKeys().hashSetValues().build();
    Multimap<Long, Pair> objIndex = MultimapBuilder.hashKeys().hashSetValues().build();

    GraphDatabaseService graph;
    BlockingQueue<String> predictionContentQueue = new LinkedBlockingDeque<>(100000);
    BlockingQueue<String> verificationContentQueue = new LinkedBlockingDeque<>(100000);
    File predictionFile;
    File verificationFile;
    Multimap<Pair, Rule> candidates;
    Set<Pair> filterSet;

    static DecimalFormat f = new DecimalFormat("####.#####");
    static Multimap<String, Integer> rankMap;
    static Multimap<String, Integer> headMap;
    static Multimap<String, Integer> tailMap;

    public Evaluator(Set<Pair> testPairs
            , Set<Pair> filterSet
            , Context context
            , File predictionFile
            , File verificationFile
            , GraphDatabaseService graph) {
        this.testPairs = new LinkedBlockingDeque<>(testPairs);
        this.predictionFile = predictionFile;
        this.verificationFile = verificationFile;
        this.candidates = context.getPredictionMultiMap();
        this.graph = graph;
        this.filterSet = filterSet;
        /*
         * 整理train生成答案的集合
         */
        for (Pair pair : candidates.keySet()) {
            subIndex.put(pair.subId, pair);
            objIndex.put(pair.objId, pair);
        }
    }

    public void createQueries() {
        long s = System.currentTimeMillis();
        /*
         * writer写入predictions.txt，predictionWriter是predictionContentQueue的监听者
         * writer1写入verification.txt，verificationWriter是verificationContentQueue的监听者
         */
        WriterTask predictionWriter = new WriterTask( predictionFile, predictionContentQueue);
        WriterTask verificationWriter = new WriterTask(verificationFile, verificationContentQueue);


        Thread queryCreators=new QueryCreator(0,predictionWriter,verificationWriter);
        try {
            queryCreators.join();
            predictionWriter.join();
            verificationWriter.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Helpers.timerAndMemory(s, "# Create Queries");
    }

    static public void evalAnyBURL(String home) {
        Logger.init(new File(home, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = buildFilterMap(home);
        scoreAnyBURL(filterMap, new File(home, "predictions.txt"));
    }

    static public Multimap<String, Triple> buildFilterMap(String home) {
        Multimap<String, Triple> filterMap = MultimapBuilder.hashKeys().arrayListValues().build();
        if(Settings.POST_FILTERING) {
            readTriples(new File(home, "data/train.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
            readTriples(new File(home, "data/test.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
            readTriples(new File(home, "data/valid.txt")).forEach(triple -> filterMap.put(triple.pred, triple));
        }
        return filterMap;
    }

    static public Set<Triple> readTriples(File f) {
        Set<Triple> triples = new HashSet<>();
        try(LineIterator l = FileUtils.lineIterator(f)) {
            while(l.hasNext()) {
                triples.add(new Triple(l.nextLine(), 0));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return triples;
    }

    static private void scoreAnyBURL(Multimap<String, Triple> filterMap, File file) {
        rankMap = MultimapBuilder.hashKeys().arrayListValues().build();
        headMap = MultimapBuilder.hashKeys().arrayListValues().build();
        tailMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try(LineIterator l = FileUtils.lineIterator(file)) {
            while(l.hasNext()) {
                Triple testTriple = new Triple(l.nextLine(), 0);
                int headRank = readAnyBURLRank(filterMap.get(testTriple.pred), testTriple, l.nextLine());
                int tailRank = readAnyBURLRank(filterMap.get(testTriple.pred), testTriple, l.nextLine());
                rankMap.put(testTriple.pred, headRank);
                rankMap.put(testTriple.pred, tailRank);
                headMap.put(testTriple.pred, headRank);
                tailMap.put(testTriple.pred, tailRank);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        reportResults();
    }

    static private void reportResults() {
        Multimap<String, Double> perPredicateResults = MultimapBuilder.hashKeys().arrayListValues().build();
        /*
         * 统计每一个谓语的情况
         */
        for (String predicate : rankMap.keySet()) {
            printResultsSinglePredicate(predicate, new ArrayList<>(headMap.get(predicate))
                    , new ArrayList<>(tailMap.get(predicate))
                    , new ArrayList<>(rankMap.get(predicate))
                    , 3, perPredicateResults, true);
        }
        /*
         * 计算所有谓语的总体情况
         * TransE,是直接以head，tail，all为单位求和，然后除于head，tail，all中的个数
         * GPFL,是对每个谓语的均值进行累加，然后除谓语的个数
         * All,是既输出TransE的结果，也输出GPFL的结果
         */
        if(Settings.EVAL_PROTOCOL.equals("TransE")) {
            printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                    , new ArrayList<>(tailMap.values())
                    , new ArrayList<>(rankMap.values())
                    , 1, perPredicateResults, false);
        } else if(Settings.EVAL_PROTOCOL.equals("GPFL")) {
            printAverageResults(perPredicateResults);
        } else if(Settings.EVAL_PROTOCOL.equals("All")) {
            printResultsSinglePredicate("TransE Protocol - All Targets", new ArrayList<>(headMap.values())
                    , new ArrayList<>(tailMap.values())
                    , new ArrayList<>(rankMap.values())
                    , 1, perPredicateResults, false);
            printAverageResults(perPredicateResults);
        } else {
            System.err.println("# Unknown Evaluation Protocol is selected.");
            System.exit(-1);
        }
    }

    static public void printAverageResults(Multimap<String, Double> perPredicateResults) {
        DecimalFormat f = new DecimalFormat("###.####");
        f.setMinimumFractionDigits(4);
        StringBuilder sb = new StringBuilder("# " + "GPFL Protocol - All Targets:\n"
                + "#           Head     Tail     All\n");
        double[] scores = new double[12];
        /*
         *hits@1的head，tail，all，hits@3的head，tail，all，hits@10的head，tail，all，mrr的head，tail，all
         * 对上面的值从左往右按谓语求和
         * score[0]是对所有谓语的hits@1的head值累加，score[1]是对所有谓语的hits@1的tail值累加
         */
        for (String predicate : perPredicateResults.keySet()) {
            List<Double> predicateScores = new ArrayList<>(perPredicateResults.get(predicate));
            assert predicateScores.size() == 12;
            for (int i = 0; i < predicateScores.size(); i++) {
                scores[i] += predicateScores.get(i);
            }
        }
        /*
         *将上面的值,除谓语的个数
         */
        for (int i = 0; i < scores.length; i++) {
            scores[i] = scores[i] / perPredicateResults.keySet().size();
        }
        int[] ns = new int[]{3,5};
        int index = 0;
        for (int n : ns) {
            sb.append("# hits@" + n + ":   ");
            sb.append((f.format(scores[index++]) + "   "));
            sb.append((f.format(scores[index++]) + "   "));
            sb.append((f.format(scores[index++]) + "\n"));
        }
        sb.append("# hits@10:  ");
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "\n"));

        sb.append("# MRR:      ");
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "   "));
        sb.append((f.format(scores[index++]) + "\n"));
        Logger.println(sb.toString(), 1);
    }

    static public void printResultsSinglePredicate(String predicate, List<Integer> headRanks, List<Integer> tailRanks
            , List<Integer> allRanks, int verb, Multimap<String, Double> perPredicateResults, boolean add) {
        DecimalFormat f = new DecimalFormat("###.####");
        double headScore, tailScore, allScore;
        f.setMinimumFractionDigits(4);
        int[] ns = new int[]{3,5};
        /*
         * sb用于记录写进eval_log.txt中的预测内容
         */
        StringBuilder sb = new StringBuilder("# " + predicate + ":\n#           Head     Tail     All\n");
        /*
         * 统计1和3当中命中了的结果
         * predicate记录谓语中的结果，从左往右，至上往下，顺序如下所示
         * hits@1的head，tail，all，hits@3的head，tail，all，hits@10的head，tail，all
         */
        for (int n : ns) {
            headScore = hitsAt(headRanks, n);
            if(add) perPredicateResults.put(predicate, headScore);
            tailScore = hitsAt(tailRanks, n);
            if(add) perPredicateResults.put(predicate, tailScore);
            allScore = hitsAt(allRanks, n);
            if(add) perPredicateResults.put(predicate, allScore);

            sb.append("# hits@" + n + ":   ");
            sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
            sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
            sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));

        }
        headScore = hitsAt(headRanks, 10);
        if(add) perPredicateResults.put(predicate, headScore);
        tailScore = hitsAt(tailRanks, 10);
        if(add) perPredicateResults.put(predicate, tailScore);
        allScore = hitsAt(allRanks, 10);
        if(add) perPredicateResults.put(predicate, allScore);
        sb.append("# hits@10:  ");
        sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
        sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
        sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));

        /*
         * mrr
         * Mean Reciprocal Rank(MRR)——越大越好
         * 返回所有正确答案的预测排名的倒数的均值t。对于一个query，若第一个正确答案排在第n位，则MRR就是1/n。
         * 最大值是1
         */
        headScore = mrr(headRanks);
        if(add) perPredicateResults.put(predicate, headScore);
        tailScore = mrr(tailRanks);
        if(add) perPredicateResults.put(predicate, tailScore);
        allScore = mrr(allRanks);
        if(add) perPredicateResults.put(predicate, allScore);
        sb.append("# MRR:      ");
        sb.append((headRanks.isEmpty() ? "No Records\t" : f.format(headScore) + "   "));
        sb.append((tailRanks.isEmpty() ? "No Records\t" : f.format(tailScore) + "   "));
        sb.append((allRanks.isEmpty() ? "No Records\n" : f.format(allScore) + "\n"));
        /*
         * verb小于3的时候才输出结果，否则不输出，直接写入文件里
         * 将每个谓语的结果填进eval_log.txt当中
         */
        Logger.println(sb.toString(), verb);
    }

    static private int readAnyBURLRank(Collection<Triple> filterSet, Triple testTriple, String line) {
        boolean headQuery = line.startsWith("Heads: ");
        String[] splits = headQuery ? line.split("Heads: ") : line.split("Tails: ");
        if(splits.length <= 1) return 0;
        String[] words = splits[1].split("\t");

        List<Triple> currentAnswers = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            if(i % 2 == 0) {
                if(headQuery)
                    currentAnswers.add(new Triple(words[i], testTriple.pred, testTriple.obj));
                else
                    currentAnswers.add(new Triple(testTriple.sub, testTriple.pred, words[i]));
            }
        }
        int filterCount = 0;
        int rank = 0;
        for (Triple currentAnswer : currentAnswers) {
            if(filterSet.contains(currentAnswer) && !currentAnswer.equals(testTriple))
                filterCount++;
            if(currentAnswer.equals(testTriple)) {
                rank = currentAnswers.indexOf(currentAnswer);
                return rank - filterCount + 1;
            }
        }
        return rank;
    }

    static public void evalGPFL(String home) {
        Logger.init(new File(home, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home);
        scoreGPFL(filterMap, new File(home, "predictions.txt"));
    }

    static public void scoreGPFL(Multimap<String, Triple> filterMap, File predictionFile) {
        rankMap = MultimapBuilder.hashKeys().arrayListValues().build();
        headMap = MultimapBuilder.hashKeys().arrayListValues().build();
        tailMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try(LineIterator l = FileUtils.lineIterator(predictionFile)) {
            while(l.hasNext()) {
                String line = l.nextLine();
                boolean headQuery = line.startsWith("Head Query: ");
                Triple testTriple = headQuery ? new Triple(line.split("Head Query: ")[1], 1) :
                        new Triple(line.split("Tail Query: ")[1], 1);
                List<Triple> currentAnswers = new ArrayList<>();
                while(l.hasNext()) {
                    String predictionLine = l.nextLine();
                    if(!predictionLine.equals("")) {
                        currentAnswers.add(new Triple(predictionLine.split("\t")[0], 1));
                    }
                    else break;
                }
                int rank = 0;
                /*
                 * 分了两种方式计算排名
                 * 1.目前采用的是正常的计算，计算实际排名
                 * 2.另一种方法是采取过滤的方式计算，不将xxxxx考虑进排名当中
                 * filterMap,是所有数据集，train、validation、test之间会有重复
                 * filterSet，获取与test的谓语相同的数据集，一般test中的数据的谓语和前面train、validation都是相同的，因此获取为整个数据集的数据
                 */
                int filterCount = 0;
                Collection<Triple> filterSet = filterMap.get(testTriple.pred);
                for (Triple currentAnswer : currentAnswers) {
                    if(currentAnswer.equals(testTriple)) {
                        rank = currentAnswers.indexOf(currentAnswer) - filterCount + 1;
                        break;
                    }
                    if(filterSet.contains(currentAnswer))
                        filterCount++;
                }
                /*
                 * 统计排名
                 * 分别统计每个谓语在预测上的排名情况
                 * rankMap是包含头实体和尾实体的预测，headMap只包含头实体的，tailMap只包含尾实体的预测
                 */
                rankMap.put(testTriple.pred, rank);
                if(headQuery)
                    headMap.put(testTriple.pred, rank);
                else
                    tailMap.put(testTriple.pred, rank);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        /*
         * 记录结果
         */
        reportResults();
    }

    static private double hitsAt(List<Integer> ranks, int n) {
        int sum = 0;
        for (Integer rank : ranks) {
            if(rank != 0 && rank <= n) sum++;
        }
        return sum == 0 ? 0 : (double) sum / ranks.size();
    }

    static private double mrr(List<Integer> ranks) {
        double sum = 0;
        for (Integer rank : ranks) {
            sum += rank == 0 ? 0 : (double) 1 / rank;
        }
        return sum == 0 ? 0 : sum / ranks.size();
    }

    class QueryCreator extends Thread {
        int id;
        WriterTask predictionWriter;
        WriterTask verificationWriter;


        QueryCreator(int id,WriterTask predictionWriter,WriterTask verificationWriter) {
            super("QueryCreator-" + id);
            this.id = id;
            this.predictionWriter=predictionWriter;
            this.verificationWriter=verificationWriter;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while (!testPairs.isEmpty()) {
                    /*
                     * testpair是测试的数据，tailAnswers是获取所有潜在的答案
                     */
                    PrintWriter writer = new PrintWriter(new FileWriter(predictionFile, true));
                    PrintWriter writer1 = new PrintWriter(new FileWriter(verificationFile, true));
                    Pair testPair = testPairs.poll();
                    if (testPair != null) {
                        /*
                         * PRIOR_FILTERING = true
                         * 只添加已经存在测试集中的答案
                         * 1.不存在测试集（newTriple）的答案去掉
                         * 2.在train与validation的答案也去掉
                         *                  * filterMap,是所有数据集，train、validation、test之间会有重复
                         *                  * filterSet，获取与test的谓语相同的数据集，一般test中的数据的谓语和前面train、validation都是相同的，因此获取为整个数据集的数据
                         */
                        Collection<Pair> tailAnswers = Settings.PRIOR_FILTERING ?
                                filter(subIndex.get(testPair.subId), testPair) : subIndex.get(testPair.subId);
                        predictionContentQueue.put(createQueryAnswers("Tail Query: ", testPair, tailAnswers));
                        Collection<Pair> headAnswers = Settings.PRIOR_FILTERING ?
                                filter(objIndex.get(testPair.objId), testPair) : objIndex.get(testPair.objId);
                        predictionContentQueue.put(createQueryAnswers("Head Query: ", testPair, headAnswers));
                        while(!predictionWriter.contentQueue.isEmpty()) {
                            String line = predictionWriter.contentQueue.poll();
                            if(line != null)
                            {
                                writer.print(line);
                                writer.flush();
                            }
                        }
                    }
                }
                tx.success();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private String createQueryAnswers(String header, Pair testPair, Collection<Pair> answers) throws IOException {
            String content = header + testPair.toQueryString(graph) + "\n";
            List<Pair> rankedAnswers = new ArrayList<>();
            if(!answers.isEmpty()) {
                /*
                 * answers是可能的答案，对答案内部对应的规则进行排名，对不同答案进行排序
                 */
                rankedAnswers = rankCandidates(answers, candidates);
                /*
                 * 把前10个答案写进contents当中
                 */
                for (Pair answer : rankedAnswers.subList(0, Math.min(rankedAnswers.size(), Settings.TOP_K))) {
                    content += answer.toQueryString(graph) + "\t" + f.format(answer.scores[0]) + "\n";
                }
            }
            /*
             * 验证是否有正确答案,并将验证的结果写入verification.txt当中
             */
            populateVerification(header, testPair, rankedAnswers);
            content += "\n";
            /*
             * 返回所有答案
             */
            return content;
        }

        private void populateVerification(String header, Pair testPair, List<Pair> rankedAnswers) throws IOException {
            int topAnswers = Settings.VERIFY_PREDICTION_SIZE;
            int topRules = Settings.VERIFY_RULE_SIZE;
            PrintWriter writer1 = new PrintWriter(new FileWriter(verificationFile, true));
            String verificationContent = header + testPair.toVerificationString(graph) + "\n";
            if(rankedAnswers.isEmpty()) {
                verificationContent += "\n";
                verificationContentQueue.add(verificationContent);
                while(!verificationWriter.contentQueue.isEmpty()) {
                    String line = verificationWriter.contentQueue.poll();
                    if(line != null)
                    {
                        writer1.print(line);
                        writer1.flush();
                    }
                }
                return;
            }

            int count = 1;
            for (Pair pair : rankedAnswers.subList(0, Math.min(topAnswers, rankedAnswers.size()))) {
                verificationContent += "Top Answer: " + count + "\t" + pair.toVerificationString(graph) + "\n";
                List<Rule> rules = new ArrayList<>(candidates.get(pair));
                /*
                 *
                 */
                rules.sort(IO.ruleComparatorBySC());
                for (Rule rule : rules.subList(0, Math.min(topRules, rules.size()))) {
                    verificationContent += rule + "\t" + f.format(rule.getQuality()) + "\n";
                }
                verificationContent += "\n";
                count++;
            }

            if(rankedAnswers.contains(testPair)) {
                verificationContent += "Correct Answer: " + (rankedAnswers.indexOf(testPair) + 1) +  "\t" + testPair.toVerificationString(graph) + "\n";
                List<Rule> rules = new ArrayList<>(candidates.get(testPair));
                /*
                 *
                 */
                rules.sort(IO.ruleComparatorBySC());
                for (Rule rule : rules.subList(0, Math.min(topRules, rules.size()))) {
                    verificationContent += rule + "\t" + f.format(rule.getQuality()) + "\n";
                }
                verificationContent += "\n";
            } else {
                verificationContent += "No Correct Answer\n\n";
            }
            /*
             * 写入verification.txt当中，testPair，answer1,answer2，answer3的形式
             */
            verificationContentQueue.add(verificationContent);
            while(!verificationWriter.contentQueue.isEmpty()) {
                String line = verificationWriter.contentQueue.poll();
                if(line != null)
                {
                    writer1.print(line);
                    writer1.flush();
                }
            }
        }

        private Set<Pair> filter(Collection<Pair> answers, Pair testPair) {
            Set<Pair> filtered = new HashSet<>();
            for (Pair answer : answers) {
                /*
                 * 只添加已经存在测试集中的答案或未知的答案
                 * 1.存在测试集的答案
                 * 2.答案是newTriple，不在训练集、验证集、测试集当中出现
                 *                  * filterMap,是所有数据集，train、validation、test之间会有重复
                 *                  * filterSet，获取与test的谓语相同的数据集，一般test中的数据的谓语和前面train、validation都是相同的，因此获取为整个数据集的数据
                 */
                if(!filterSet.contains(answer) || answer.equals(testPair))
                    filtered.add(answer);
            }
            return filtered;
        }
    }

    class WriterTask extends Thread {
        File file;
        BlockingQueue<String> contentQueue;

        public WriterTask(File file, BlockingQueue<String> contentQueue) {
            this.file = file;
            this.contentQueue = contentQueue;
        }

    }

    protected List<Pair> rankCandidates(Collection<Pair> answers, Multimap<Pair, Rule> ruleMap) {
        /*
         * 对答案内部的规则进行排序
         */
        for (Pair pair : answers) {
            Double[] scores = new Double[ruleMap.get(pair).size()];
            int count = 0;
            for (Rule rule : ruleMap.get(pair)) scores[count++] = rule.getQuality();
            Arrays.sort(scores, Comparator.reverseOrder());
            pair.scores = scores;
        }
        /*
         * 对不同的答案之间进行比较排序
         */
        return sortTies(answers.toArray(new Pair[0]), 0);
    }

    protected List<Pair> sortTies(Pair[] ar, int l) {
        /*
         * 按支撑答案规则分数的最高值，对answers进行排序
         */
        Arrays.sort(ar, Pair.scoresComparator(l));
        List<Pair> set = Lists.newArrayList();
        Multimap<Pair, Pair> ties = MultimapBuilder.hashKeys().hashSetValues().build();
        /*
         * 类似归并的排序
         * 得到两个集合
         * 一个集合是set，遍历得到第i条规则分数不同的集合，这部分可以直接完成排序。
         * 另一部分集合是ties，第i条规则分数相同的集合，还需要继续递归对第i+1条规则进行排序，排序完再添加到set集合当中
         */
        createTies(ar, set, ties, l);

        //A base case to avoid deep recursion introducing stack overflow
        if(l > Settings.MAX_RECURSION_DEPTH) return Arrays.asList(ar);

        List<Pair> sorted = Lists.newArrayList();
        for(Pair i : set) {
            if(ties.containsKey(i)) {
                Pair[] ar1 = ties.get(i).toArray(new Pair[0]);
                sorted.addAll(sortTies(ar1, l + 1));
            } else sorted.add(i);//从i=0开始，比较answers中第i条规则的分数，如果前后都不相同的话，那直接添加到sorted当中；否则执行递归，直到比较完为止
        }
        return sorted;
    }

    protected void createTies(Pair[] ar, List<Pair> set, Multimap<Pair, Pair> ties, int l) {
        for(int i = 0; i < ar.length; i++) {
            final int here = i;
            /*
             * 如果answers只有一个的话，不用再比较了，直接添加到set当中
             */
            if(i == ar.length - 1) set.add(ar[here]);
            /*
             * 一开始是比较所有answers当中第一条规则的分数
             */
            for(int j = i + 1; j < ar.length; j++) {
                double p = -1, q = -1;
                if(l < ar[here].scores.length) p = ar[here].scores[l];
                if(l < ar[j].scores.length) q = ar[j].scores[l];
                if(p == q && p != -1) {
                    i++;
                    /*
                     * 统计第i条规则分数相同的集合
                     * 比如(here,here),(here,j),(here,j+1)
                     * 则要对ties集合递归排序第i+1条规则，(here,here),(here,j),(here,j+1)
                     */
                    if(!ties.keySet().contains(ar[here])) ties.put(ar[here], ar[here]);
                    if(i == ar.length - 1) set.add(ar[here]);
                    ties.put(ar[here], ar[j]);
                } else {
                    /*
                     * 第i条规则分数不同的集合
                     * 一开始是比较所有answers当中第一条规则的分数
                     */
                    set.add(ar[here]);
                    break;
                }
            }
        }
    }

}
