package uk.ac.ncl.core;

import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;
import uk.ac.ncl.utils.MathUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.*;
import uk.ac.ncl.structure.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GraphOps {


    public static GraphDatabaseService createEmptyGraph(File home) {
        if(!home.exists()) home.mkdir();
        File databaseFile = new File(home, "databases");
        if(databaseFile.exists()) deleteDirectory(databaseFile);
        return loadGraph(home);
    }

    private static boolean deleteDirectory(File file) {
        File[] contents = file.listFiles();
        if(contents != null) {
            for (File content : contents) {
                deleteDirectory(content);
            }
        }
        return file.delete();
    }

    public static GraphDatabaseService loadGraph(File home) {
        GraphDatabaseService graph = new GraphDatabaseFactory()
                .newEmbeddedDatabase(new File(home, "databases/graph.db"));
        Runtime.getRuntime().addShutdownHook(new Thread(graph::shutdown));
        return graph;
    }

    public static Set<Relationship> getRelationshipsAPI(GraphDatabaseService graph, String relationshipName) {
        Set<Relationship> relationships = new HashSet<>();
        try(Transaction tx = graph.beginTx()) {
            for (Relationship relationship : graph.getAllRelationships()) {
//                if(relationships.size() > Settings.LEARN_GROUNDINGS) break;
                if(relationship.getType().name().equals(relationshipName)) relationships.add(relationship);
            }
            tx.success();
        }
        return relationships;
    }



    public static CountedSet<Pair> bodyGroundingCoreAPI(GraphDatabaseService graph, Rule rule
            ) {
        CountedSet<Pair> pairs = new CountedSet<>();
        Flag stop = new Flag();

        /*
         * ！！！！！！！！！一共有两次调用了bodyGroundingCoreAPI函数，分别是specialization与application
         * 其中，specialization用来为instantiatedRule查找path>1的bodygrounding，但此时它还只是not closed，只属于rule，还未算是new InstantiatedRule，不执行checkTail
         * application用来生成答案集合，执行checkTail
         * ！！！！！！！！！！
         * InstantiatedRule是在生成完bodygrounding后生成，分为HAR或者BAR
         * SimpleInsRule是在RuleApplication中，读取ruletempFile当中生成，也分为HAR或者BAR
         * SimpleInsRule与InstantiatedRule的区别,除了分数以外，还将headAnchoringId与tailAnchoringId添加进了规则的内容当中
         * type==1，是在sigmoid函数生成，但这个GPFL代码并没有使用sigmoid函数
         *     public static double sigmoid(double length, int type) {
         *         type = type == 2 ? 1 : type;
         *         double complexity = length + type;
         *         return ((1 / (1 + Math.exp(-complexity * 0.5))) - 0.5) * 2;
         *     }
         * ！！！！！！！！！！！！！！
         * checkTail的作用，只对instantiatedRule执行checktail，没有对SimpleInsRule执行
         * 因为在执行application当中，abstractRule当中读取的都是closedRule或者instantiatedRule（附带HAR或者BAR）
         */
        boolean checkTail = false;
        if(rule instanceof InstantiatedRule || rule instanceof SimpleInsRule) {
            int type = rule.getType();
            if(type == 1 || type == 2) checkTail = true;
        }
        /*
         * 执行深搜的入口
         * 获取firstatom谓语相同的关系，不管方向，正向反向都有
         */
        Set<Relationship> currentRelationships = getRelationshipsAPI(graph, rule.getBodyAtom(0).getBasePredicate());
        for (Relationship relationship : currentRelationships) {

            /*
             * 将满足的节点，按节点方向（先后）存储到currentPath的nodes变量当中,深搜拓展currentPath
             */
            LocalPath currentPath = new LocalPath(relationship, rule.getBodyAtom(0).direction);
            DFSGrounding(rule, currentPath, pairs, stop, checkTail);
        }

        return pairs;
    }

    private static void DFSGrounding(Rule rule, LocalPath path, CountedSet<Pair> pairs, Flag stop
            , boolean checkTail) {
        /*
         * 如果大于等于bodytom的总长度，则检测最后一个节点是否满足条件
         * 如果不满足条件，return
         */
        if(path.length() >= rule.length()) {
            /*
             * 在specialization当中不用检查
             * 只有在application当中才需要检查，在找grounding的时候，要限制lastatom中的tail值进行查找
             */
            if(checkTail && rule.getTailAnchoring() != path.getEndNode().getId()) return;
            /*
             * Pair存储HeadAnchoring与tail的值
             */
            pairs.add(new Pair(path.getStartNode().getId(), path.getEndNode().getId()));
        }
        else {
            /*
             * 和前面的getRelationshipApi的函数功能相同，写法上不同
             * 这里是指定了方向再获取relationship
             */
            Direction nextDirection = rule.getBodyAtom(path.length()).direction;
            RelationshipType nextType = RelationshipType.withName(rule.getBodyAtom(path.length()).predicate);
            for (Relationship relationship : path.getEndNode().getRelationships(nextDirection, nextType)) {
                /*
                 * 这条if语句说的是，nextrelationship中的节点不应该和前面已有的节点有重复，避免重复
                 * 将满足的节点，按节点方向（先后）继续存储到currentPath的nodes变量当中,深搜拓展currentPath
                 */

                if(!path.nodes.contains(relationship.getOtherNode(path.getEndNode()))) {
                    LocalPath currentPath = new LocalPath(path, relationship);
                    DFSGrounding(rule, currentPath, pairs, stop, checkTail);
                }
            }
        }
    }

    public static Traverser buildStandardTraverser(GraphDatabaseService graph, Pair pair, int randomWalkers){
        Traverser traverser;
        Node startNode = graph.getNodeById(pair.subId);
        Node endNode = graph.getNodeById(pair.objId);
        /*
         * traversalDescription:开始配置规则的标志
         * uniqueness(Uniqueness.NODE_PATH): 一个节点可以被遍历不止一次，但在同一路径中不能再次出现，就是在一个path中是uniqueness，但可以在多个path中出现这个节点
         * order(BranchingPolicy.PreorderBFS()):采用广度优先搜索
         * expand(standardRandomWalker(randomWalkers))：该方法的作用是筛选路径的数量
         * evaluator：1控制长度 2是否保留当前路径 3是否对路径的节点做下一步的遍历
         */
        traverser = graph.traversalDescription()
                .uniqueness(Uniqueness.NODE_PATH)
                .order(BranchingPolicy.PreorderBFS())
                .expand(standardRandomWalker(randomWalkers))
                .evaluator(toDepthNoTrivial(Settings.DEPTH, pair))
                .traverse(startNode, endNode);
        return traverser;
    }


    public static PathExpander standardRandomWalker(int randomWalkers) {
        return new PathExpander() {
            @Override
            public Iterable<Relationship> expand(Path path, BranchState state) {
                Set<Relationship> results = Sets.newHashSet();
                List<Relationship> candidates = Lists.newArrayList( path.endNode().getRelationships() );
                if ( candidates.size() < randomWalkers || randomWalkers == 0 ) return candidates;

                Random rand = new Random();
                for ( int i = 0; i < randomWalkers; i++ ) {
                    int choice = rand.nextInt( candidates.size() );
                    results.add( candidates.get( choice ) );
                    candidates.remove( choice );
                }

                return results;
            }

            @Override
            public PathExpander reverse() {
                return null;
            }
        };
    }

    public static  PathEvaluator toDepthNoTrivial(final int depth, Pair pair) {
        return new PathEvaluator.Adapter()
        {
            @Override
            public Evaluation evaluate(Path path, BranchState state)
            {
                /*
                 * 举例
                 * 对trainPair，198->pub->273，进行拓展生成bodyatom
                 **/
                boolean fromSource = pair.subId == path.startNode().getId();
                /*
                 * closed，它定义了两种情况是closed的
                 * bodyatom198->pub->233->pub->273
                 * bodyatom273->pub->198,path=1也是closed，当path>1就不是closed了
                 * bodyatom273->pub->193->pub->198,但是这一种在生成的时候被excluded了！！！！！！！！！！！！
                 */
                boolean closed = pathIsClosed( path, pair );
                boolean hasTargetRelation = false;
                int pathLength = path.length();
                /*
                 * 举例
                 * 对trainPair，198->pub->273，进行拓展生成bodyatom
                 * 在筛选的过程中，重点是看数字和target，而不用看方向的指向
                 * 其中include是可以生成bodyatom，exclude是不可以生成bodyatom
                 * continue是可以节点继续拓展，prune是不可以对节点继续拓展
                 */
                /*
                 * 生成的bodyatom是273<-pub<-198
                 */
                if ( path.lastRelationship() != null ) {
                    Relationship relation = path.lastRelationship();
                    hasTargetRelation = relation.getType().equals(pair.type);
                    if ( pathLength == 1
                            && relation.getStartNodeId() == pair.objId
                            && relation.getEndNodeId() == pair.subId
                            && hasTargetRelation)
                        return Evaluation.INCLUDE_AND_PRUNE;
                }
                /*
                 * 确保拓展一次后198，198不再重复拓展
                 * 确保拓展一次后273，273不再重复拓展
                 */
                if ( pathLength == 0 )
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                /*
                 * 不可以生成bodyatom198->pub->273
                 * 也不可以生成bodyatom198<-pub<-273
                 */
                if ( pathLength == 1 && hasTargetRelation && closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 * 可以生成bodyatom198->pub->233->pub->273，但不能对这些节点继续拓展
                 * 不能生成bodyatom273->pub->193->pub->198！！！！！！！（被exclude了），也不能对这些节点继续拓展
                 */
                if ( closed && fromSource )
                    return Evaluation.INCLUDE_AND_PRUNE;
                else if ( closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 *不能是首尾相同
                 * 198->pub->123->pub->198
                 */
                if (selfloop(path))
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 * 其他情况下，只判断长度是否符合
                 * 198->pub->133
                 * 198<-advisedby<-251
                 * 198->pub->128<-advisedby<-251
                 */
                return Evaluation.of( pathLength <= depth, pathLength < depth );
            }
        };
    }

    private static boolean pathIsClosed(Path path, Pair pair) {
        boolean fromSource = path.startNode().getId() == pair.subId;
        if ( fromSource )
            return path.endNode().getId() == pair.objId;
        else
            return path.endNode().getId() == pair.subId;
    }

    private static boolean selfloop(Path path) {
        return path.startNode().equals( path.endNode() ) && path.length() != 0;
    }

    static class Counter {
        int count = 0;
        public void tick() {
            count++;
        }
    }

    static class Flag {
        boolean flag;
        public Flag() {
            flag = false;
        }
    }

    public static String readNeo4jProperty(Node n) {
        Object o = n.getProperty(Settings.NEO4J_IDENTIFIER);
        if(!(o instanceof String))
            return String.valueOf(o);
        else
            return (String) o;
    }

    public static void removeRelationships(Set<Pair> pairs, GraphDatabaseService graph) {
        int count = 0;
        try(Transaction tx = graph.beginTx()) {
            for (Pair pair : pairs) {
                Node s = graph.getNodeById(pair.subId);
                Node e = graph.getNodeById(pair.objId);
                for (Relationship r : s.getRelationships(Direction.OUTGOING, RelationshipType.withName(Settings.TARGET))) {
                    if(r.getOtherNode(s).equals(e)) {
                        r.delete();
                        count++;
                    }
                }
            }
            tx.success();
        }
//        Logger.println("# Removed validation and test relationships: " + count);
    }

    public static void addRelationships(Set<Pair> pairs, GraphDatabaseService graph) {
        int count = 0;
        try(Transaction tx = graph.beginTx()) {
            for (Pair pair : pairs) {
                Node s = graph.getNodeById(pair.subId);
                Node e = graph.getNodeById(pair.objId);
                s.createRelationshipTo(e, RelationshipType.withName(Settings.TARGET));
                count++;
            }
            tx.success();
        }
//        Logger.println("# Added validation and test relationships back: " + count);
    }
}
