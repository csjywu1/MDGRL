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
         * ������������������һ�������ε�����bodyGroundingCoreAPI�������ֱ���specialization��application
         * ���У�specialization����ΪinstantiatedRule����path>1��bodygrounding������ʱ����ֻ��not closed��ֻ����rule����δ����new InstantiatedRule����ִ��checkTail
         * application�������ɴ𰸼��ϣ�ִ��checkTail
         * ��������������������
         * InstantiatedRule����������bodygrounding�����ɣ���ΪHAR����BAR
         * SimpleInsRule����RuleApplication�У���ȡruletempFile�������ɣ�Ҳ��ΪHAR����BAR
         * SimpleInsRule��InstantiatedRule������,���˷������⣬����headAnchoringId��tailAnchoringId��ӽ��˹�������ݵ���
         * type==1������sigmoid�������ɣ������GPFL���벢û��ʹ��sigmoid����
         *     public static double sigmoid(double length, int type) {
         *         type = type == 2 ? 1 : type;
         *         double complexity = length + type;
         *         return ((1 / (1 + Math.exp(-complexity * 0.5))) - 0.5) * 2;
         *     }
         * ����������������������������
         * checkTail�����ã�ֻ��instantiatedRuleִ��checktail��û�ж�SimpleInsRuleִ��
         * ��Ϊ��ִ��application���У�abstractRule���ж�ȡ�Ķ���closedRule����instantiatedRule������HAR����BAR��
         */
        boolean checkTail = false;
        if(rule instanceof InstantiatedRule || rule instanceof SimpleInsRule) {
            int type = rule.getType();
            if(type == 1 || type == 2) checkTail = true;
        }
        /*
         * ִ�����ѵ����
         * ��ȡfirstatomν����ͬ�Ĺ�ϵ�����ܷ�����������
         */
        Set<Relationship> currentRelationships = getRelationshipsAPI(graph, rule.getBodyAtom(0).getBasePredicate());
        for (Relationship relationship : currentRelationships) {

            /*
             * ������Ľڵ㣬���ڵ㷽���Ⱥ󣩴洢��currentPath��nodes��������,������չcurrentPath
             */
            LocalPath currentPath = new LocalPath(relationship, rule.getBodyAtom(0).direction);
            DFSGrounding(rule, currentPath, pairs, stop, checkTail);
        }

        return pairs;
    }

    private static void DFSGrounding(Rule rule, LocalPath path, CountedSet<Pair> pairs, Flag stop
            , boolean checkTail) {
        /*
         * ������ڵ���bodytom���ܳ��ȣ��������һ���ڵ��Ƿ���������
         * ���������������return
         */
        if(path.length() >= rule.length()) {
            /*
             * ��specialization���в��ü��
             * ֻ����application���в���Ҫ��飬����grounding��ʱ��Ҫ����lastatom�е�tailֵ���в���
             */
            if(checkTail && rule.getTailAnchoring() != path.getEndNode().getId()) return;
            /*
             * Pair�洢HeadAnchoring��tail��ֵ
             */
            pairs.add(new Pair(path.getStartNode().getId(), path.getEndNode().getId()));
        }
        else {
            /*
             * ��ǰ���getRelationshipApi�ĺ���������ͬ��д���ϲ�ͬ
             * ������ָ���˷����ٻ�ȡrelationship
             */
            Direction nextDirection = rule.getBodyAtom(path.length()).direction;
            RelationshipType nextType = RelationshipType.withName(rule.getBodyAtom(path.length()).predicate);
            for (Relationship relationship : path.getEndNode().getRelationships(nextDirection, nextType)) {
                /*
                 * ����if���˵���ǣ�nextrelationship�еĽڵ㲻Ӧ�ú�ǰ�����еĽڵ����ظ��������ظ�
                 * ������Ľڵ㣬���ڵ㷽���Ⱥ󣩼����洢��currentPath��nodes��������,������չcurrentPath
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
         * traversalDescription:��ʼ���ù���ı�־
         * uniqueness(Uniqueness.NODE_PATH): һ���ڵ���Ա�������ֹһ�Σ�����ͬһ·���в����ٴγ��֣�������һ��path����uniqueness���������ڶ��path�г�������ڵ�
         * order(BranchingPolicy.PreorderBFS()):���ù����������
         * expand(standardRandomWalker(randomWalkers))���÷�����������ɸѡ·��������
         * evaluator��1���Ƴ��� 2�Ƿ�����ǰ·�� 3�Ƿ��·���Ľڵ�����һ���ı���
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
                 * ����
                 * ��trainPair��198->pub->273��������չ����bodyatom
                 **/
                boolean fromSource = pair.subId == path.startNode().getId();
                /*
                 * closed�������������������closed��
                 * bodyatom198->pub->233->pub->273
                 * bodyatom273->pub->198,path=1Ҳ��closed����path>1�Ͳ���closed��
                 * bodyatom273->pub->193->pub->198,������һ�������ɵ�ʱ��excluded�ˣ�����������������������
                 */
                boolean closed = pathIsClosed( path, pair );
                boolean hasTargetRelation = false;
                int pathLength = path.length();
                /*
                 * ����
                 * ��trainPair��198->pub->273��������չ����bodyatom
                 * ��ɸѡ�Ĺ����У��ص��ǿ����ֺ�target�������ÿ������ָ��
                 * ����include�ǿ�������bodyatom��exclude�ǲ���������bodyatom
                 * continue�ǿ��Խڵ������չ��prune�ǲ����ԶԽڵ������չ
                 */
                /*
                 * ���ɵ�bodyatom��273<-pub<-198
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
                 * ȷ����չһ�κ�198��198�����ظ���չ
                 * ȷ����չһ�κ�273��273�����ظ���չ
                 */
                if ( pathLength == 0 )
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                /*
                 * ����������bodyatom198->pub->273
                 * Ҳ����������bodyatom198<-pub<-273
                 */
                if ( pathLength == 1 && hasTargetRelation && closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 * ��������bodyatom198->pub->233->pub->273�������ܶ���Щ�ڵ������չ
                 * ��������bodyatom273->pub->193->pub->198������������������exclude�ˣ���Ҳ���ܶ���Щ�ڵ������չ
                 */
                if ( closed && fromSource )
                    return Evaluation.INCLUDE_AND_PRUNE;
                else if ( closed )
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 *��������β��ͬ
                 * 198->pub->123->pub->198
                 */
                if (selfloop(path))
                    return Evaluation.EXCLUDE_AND_PRUNE;
                /*
                 * ��������£�ֻ�жϳ����Ƿ����
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
