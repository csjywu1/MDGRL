package uk.ac.ncl;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.Run;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.Context;
import uk.ac.ncl.structure.Pair;
import uk.ac.ncl.structure.Triple;
import uk.ac.ncl.utils.GraphBuilder;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;

/*
 * @author wu
 * @school gdou
 **/
public class StaticAnalysis {
    public static void main(String[] args) throws IOException {
        /*
         * 写入的文件夹
         */

        PrintWriter writer = new PrintWriter(new FileWriter("UWCSE StaticAnalysis.txt"));

        DecimalFormat df = new DecimalFormat("######0.00");

        /*
         * 统计数据1
         * 统计head对应tail的情况
         * 用于分析HeadQuery
         */

        File dataset = new File("data/FB15K.txt");
        Set<GraphBuilder.localTriple> AllTriples = new HashSet<>(readTriples(dataset));
        Multimap<String, GraphBuilder.localTriple> sort_on_traget = MultimapBuilder.hashKeys().hashSetValues().build();

        /*
         * 先将数据分类
         */
        Set<String> entitys=new HashSet<>();
        Set<String> relations=new HashSet<>();

        for (GraphBuilder.localTriple triple : AllTriples) {
            String relation = triple.relation;
            sort_on_traget.put(relation, triple);
            entitys.add(triple.head);
            entitys.add(triple.tail);
            relations.add(triple.relation);

        }
        PrintWriter writer1 = new PrintWriter(new FileWriter("entity2id.txt",false));
        int i=0;
        for(String entity:entitys){
            writer1.write(entity+","+i);
            writer1.write("\n");
            i++;
        }
        writer1.flush();
        writer1.close();


        PrintWriter writer2 = new PrintWriter(new FileWriter("relation2id.txt",false));
        int j=0;
        for(String relation:relations){
            writer2.write(relation+","+j);
            writer2.write("\n");
            j++;
        }
        writer2.flush();
        writer2.close();
        /*
         * 先将数据分类
         */
        for (GraphBuilder.localTriple triple : AllTriples) {
            String relation = triple.relation;
            sort_on_traget.put(relation, triple);

        }
        /*
         * 再对同一类的数据进行统计
         */
        for (String relation : sort_on_traget.keySet()) {
            Collection<GraphBuilder.localTriple> head_tailsss = sort_on_traget.get(relation);
            Map<String, List> staticmap = new HashMap<>();
            for (GraphBuilder.localTriple head_tail : head_tailsss) {
                String head = head_tail.head;
                String tail = head_tail.tail;
                if (staticmap.containsKey(head) == false) {
                    List tails = new ArrayList();
                    tails.add(tail);
                    staticmap.put(head, tails);
                } else {
                    List tails = staticmap.get(head);
                    tails.add(tail);
                    staticmap.put(head, tails);
                }
            }
            /*
             * 1.统计每个target中一个head对应多少个tail的数量
             * */
            //1
            int headsize = staticmap.values().size();
            Double onehead_manytail = 0.0;
            Set tails = new HashSet();
            for (List list : staticmap.values()) {
                onehead_manytail = onehead_manytail + list.size();
                for (Object l : list) {
                    tails.add(l);
                }
            }
            onehead_manytail = onehead_manytail / staticmap.values().size();
            writer.println("target: " + relation);

            /*
             * 统计数据2
             * 统计tail对应head的数据
             * 用于分析TailQuery
             */
            head_tailsss = sort_on_traget.get(relation);
            staticmap = new HashMap<>();
            for (GraphBuilder.localTriple head_tail : head_tailsss) {
                String head = head_tail.head;
                String tail = head_tail.tail;
                if (staticmap.containsKey(tail) == false) {
                    List heads = new ArrayList();
                    heads.add(head);
                    staticmap.put(tail, heads);
                } else {
                    List heads = staticmap.get(tail);
                    heads.add(head);
                    staticmap.put(tail, heads);
                }
            }
            /*
             * 1.统计每个target中一个tail对应多少个head的数量
             * 2.计算Head与Tail的比例统计head节点的数量，tail节点的数量.计算Head与Tail的比例
             */
            //1
            Double onetail_manyhead = 0.0;
            Set heads = new HashSet();
            for (List list : staticmap.values()) {
                onetail_manyhead = onetail_manyhead + list.size();
                for (Object l : list) {
                    heads.add(l);
                }
            }
            onetail_manyhead = onetail_manyhead / staticmap.values().size();

            //2
            int tailsize = tails.size();
            /*
             * 将数值情况写入文件夹
             */
            writer.print("head entity: " + headsize + "     ");
            writer.print("tail entity: " + tailsize + "     ");
            writer.println("head / tail: " + df.format((double)headsize / tailsize) + "     ");
            writer.print("head to average tail: " + df.format(onehead_manytail) + "     ");
            writer.print("tail to average head: " + df.format(onetail_manyhead)+ "     ");

            /*
             * 对上面的统计结果求均值
             */
            writer.println("average entity ratio: " +df.format( (onehead_manytail * headsize + onetail_manyhead * tailsize) / (headsize + tailsize)));
            writer.println();
            writer.flush();
        }
        writer.close();
    }

    public static Set<GraphBuilder.localTriple> readTriples(File file) {
        Set<GraphBuilder.localTriple> triples = new HashSet<>();
        if (file.exists()) {
            try (LineIterator l = FileUtils.lineIterator(file)) {
                while (l.hasNext()) {
                    triples.add(new GraphBuilder.localTriple(l.nextLine()));
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        } else {
            System.out.println("# Notice: " + file.getName() + " does not exist.");
        }
        return triples;
    }

}
