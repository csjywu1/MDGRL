package uk.ac.ncl;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import uk.ac.ncl.core.Engine;

import java.util.Arrays;

/**
 * @author wu
 * @school jnu
 * @date 2023-02-08-10:03
 **/
public class Caculate {

    public double calc_sum(double[] entity1, RelationshipType[] rel, Direction[] der, double[] entity) {
//        double[] bodyentity=new double[100];
        double[] bodyrelation=new double[100];


        for(int i=rel.length;i>0;i--){//只取第一个relation与entity相减，后面的直接减去或者加上relation
            Direction b=der[i-1];

            int c= Engine.relation2id.get(rel[i-1].name());
            bodyrelation= Arrays.copyOf(Engine.relation_vec[c], Engine.relation_vec[c].length);

            if(b==Direction.OUTGOING){//entity-direction
                for (int k = 0; k < Engine.vector_len; k++) {
                    entity1[k]=entity1[k]-bodyrelation[k];
                }
            }
            else{//entity+direction
                for (int k = 0; k < Engine.vector_len; k++) {
                    entity1[k]=entity1[k]+bodyrelation[k];
                }
            }
        }

        double sum=0.0;
        //与head的值相减,绝对值，平均值还是取平方
        for (int i = 0; i < Engine.vector_len; i++) {
            sum=sum+Math.abs(entity1[i]-entity[i]);
//            sum=sum+ Utils.sqr(entity1[i]-entity[i]);
        }
        return sum;
    }

    public static double calc_sum(double[] e1, double[] e2, double[] rel) {
        double sum = 0;
        double k;
        for (int i = 0; i < Engine.vector_len; i++) {
            k=e1[i]+rel[i]-e2[i];
            sum+=k*k;
//            sum=sum+ Utils.sqr(e2[i] - e1[i] - rel[i]);
        }
        return Math.sqrt(sum);
    }

    public double[] rel(RelationshipType[] rel, Direction[] der) {
        double[] relation=new double[100];
        double[] relation1=new double[100];


        for(int i=0;i<rel.length;i++){//只取第一个relation与entity相减，后面的直接减去或者加上relation
            Direction b=der[i];

            int c= Engine.relation2id.get(rel[i].name());
            relation1= Arrays.copyOf(Engine.relation_vec[c], Engine.relation_vec[c].length);

            if(b==Direction.OUTGOING){//entity-direction
                for (int k = 0; k < Engine.vector_len; k++) {
                    relation[k]=relation[k]+relation1[k];
                }
            }
            else{//entity+direction
                for (int k = 0; k < Engine.vector_len; k++) {
                    relation[k]=relation[k]-relation1[k];
                }
            }
        }

        return relation;
    }
}
