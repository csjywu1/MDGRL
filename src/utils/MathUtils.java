package uk.ac.ncl.utils;
import java.util.List;
import java.util.function.Supplier;

public class MathUtils {

    public static double log(double x) {
        return Math.log( x );
    }



    public static double listMean(List<Double> list) {
        double sum = 0;
        if(list.isEmpty()) return 0;
        for (double num : list) {
            sum += num;
        }
        return sum / list.size();
    }

    public static double listSum(List<Double> list) {
        double sum = 0;
        for (Double i : list) {
            sum += i;
        }
        return sum;
    }




    public static double sigmoid(double length, int type) {
        type = type == 2 ? 1 : type;
        double complexity = length + type;
        return ((1 / (1 + Math.exp(-complexity * 0.5))) - 0.5) * 2;
    }

    public static double sigmoid(double length) {
        return ((1 / (1 + Math.exp(-length * 0.5))) - 0.5) * 2;
    }
}
