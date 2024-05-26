package uk.ac.ncl.structure;

import uk.ac.ncl.Settings;
import org.apache.commons.compress.utils.Lists;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public abstract class Rule {
    public boolean closed;
    public boolean fromSubject;
    public Atom head;
    public List<Atom> bodyAtoms = new ArrayList<>();
    public RuleStats stats = new RuleStats();
    public int type;
    public int index;

    Rule() {}

    Rule(Atom head, List<Atom> bodyAtoms) {
        this.head = head;
        this.bodyAtoms = new ArrayList<>(bodyAtoms);
        Atom lastAtom = bodyAtoms.get(bodyAtoms.size() - 1);
        closed = head.getObjectId() == lastAtom.getObjectId();
        Atom firstAtom = bodyAtoms.get( 0 );
        fromSubject = head.getSubjectId() == firstAtom.getSubjectId();
    }

    public void setStats(double support, double totalPredictions, double groundTruth) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.groundTruth = groundTruth;
        stats.compute();
    }

    public void setStats(double support, double totalPredictions, double pcaTotalPredictions, double groundTruth) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.pcaTotalPredictions = pcaTotalPredictions;
        stats.groundTruth = groundTruth;
        stats.compute();
    }

    public void setStats(double support, double totalPredictions
            , double pcaTotalPredictions, double groundTruth
            , double validTotalPredictions, double validPredictions) {
        stats.support = support;
        stats.totalPredictions = totalPredictions;
        stats.pcaTotalPredictions = pcaTotalPredictions;
        stats.groundTruth = groundTruth;
        stats.validTotalPredictions = validTotalPredictions;
        stats.validPredictions = validPredictions;
        stats.compute();
    }

    public int length() {
        return bodyAtoms.size();
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isFromSubject() {
        return fromSubject;
    }

    public Atom copyHead() {
        return new Atom( head );
    }

    public List<Atom> copyBody() {
        List<Atom> result = Lists.newArrayList();
        bodyAtoms.forEach( atom -> result.add( new Atom(atom)));
        return result;
    }

    @Override
    public String toString() {
        String str = head + " <- ";
        List<String> atoms = new ArrayList<>();
        bodyAtoms.forEach( atom -> atoms.add(atom.toString()));
        return str + String.join(", ", atoms);
    }

    @Override
    public int hashCode() {
        int hashcode = head.hashCode();
        for(Atom atom : bodyAtoms) {
            hashcode += atom.hashCode();
        }
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Rule) {
            Rule right = (Rule) obj;
            return this.toString().equals(right.toString());
        }
        return false;
    }

    public double getQuality() {
//        Settings.QUALITY_MEASURE
        //直接在这里修改使用什么指标，而不是在Settings中修改
        switch ("Jaccard") {
            case "HeadCoverage":
                return  stats.headCoverage;
            case "smoothedConf":
                return stats.smoothedConf;
            case "standardConf":
                return stats.standardConf;
            case "pcaConf":
                return stats.pcaConf;
            case "lift":
                return  stats.lift;
            case "leverage":
                return  stats.leverage;
            case "Jaccard":
                return stats.Jaccard;
            case "Conise":
                return  stats.Conise;
            case "AddedValue":
                return stats.AddedValue;
            case "Conise1":
                return  stats.Jaccard;
            case "Relative_risk":
                return stats.Relative_risk;
            default:
                return stats.apcaConf;
        }
    }

    public double getQuality(String measure) {
        switch (measure) {
            case "HeadCoverage":
                return  stats.headCoverage;
            case "smoothedConf":
                return stats.smoothedConf;
            case "standardConf":
                return stats.standardConf;
            case "pcaConf":
                return stats.pcaConf;
            case "lift":
                return  stats.lift;
            case "leverage":
                return  stats.leverage;
            case "Jaccard":
                return  stats.Jaccard;
            case "Conise1":
                return  stats.Jaccard;
            default:
                return stats.apcaConf;
        }
    }

    public double getHeadCoverage() {
        return stats.headCoverage;
    }

    public double getStandardConf() {
        return stats.standardConf;
    }

    public double getSmoothedConf() {
        return stats.smoothedConf;
    }

    public double getPcaConf() {
        return stats.pcaConf;
    }

    public double getApcaConf() {
        return stats.apcaConf;
    }

    public double getValidPrecision() {
        return stats.validPrecision;
    }

    public double getPrecision() {
        return stats.precision;
    }

    public abstract long getTailAnchoring();

    public abstract long getHeadAnchoring();

    public int getType() {
        return type;
    }

    public Atom getBodyAtom(int i) {
        assert i < bodyAtoms.size();
        return bodyAtoms.get(i);
    }

    public static class RuleStats {
        public double support = 0d;
        public double groundTruth = 0d;
        public double totalPredictions = 0d;
        public double pcaTotalPredictions = 0d;
        public double validTotalPredictions = 0d;
        public double validPredictions = 0d;

        public double standardConf;
        public double smoothedConf;
        public double pcaConf;
        public double apcaConf;
        public double headCoverage;
        public double validPrecision;
        public double precision;

        public double bingji;
        public double lift;
        public double leverage;
        public double Jaccard;
        private double Conise;
        private double Conise1;
        private double AddedValue;
        private double Relative_risk;

        @Override
        public String toString() {
            return MessageFormat.format("Support = {0}\nSC = {1}\nHC = {2}"
                    , String.valueOf(support)
                    , String.valueOf(smoothedConf)
                    , String.valueOf(headCoverage));
        }

        public void compute() {
            /*
             * 常用的3个指标，参考AMIE+论文
             * standardConf:两者都满足/所有与bodyatom形式相关的值
             * pcaConf:fromSource的时候pcaConf=1，非fromSource的时候与standardConf的值相同
             * headCoverage:两者都满足/满足头的
             * 非常用的三个指标！！！！！！！！！！！！！！！！
             * smoothedConf：在standardConf的基础上加了一个偏移的变量
             * apcaConf：如果一对一比较多，就选择使用pcaConf，否则使用smoothedConf
             * 训练集中不满足的放到验证集中完成预测
             * validPrecision：在验证集当中的standardConf
             * validTotalPredictions=！！！训练集中，限定headanchoring值以后，不满足规则的数量
             * validPredictions在代入HeadAnchoring值进validPair以后，验证集中符合规则，既满足head也满足bodyatom的的数量
             */
            /**
             * 这里的变量名support/totalPredicitons/groundTruth表示只是作者的标记，与实际有点不同
             * support表示|A∩B|
             * totalPredicitons是|B|
             * groundTruth是|A|
             * bingji=|A∪B|=|A|+|B|-|A∩B|
             * validPredictions表示验证集中的|A∩B|
             * totalPredicitons是验证集中的|B|
             */
            bingji=groundTruth+totalPredictions-support;
            double jiaoji=support;
            double A=groundTruth;
            double B=totalPredictions;
            smoothedConf = support / (totalPredictions + Settings.CONFIDENCE_OFFSET);
            standardConf = totalPredictions == 0 ? 0 : support / totalPredictions;
            pcaConf = totalPredictions == 0 ? 0 : support / pcaTotalPredictions;
            headCoverage = groundTruth == 0 ? 0 : support / (groundTruth+ Settings.CONFIDENCE_OFFSET);
            apcaConf = Settings.TARGET_FUNCTIONAL ? pcaConf : smoothedConf;
            validPrecision = validTotalPredictions == 0 ? 0 : validPredictions / validTotalPredictions;
            lift=((totalPredictions + Settings.CONFIDENCE_OFFSET)/bingji)==0?0:(jiaoji/(totalPredictions + Settings.CONFIDENCE_OFFSET))/((totalPredictions + Settings.CONFIDENCE_OFFSET)/bingji);
            leverage=bingji==0?0:((jiaoji/bingji)*(groundTruth/bingji)*(totalPredictions/bingji));
            Jaccard=(A+B-jiaoji)==0?0:(jiaoji/(A+B-jiaoji));

            Conise=bingji==0?0:(jiaoji/((A*B)/(bingji)));
            Conise1=bingji==0?0:(jiaoji/Math.sqrt(A*B)/(bingji));
            AddedValue=(B==0||A==0)?0:(jiaoji/B)+(jiaoji/A);//
//            leverage=bingji==0?0:(A/bingji)+(B/bingji)-(jiaoji/bingji);
            Relative_risk=((B-jiaoji)==0||(A-jiaoji)==0)?0:(1-jiaoji/(B-jiaoji))*(1-jiaoji/(A-jiaoji));//效果不好
        }

        public void setApcaConf(double apcaConf) {
            this.apcaConf = apcaConf;
        }

        public void setStandardConf(double standardConf) {
            this.standardConf = standardConf;
        }

        public void setSmoothedConf(double smoothedConf) {
            this.smoothedConf = smoothedConf;
        }

        public void setPcaConf(double pcaConf) {
            this.pcaConf = pcaConf;
        }

        public void setHeadCoverage(double headCoverage) {
            this.headCoverage = headCoverage;
        }

        public void setValidPrecision(double validPrecision) {
            this.validPrecision = validPrecision;
        }

        public void setPrecision(double precision) {
            this.precision = precision;
        }

    }
}
