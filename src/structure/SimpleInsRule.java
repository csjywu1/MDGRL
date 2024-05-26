package uk.ac.ncl.structure;

import uk.ac.ncl.Settings;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.ArrayList;
import java.util.List;

public class SimpleInsRule extends Rule {
    public long headAnchoringId;
    public long tailAnchoringId;
    public Template base;
    private String rep;

    public SimpleInsRule(Template base, String line) {
        this.base = base;
        this.fromSubject = base.fromSubject;
        String[] words = line.split(",");
        type = Integer.parseInt(words[0]);
        /*
         * SimpleInsRule与InstantiatedRule的区别,除了分数以外，还将headAnchoringId与tailAnchoringId添加进了规则的内容当中
         * HAR设置headAnchoringId，type是0
         * BAR设置headAnchoringId与tailAnchoringId，type是2
         * 0,30,0.13333,0.1,0.13333,0.1,1,0	 HeadAnchoring 30 type=0 剩下的内容是分数
         * 2,30,645,0.66667,0.25,0.66667,0.25,1,0 HeadAnchoring 30,tailAnchoring 645 type=2 剩下的内容是分数
         *
          */
        headAnchoringId = Long.parseLong(words[1]);
        if(type == 2) {
            /*
             * BAR在的type为2
             */
            tailAnchoringId = Long.parseLong(words[2]);
            stats.setStandardConf(Double.parseDouble(words[3]));
            stats.setSmoothedConf(Double.parseDouble(words[4]));
            stats.setPcaConf(Double.parseDouble(words[5]));
            stats.setApcaConf(Double.parseDouble(words[6]));
            stats.setHeadCoverage(Double.parseDouble(words[7]));
            stats.setValidPrecision(Double.parseDouble(words[8]));
        } else {
            /*
             * HAR的type为0
             */
            stats.setStandardConf(Double.parseDouble(words[2]));
            stats.setSmoothedConf(Double.parseDouble(words[3]));
            stats.setPcaConf(Double.parseDouble(words[4]));
            stats.setApcaConf(Double.parseDouble(words[5]));
            stats.setHeadCoverage(Double.parseDouble(words[6]));
            stats.setValidPrecision(Double.parseDouble(words[7]));
        }
    }

    public void insRuleString(GraphDatabaseService graph) {
        rep = type == 0 ? "HAR\t" : "BAR\t";
        Atom head = new Atom(base.head);
        List<Atom> bodyAtoms = new ArrayList<>();
        base.bodyAtoms.forEach( atom -> bodyAtoms.add(new Atom(atom)));
        Atom lastAtom = bodyAtoms.get(bodyAtoms.size() - 1);

        if(base.fromSubject)
            head.object = (String) graph.getNodeById(headAnchoringId).getProperty(Settings.NEO4J_IDENTIFIER);
        else
            head.subject = (String) graph.getNodeById(headAnchoringId).getProperty(Settings.NEO4J_IDENTIFIER);

        if(type == 2) {
            lastAtom.object = (String) graph.getNodeById(tailAnchoringId).getProperty(Settings.NEO4J_IDENTIFIER);
        }

        rep += head + " <- ";
        List<String> words = new ArrayList<>();
        bodyAtoms.forEach( atom -> words.add(atom.toString()));
        rep += String.join(", ", words);
    }

    @Override
    public long getTailAnchoring() {
        return tailAnchoringId;
    }

    @Override
    public long getHeadAnchoring() {
        return headAnchoringId;
    }

    @Override
    public String toString() {
        if(rep != null) return rep;
        String content = base.toString() + "\t";
        content += type == 0 ? "[" + headAnchoringId + "]" : "[" + headAnchoringId + "," + tailAnchoringId +"]";
        content += "\t" + getQuality();
        return content;
    }

    @Override
    public int hashCode() {
        return base.hashCode() + type * 15 + (int) headAnchoringId + (int) tailAnchoringId + (int) (getQuality() * 15d);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof SimpleInsRule) {
            SimpleInsRule right = (SimpleInsRule) obj;
            return right.type == type && right.headAnchoringId == headAnchoringId
                    && right.tailAnchoringId == tailAnchoringId && right.getQuality() == getQuality() && right.base.equals(base);
        }
        return false;
    }

    @Override
    public Atom getBodyAtom(int i) {
        return base.getBodyAtom(i);
    }

    @Override
    public int length() {
        return base.length();
    }

    @Override
    public boolean isClosed() {
        return base.isClosed();
    }
}
