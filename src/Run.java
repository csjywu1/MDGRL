package uk.ac.ncl;

import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.Evaluator;
import uk.ac.ncl.model.GPFL;
import uk.ac.ncl.model.GenSpec;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.validations.Ensemble;
import uk.ac.ncl.validations.ValidRuleEvalEfficiency;
import uk.ac.ncl.validations.ValidRuleQuality;
import org.apache.commons.cli.*;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

public class Run {
    public static void main(String[] args1) {
        /*
         * 划分数据集，train，validation，test
         */

//        String[] args1={"-c","Foo/config.json","-sg"};


        Options options = new Options();

        options.addOption(Option.builder("c").longOpt("config").hasArg().argName("FILE")
                .desc("Specify the location of the GPFL configuration file.").build());


        options.addOption(Option.builder("r").longOpt("run")
                .desc("Learn, apply and evaluate rules for link prediction.").build());

        String header = "GPFL is a probabilistic rule learner optimized to learn instantiated first-order logic rules from knowledge graphs. " +
                "For more information, please refer to https://github.com/irokin/GPFL";
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = (new DefaultParser()).parse(options, args1);

            if(cmd.hasOption("c")) {
                File config = new File(cmd.getOptionValue("c"));

                    if (cmd.hasOption("r")) {
                        GPFL system = new GPFL(config, "log");
                        system.run();
                        system.close();
                    }

            }
        } catch (ParseException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
