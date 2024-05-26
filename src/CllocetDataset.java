package uk.ac.ncl;

import java.io.*;

/*
 * @author wu
 * @school gdou
 **/
public class CllocetDataset {
    public static void main(String[] args) throws IOException {
        File hxyk = new File("data/FB15K");
        File hxykhz = new File("data/FB15K.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(hxykhz));

        File[] files = hxyk.listFiles();

        for (File file : files) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            char[] chars = new char[1024];
            int len;
            while ((len = br.read(chars)) != -1){
                bw.write(chars,0,len);
            }
            br.close();
        }

        bw.close();
    }
}




