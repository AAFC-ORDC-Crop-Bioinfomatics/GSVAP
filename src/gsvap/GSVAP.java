/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gsvap;
import java.io.*;
import java.util.*;
import rcaller.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 *
 * @author frankyou
 */
public class GSVAP {

    /**
     * @param args the command line arguments
     */
    public static String RScriptPath = "/usr/bin/Rscript";
    private static final DecimalFormat df6 = new DecimalFormat("0.000000");
    private static final DecimalFormat df3 = new DecimalFormat("0.000");
    private static final DecimalFormat df2 = new DecimalFormat("0.00");
    private static Hashtable<String, Float> ref_ids_len_hash = new Hashtable();
    private static Hashtable<String, Float> query_ids_len_hash = new Hashtable();

    
    
    public static void main(String[] args) {
        // TODO code application logic here
        if (args.length != 2) {
            System.out.println("Usage:");
            System.out.println("java -jar GSVAP.jar input_data_folder output_folder");
            System.exit(1);
        }
        
        String input_data_folder = args[0];
        String output_data_folder = args[1];

        
        // error check
        //test
        

        if (!new File(input_data_folder).exists()) {
            System.out.println("The input data folder " + input_data_folder + " does not exist!");
            System.exit(1);
        }
        
        if (!new File(output_data_folder).exists()) {
            // generate a new folder if no output folder exists
            System.out.println("The output data folder " + output_data_folder + " does not exist!");
            new File(output_data_folder).mkdir();
            System.out.println("The output data folder " + output_data_folder + " has been createdt!");
        }

        
        try {
            // the top level function to do all data analysis in this application
            new GSVAP().startPipeline(input_data_folder, output_data_folder);
        
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
    
    void startPipeline(String input_data_folder, String output_data_folder) throws Exception {
            // Objective 1: generate and parse coords result file to generate summary file 
            parseCoordsFiles(input_data_folder, output_data_folder);
            
            // Objective 2: generate parse showdiff result file to generate summary file 
            parseShowDiffFiles(input_data_folder, output_data_folder);
        
    }
    
    private void parseShowDiffFiles(String input_data_folder, String output_data_folder)throws Exception {
        System.out.println("Generate SVs file from the file *.delta");
        
        File in_files = new File(input_data_folder);
        String[] files = in_files.list();
        for (int i = 0; i < files.length; i++) {
            if (!files[i].endsWith("delta")) {
                continue;
            }
            
            String in_file = input_data_folder + File.separator + files[i];
        
            // generate coords file using mummer command show-coords which must be availabel on the file path
            String out_file = output_data_folder + File.separator + files[i] + ".showdiff.SVs.txt";
            String [] cmd_arr = {"/bin/sh" , "-c",  "show-diff -q " + in_file + ">" + out_file};
            System.out.print("command: ");
            System.out.println(Arrays.toString(cmd_arr));
            
            Process p = Runtime.getRuntime().exec(cmd_arr);
            p.waitFor();
            
            if (p.exitValue() != 0) {
                InputStream errorStream = p.getErrorStream();
                int c = 0;
                while ((c = errorStream.read()) != -1) {
                    System.out.print((char)c);
                }
            }
        
            // Parse coords file
            parseDiffsFile(out_file, output_data_folder);
        }
        
    }

    private void parseCoordsFiles(String input_data_folder, String output_data_folder)throws Exception {
        System.out.println("Generate coords file using file *.delta");
        
        File in_files = new File(input_data_folder);
        String[] files = in_files.list();
        for (int i = 0; i < files.length; i++) {
            if (!files[i].endsWith("delta")) {
                continue;
            }
            
            String in_file = input_data_folder + File.separator + files[i];
        
            // generate coords file using mummer command show-coords which must be availabel on the file path
            String out_file = output_data_folder + File.separator + files[i] + ".coords";
            String [] cmd_arr = {"/bin/sh" , "-c",  "show-coords -l " + in_file + ">" + out_file};
            System.out.print("command: ");
            System.out.println(Arrays.toString(cmd_arr));
            
            Process p = Runtime.getRuntime().exec(cmd_arr);
            p.waitFor();
            
            if (p.exitValue() != 0) {
                InputStream errorStream = p.getErrorStream();
                int c = 0;
                while ((c = errorStream.read()) != -1) {
                    System.out.print((char)c);
                }
            }
        
            // Parse coords file
            parseCoordsFile(out_file, output_data_folder);
        }
    }

    private void parseCoordsFile(String coords_out_file, String output_data_folder)throws Exception {
        System.out.println("Parse coords file: " + coords_out_file);

        String summary_file = coords_out_file + ".summary.txt";
        BufferedWriter w = new BufferedWriter(new FileWriter(summary_file));
        w.write("ref_match_len_kb\tquery_match_len_kb\tref_len_kb\tquery_len_kb\tidentity\tref_id\tquery_id\n");

        
        BufferedReader r = new BufferedReader(new FileReader(coords_out_file));
        String line = null;
        int ncols = 0;
        StringBuffer buf = new StringBuffer();
        int line_count = 0;
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count <= 5)   // remove the first 5 header lines
                continue;
            
            line = line.trim();
            // remove the header lines
            
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            int ref_s = new Integer(cols[0]).intValue();                
            int ref_e = new Integer(cols[1]).intValue();                
            int q_s = new Integer(cols[3]).intValue();                
            int q_e = new Integer(cols[4]).intValue();                
            int ref_match_len = new Integer(cols[6]).intValue();                
            int q_match_len = new Integer(cols[7]).intValue();                
            float identity = new Float(cols[9]).floatValue();

            int ref_len = new Integer(cols[11]).intValue();                
            int q_len = new Integer(cols[12]).intValue();                

            String ref_id = cols[14];
            String q_id = cols[15];
            if (!ref_ids_len_hash.containsKey(ref_id)) {
                ref_ids_len_hash.put(ref_id, new Float(ref_len));
            }

            if (!query_ids_len_hash.containsKey(q_id)) {
                query_ids_len_hash.put(q_id, new Float(q_len));
            }

            // convert bp to kb via dividing by 1000
            w.write( df3.format(ref_match_len/1000.0) + "\t" + df3.format(q_match_len/1000.0) + "\t" + 
                    df3.format(ref_len/1000.0) + "\t" + df3.format(q_len/1000.0) + "\t" +
                    df2.format(identity) + "\t" + ref_id + "\t" + q_id + "\n");
        }
        r.close();
        w.close();
        
        // summary the results from show-coords command
        coordStatiscs(summary_file);
        
       
    }
    
    private void coordStatiscs(String summary_file) throws Exception {
        // RCaller is a third-party Java library that must be added to the project.
        // this library (RCaller-2.5.jar has been placed to the folder "lib"
        // You need to add it to the project by (1) right click on the project name;
        // (2) Click on "Properties"; (3) choose "Libraries" (4) click on "Add JAR/Folder"
        // (5) choose RCaller jar file; and finally (6) click Ok button
        
        System.out.println("Calculate coords statistics...");
        
        String ref_histogram_file = summary_file + ".ref_histogram.tiff";
        String query_histogram_file = summary_file + ".query_histogram.tiff";
        String identity_histogram_file = summary_file + ".identity_histogram.tiff";
        
        RCaller caller = new RCaller();
        RCode  code = new RCode();
        caller.setRscriptExecutable(RScriptPath);
        caller.cleanRCode();
        
        // draw histograms of match lengths (ref and query) and match identity
        code.R_require("ggplot2");
        code.R_require("dplyr");
        code.addRCode("data<-read.table(\"" + summary_file + "\", sep=\"\\t\", header=T)");
        code.addRCode("tiff(\"" + ref_histogram_file + "\", units=\"in\", width=5, height=5, res=300)");
        code.addRCode("ggplot(data, aes(x=ref_match_len_kb)) +  geom_histogram(color=\"black\", fill=\"white\")");
        code.addRCode("dev.off()");
        
        code.addRCode("tiff(\"" + query_histogram_file + "\", units=\"in\", width=5, height=5, res=300)");
        code.addRCode("ggplot(data, aes(x=query_match_len_kb)) +  geom_histogram(color=\"black\", fill=\"white\")");
        code.addRCode("dev.off()");

        code.addRCode("tiff(\"" + identity_histogram_file + "\", units=\"in\", width=5, height=5, res=300)");
//        code.addRCode("ggplot(data, aes(x=identity)) +  geom_histogram(color=\"black\", fill=\"white\") + xlab(\"Identity (%)\"");
        code.addRCode("ggplot(data, aes(x=identity)) +  geom_histogram(color=\"black\", fill=\"white\")");
        code.addRCode("dev.off()");
        
        //calculate summary statistics
        
        String ref_sum_file = summary_file + ".ref.sum.txt";
        String query_sum_file = summary_file + ".query.sum.txt";
        
//        code.addRCode("ref_sum <-aggregate(data$ref_match_len_kb, by=list(Category=data$ref_id), FUN=sum)");
//        code.addRCode("query_sum <-aggregate(data$query_match_len_kb, by=list(Category=data$query_id), FUN=sum)");
        code.addRCode("ref_sum <-data %>% "); 
        code.addRCode("group_by(ref_id) %>% "); 
        code.addRCode("summarise(across(c(ref_match_len_kb, query_match_len_kb, ref_len_kb, query_len_kb), list(mean = mean, sum = sum, min=min, max=max)))");

        code.addRCode("query_sum <-data %>% "); 
        code.addRCode("group_by(query_id) %>% "); 
        code.addRCode("summarise(across(c(ref_match_len_kb, query_match_len_kb, ref_len_kb, query_len_kb), list(mean = mean, sum = sum, min=min, max=max)))");

        code.addRCode("write.table(ref_sum, file=\"" + ref_sum_file +"\", quote=F, sep=\"\\t\",row.names=FALSE)");
        code.addRCode("write.table(query_sum, file=\"" + query_sum_file +"\", quote=F, sep=\"\\t\",row.names=FALSE)");
             
        //couts
        String ref_count_file = summary_file + ".ref.count.txt";
        String query_count_file = summary_file + ".query.count.txt";
        code.addRCode("ref_count <-data %>% group_by(ref_id) %>% tally()");
        code.addRCode("query_count <-data %>% group_by(query_id) %>% tally()");
        
        code.addRCode("write.table(ref_count, file=\"" + ref_count_file +"\", quote=F, sep=\"\\t\", row.names=FALSE)");
        code.addRCode("write.table(query_count, file=\"" + query_count_file +"\", quote=F, sep=\"\\t\", row.names=FALSE)");
        
        caller.setRCode(code);
        caller.runOnly();

        // reorganize the results files into one:(1) put sum and counts from two different output files together (2) calculate overall total
        String ref_stats_file = summary_file + ".ref.stats.txt";
        String query_stats_file = summary_file + ".query.stats.txt";
        
        boolean is_ref = true;
        mergeCoordsStatsFiles(ref_sum_file, ref_count_file, ref_stats_file, is_ref);
        is_ref = false;
        mergeCoordsStatsFiles(query_sum_file, query_count_file, query_stats_file, is_ref);
        
        // delete two intermediate files
        new File(query_sum_file).delete();
        new File(query_count_file).delete();
        new File(ref_sum_file).delete();
        new File(ref_count_file).delete();
        
    }

    private void mergeCoordsStatsFiles(String sum_file, String count_file,
            String stats_file, boolean is_ref) throws Exception {
        
        BufferedWriter w = new BufferedWriter(new FileWriter(stats_file));
        
        // read count file
        Hashtable<String, Integer> counts_hash = new Hashtable();
        BufferedReader r = new BufferedReader(new FileReader(count_file));
        String line = null;
        int ncols = 0;
        StringBuffer buf = new StringBuffer();
        int line_count = 0;
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count <= 2) {       // print header
                continue;
            }
            
            line = line.trim();
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            String id = cols[0];
            int count = new Integer(cols[1]).intValue();                
            counts_hash.put(id, count);
        }
        r.close();
        
        // read sum file
        r = new BufferedReader(new FileReader(sum_file));
        line = null;
        buf = new StringBuffer();
        line_count = 0;
        float match_total = 0.0f;
                
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count == 1) {       // print header
                w.write(line + "\t");
                
                if (is_ref) {
                    w.write("ref_match_count\tref_match_pct (%)\n");
                } else {
                    w.write("query_match_count\tquery_match_pct (%)\n");
                }
                
                continue;
            } else if (line_count == 2) { // skip this line
                continue;
            }
            
            line = line.trim();
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            String id = cols[0];
            
            w.write(id + "\t");
            for (int i = 1; i < cols.length; i++) {
                w.write(df3.format(new Float(cols[i])) + "\t");
            }
            w.write(counts_hash.get(id) + "\t");
            
            float pct = 0;
            if (is_ref) {
                float ref_match_len_kb_sum = new Float(cols[2]).floatValue();
                float ref_len_kb = new Float(cols[9]).floatValue();
                pct = ref_match_len_kb_sum/ref_len_kb * 100.0f;
                
                match_total += new Float(cols[2]).floatValue(); 
            } else {
                float query_match_len_kb_sum = new Float(cols[6]).floatValue();
                float query_len_kb = new Float(cols[13]).floatValue();
                pct = query_match_len_kb_sum/query_len_kb * 100.0f;
                
                match_total += new Float(cols[6]).floatValue(); 
            }
            w.write(df2.format(pct) + "\n");
        }
        
        // calculate the total length of reference sequence
        float total = 0;
        if (is_ref) {
            Enumeration<String> keys = ref_ids_len_hash.keys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                total += ref_ids_len_hash.get(key);
            }
        } else {
            Enumeration<String> keys = query_ids_len_hash.keys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                total += query_ids_len_hash.get(key);
            }
            
        }
        
        float total_match_pct = match_total/total*100;
        //System.out.println(match_total + "\t" +  total + "\t" + total_match_pct);
        
        if (is_ref) {
            w.write("Total" + "\t \t");
            w.write(df3.format(match_total) + "\t \t \t \t \t \t \t \t \t \t \t \t \t \t \t \t");
            w.write(df6.format(total_match_pct) + "\n");
        } else {
            w.write("Total\t \t \t \t \t \t");
            w.write(df3.format(match_total) + "\t \t \t \t \t \t \t \t \t \t \t \t");
            w.write(df6.format(total_match_pct) + "\n");
        }
        
        r.close();
        w.close();        
                
    }
        
    
    private void parseDiffsFile(String sv_out_file, String output_data_folder)throws Exception {
        System.out.println("Parse diffs (SVs) file: " + sv_out_file);


        // reorganize the output file from show-diff: change bp to kb via dividing by 1000
        // and also add a readable header for summary in R
        String summary_file = sv_out_file + ".SVs.summary.txt";
        BufferedWriter w = new BufferedWriter(new FileWriter(summary_file));
        w.write("query_id\ttype\tstart_kb\tend_kb\tquery_match_len_kb\n");
        
        BufferedReader r = new BufferedReader(new FileReader(sv_out_file));
        String line = null;
        int ncols = 0;
        StringBuffer buf = new StringBuffer();
        int line_count = 0;
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count <= 4)   // remove the first 5 header lines
                continue;
            
            line = line.trim();
            // remove the header lines
            
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            String id = cols[0];
            String type = cols[1];
            int start = new Integer(cols[2]).intValue();                
            int end = new Integer(cols[3]).intValue();                
            int len = new Integer(cols[4]).intValue();                

            
            // convert bp to kb via dividing by 1000
            w.write(id + "\t" +  type + "\t" + df3.format(start/1000.0) + "\t" + df3.format(end/1000.0) + "\t" + 
                    df3.format(len/1000.0) +  "\n");
        }
        r.close();
        w.close();
        
        // summary the results from show-diff command
        SVsStatiscs(summary_file);
        
    }
    
    private void SVsStatiscs(String summary_file) throws Exception {
        System.out.println("Calculate SVs statistics...");
        
        
        RCaller caller = new RCaller();
        RCode  code = new RCode();
        caller.setRscriptExecutable(RScriptPath);
        caller.cleanRCode();
        
        // draw histograms of match lengths (ref and query) and match identity
        code.R_require("ggplot2");
        code.R_require("dplyr");
        code.addRCode("data<-read.table(\"" + summary_file + "\", sep=\"\\t\", header=T)");
        
        //calculate summary statistics
        String query_sum_file = summary_file + ".SVs.query.sum.txt";
        
        code.addRCode("query_sum <-data %>% "); 
        code.addRCode("group_by(type) %>% "); 
        code.addRCode("summarise(across(c(query_match_len_kb), list(mean = mean, sum = sum, min=min, max=max)))");
        code.addRCode("write.table(query_sum, file=\"" + query_sum_file +"\", quote=F, sep=\"\\t\",row.names=FALSE)");
             
        //couts
        String query_count_file = summary_file + ".SVs.query.count.txt";
        code.addRCode("query_count <-data %>% group_by(type) %>% tally()");
        
        code.addRCode("write.table(query_count, file=\"" + query_count_file +"\", quote=F, sep=\"\\t\", row.names=FALSE)");
        
        caller.setRCode(code);
        caller.runOnly();

        // reorganize the results files into one:(1) put sum and counts from two different output files together (2) calculate overall total
        // save into this file
        String query_stats_file = summary_file + ".query.stats.txt";
        
        boolean is_ref = false;
        mergeSVsStatsFiles(is_ref, query_sum_file, query_count_file, query_stats_file);
        
        // delete two intermediate files
        new File(query_sum_file).delete();
        new File(query_count_file).delete();
        
        //draw SV count bar chart
        drawSVBarChart(query_stats_file);
    }

    private void drawSVBarChart(String query_stats_file) throws Exception {
        System.out.println("Draw barchart...");
        
        RCaller caller = new RCaller();
        RCode  code = new RCode();
        caller.setRscriptExecutable(RScriptPath);
        caller.cleanRCode();
        
        // draw histograms of match lengths (ref and query) and match identity
        code.R_require("ggplot2");
        code.addRCode("data<-read.table(\"" + query_stats_file + "\", sep=\"\\t\", header=T)");

        String barchart_file = query_stats_file +  ".barchart.tiff";
        code.addRCode("tiff(\"" + barchart_file + "\", units=\"in\", width=5, height=5, res=300)");
        code.addRCode("ggplot(data, aes(x=type, y=query_match_count)) +  geom_bar(stat=\"identity\")");
        code.addRCode("dev.off()");
        //System.out.println(code.getCode());
        caller.setRCode(code);
        caller.runOnly();

    }
    
    private void mergeSVsStatsFiles(boolean is_ref, String sum_file, String count_file, String stats_file) throws Exception {
                
        
        BufferedWriter w = new BufferedWriter(new FileWriter(stats_file));
        
        // read count file
        Hashtable<String, Integer> counts_hash = new Hashtable();
        BufferedReader r = new BufferedReader(new FileReader(count_file));
        String line = null;
        int ncols = 0;
        StringBuffer buf = new StringBuffer();
        int line_count = 0;
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count == 1) {       // skip header
                continue;
            }
            
            line = line.trim();
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            String id = cols[0];
            int count = new Integer(cols[1]).intValue();                
            counts_hash.put(id, count);
        }
        r.close();
        
        // read sum file
        r = new BufferedReader(new FileReader(sum_file));
        line = null;
        buf = new StringBuffer();
        line_count = 0;
        while ((line=r.readLine()) != null) {
            line_count++;
            
            if (line_count == 1) {       // print header
                w.write(line + "\t");
                
                if (is_ref) {
                    w.write("ref_match_count\n");
                } else {
                    w.write("query_match_count\n");
                }
                
                continue;
            }
            
            line = line.trim();
            if (line.equals("")){
                continue;
            }
            
            String[] cols = line.split("\\s+");
            String id = cols[0];
            
            w.write(id + "\t");
            for (int i = 1; i < cols.length; i++) {
                w.write(df3.format(new Float(cols[i])) + "\t");
            }
            w.write(counts_hash.get(id) + "\n");
            
        }
        r.close();
        w.close();        

        
       String test = "";
    }

    
}
