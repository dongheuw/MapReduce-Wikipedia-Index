/*
 * The MIT License
 *
 * Copyright 2018 hed.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package hed;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author hed
 */
public class TermFrequencyCalculator {

    public static class TermFrequencyMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String doc = value.toString();

            String text = sliceDocument(doc, "<text", "</text>");
            if (text.length() < 1) {
                return;
            }

            char txt[] = text.toLowerCase().toCharArray();
            normalizeCharacter(txt);

            final String pageId = sliceDocument(doc, "<id>", "</id>");
            if (pageId.length() < 1) {
                return;
            }

            StringTokenizer itr = new StringTokenizer(String.valueOf(txt));
            int sumOfWords = itr.countTokens();

            while (itr.hasMoreTokens()) {

                final String term = itr.nextToken();
                final String outputKey = pageId.concat("#").concat(term);
                IntWritable content[] = {new IntWritable(sumOfWords), new IntWritable(1)};
                IntArrayWritable contentWritable = new IntArrayWritable(content);
                context.write(new Text(outputKey), contentWritable);

            }
        }

        private void normalizeCharacter(char[] txt) {
            for (int i = 0; i < txt.length; ++i) {
                if (!((txt[i] >= 'a' && txt[i] <= 'z') || (txt[i] >= 'A' && txt[i] <= 'Z'))) {
                    txt[i] = ' ';
                }
            }
        }

        private String sliceDocument(String doc, String st, String ed) {
            int s = doc.indexOf(st);
            int t;
            if ("</text>".equals(ed)) {
                t = doc.lastIndexOf(ed);
            } else {
                t = doc.indexOf(ed);
            }
            if (s < 0 || t < 0) {
                return "";
            }
            while (doc.charAt(s) != '>') {
                s++;
            }
            return doc.substring(s + 1, t);
        }

    }

    public static class TermFrequencyReducer extends Reducer<Text, IntArrayWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0, sumOfWords = 0;
            for (IntArrayWritable value : values) {
                Writable val[] = value.get();
                sum += ((IntWritable) val[1]).get();
                sumOfWords = ((IntWritable) val[0]).get();
            }

            final double termFrequency = (double) sum / sumOfWords;
            context.write(key, new DoubleWritable(termFrequency));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("xmlinput.start", "<page>");
        configuration.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(configuration);
        job.setJobName("Term Frequency Calculation");
        job.setJarByClass(TermFrequencyCalculator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(TermFrequencyMapper.class);
        job.setReducerClass(TermFrequencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
