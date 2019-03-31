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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author hed
 */
public class TFIDFSelector {

    public static class SelectorMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String doc[] = value.toString().split("\t");
            double tfidf = Double.parseDouble(doc[1]);

            doc = doc[0].split("#");
            final String pageId = doc[0];
            final String term = doc[1];

            Text[] content = {new Text(term), new Text(String.valueOf(tfidf))};
            context.write(new Text(pageId), new TextArrayWritable(content));

        }
    }

    public static class SelectorReducer extends Reducer<Text, TextArrayWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            double max1 = 0, max2 = 0, max3 = 0, max4 = 0, max5 = 0;
            String term1 = "", term2 = "", term3 = "", term4 = "", term5 = "";

            for (TextArrayWritable value : values) {

                String curTerm = value.get()[0].toString();
                double tfidf = Double.parseDouble(value.get()[1].toString());

                if (tfidf > max1) {
                    max5 = max4;
                    max4 = max3;
                    max3 = max2;
                    max2 = max1;
                    max1 = tfidf;
                    term5 = term4;
                    term4 = term3;
                    term3 = term2;
                    term2 = term1;
                    term1 = curTerm;
                } else if (tfidf > max2) {
                    max5 = max4;
                    max4 = max3;
                    max3 = max2;
                    max2 = tfidf;
                    term5 = term4;
                    term4 = term3;
                    term3 = term2;
                    term2 = curTerm;
                } else if (tfidf > max3) {
                    max5 = max4;
                    max4 = max3;
                    max3 = tfidf;
                    term5 = term4;
                    term4 = term3;
                    term3 = curTerm;
                } else if (tfidf > max4) {
                    max5 = max4;
                    max4 = tfidf;
                    term5 = term4;
                    term4 = curTerm;
                } else if (tfidf > max5) {
                    max5 = tfidf;
                    term5 = curTerm;
                }

            }

            context.write(key, new Text(term1));
            context.write(key, new Text(term2));
            context.write(key, new Text(term3));
            context.write(key, new Text(term4));
            context.write(key, new Text(term5));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("TF-IDF Selector");
        job.setJarByClass(TFIDFSelector.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SelectorMapper.class);
        job.setReducerClass(SelectorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean wait = job.waitForCompletion(true);
        System.exit(wait ? 0 : 1);
    }
}
