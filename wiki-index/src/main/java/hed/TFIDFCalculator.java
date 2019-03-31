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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class TFIDFCalculator {

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

        private final double pageSum = 2115098; // Hardcoded from the results of DF Calculation

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");

            if (line[0].indexOf('#') < 0) {

                final String term = line[0];

                if (!term.equals("0SumOfPages")) {

                    Text[] idfText = {new Text(String.valueOf(Math.log(pageSum / (Double.parseDouble(line[1]) + 1.0))))};
                    TextArrayWritable idfTextWritable = new TextArrayWritable(idfText);
                    context.write(new Text(term), idfTextWritable);
                }

            } else {

                final String termFrequency = String.valueOf(line[1]);

                line = line[0].split("#");
                final String pageId = line[0];
                final String term = line[1];

                Text[] contentText = {new Text(pageId), new Text(termFrequency)};
                TextArrayWritable contentTextWritable = new TextArrayWritable(contentText);
                context.write(new Text(term), contentTextWritable);

            }
        }
    }

    public static class TFIDFReducer extends Reducer<Text, TextArrayWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            double idf = 0;
            List<TextArrayWritable> valueList = new ArrayList<TextArrayWritable>();

            for (TextArrayWritable value : values) {
//                if ("european".equals(key.toString()))
//                {
//                    System.out.println(idf);
//                    System.out.println(value.get().length);
//                }
                if (value.get().length == 1) {
                    idf = Double.parseDouble(((Text) value.get()[0]).toString());
                    Text[] contentText = {new Text(((Text) value.get()[0]).toString())};
                    TextArrayWritable contentTextWritable = new TextArrayWritable(contentText);
                    valueList.add(contentTextWritable);
                } else {
                    Text[] contentText = {new Text(((Text) value.get()[0]).toString()), new Text(((Text) value.get()[1]).toString())};
                    TextArrayWritable contentTextWritable = new TextArrayWritable(contentText);
                    valueList.add(contentTextWritable);
                }

            }

//            if ("european".equals(key.toString()))
//            {
//                System.out.println(idf);
//                System.out.println(valueList.size());
//            }
            for (TextArrayWritable value : valueList) {
//                if ("european".equals(key.toString()))
//                {
//                    System.out.println("yes");
//                    System.out.println(value.get().length);
//                }
                if (value.get().length != 1) {
//                    if ("european".equals(key.toString()))
//                    {
//                        System.out.println("entered and length = 1");
//                    }
                    final String pageId = ((Text) value.get()[0]).toString();
                    final double termFrequency = Double.parseDouble(((Text) value.get()[1]).toString());
                    final String outputKey = pageId.concat("#").concat(key.toString()); // outputKey = "pageID#term"
                    context.write(new Text(outputKey), new DoubleWritable(termFrequency * idf));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJobName("TF-IDF Calculation");
        job.setJarByClass(TFIDFCalculator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
