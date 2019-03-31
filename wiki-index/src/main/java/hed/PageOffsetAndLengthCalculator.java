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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.wikiclean.WikiClean;

/**
 *
 * @author hed
 */
public class PageOffsetAndLengthCalculator {

    public static class PageOffsetAndLengthMapper extends Mapper<LongWritable, Text, LongWritable, LongArrayWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            WikiClean cleaner = new WikiClean.Builder().withTitle(true).build();
            LongWritable id = new LongWritable(Long.parseLong(cleaner.getId(text)));
            LongWritable len = new LongWritable(value.getLength());
            if (id.get() == -1) {
                return;
            }
            LongWritable content[] = {key, len};
            context.write(id, new LongArrayWritable(content));
        }

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("xmlinput.start", "<page>");
        configuration.set("xmlinput.end", "</page>");

        Job job = Job.getInstance(configuration);
        job.setJobName("Page Offset and Length Calculation");
        job.setJarByClass(PageOffsetAndLengthCalculator.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongArrayWritable.class);

        job.setMapperClass(PageOffsetAndLengthMapper.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
