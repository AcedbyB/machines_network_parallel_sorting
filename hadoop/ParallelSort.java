import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.filecache.DistributedCache;

public class ParallelSort {

  final static int REDUCER_COUNT = 20;
  final static int MAX_SAMPLE = 1000000;

  public static void main(String[] args) throws Exception {
    int size = Integer.parseInt(args[4]);
    String inputFile = args[0];
    String outputFile = args[1];
    String cacheFile = args[2];
    String partitionFile = args[3];

    long lastTime = System.nanoTime();
    JobConf conf = new JobConf(ParallelSort.class);
    conf.set("mapred.textoutputformat.separator", "");
    conf.set("stream.map.output.field.separator", "");
    DistributedCache.addCacheFile(new URI(cacheFile), conf);
    DistributedCache.createSymlink(conf);

    // conf.setNumMapTasks(150);
    conf.setNumReduceTasks(REDUCER_COUNT);

    FileInputFormat.setInputPaths(conf, new Path(inputFile));
    FileOutputFormat.setOutputPath(conf, new Path(outputFile));

    Job job = new Job(conf, "Parallel Sorting");
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
    job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);

    TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(partitionFile));
    // InputSampler.Sampler<Text, Text> sampler = 
    //   new InputSampler.RandomSampler<>(
    //     0.01, // sample rate
    //     10000, // max number of samples
    //     100); // max number of splits
    InputSampler.Sampler<Text, Text> sampler = 
      new InputSampler.SplitSampler<>(Math.min(size / REDUCER_COUNT * 10, MAX_SAMPLE));
		InputSampler.writePartitionFile(job, sampler);
    job.setPartitionerClass(TotalOrderPartitioner.class);

    long samplingTime = System.nanoTime() - lastTime;
    lastTime = System.nanoTime();

    int code = job.waitForCompletion(true) ? 0 : 1;
    long mapReduceTime = System.nanoTime() - lastTime;

    System.out.println("Sampling time: " 
        + (double) samplingTime / TimeUnit.SECONDS.toNanos(1) 
        + " seconds");
    System.out.println("MapReduce time: " 
        + (double) mapReduceTime / TimeUnit.SECONDS.toNanos(1) 
        + " seconds");
    System.exit(code);
  }
}