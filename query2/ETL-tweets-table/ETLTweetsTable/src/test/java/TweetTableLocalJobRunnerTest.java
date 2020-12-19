import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;



public class TweetTableLocalJobRunnerTest {

    @Test
    public void run() throws Exception {

        Configuration conf = new Configuration();

        // use LocalJobRunner
        conf.set("mapred.job.tracker", "local");
        // set the filesystem to be local
        conf.set("fs.default.name", "file:///");
        // read the input from the local folder "input"
        Path inputPath = new Path("inputnewbyte");
        // write the output to the local file "output"
        Path outputPath = new Path("outputnewbyteTweetTable");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        // run the local MapReduce job
        Job job = runJob(conf, inputPath, outputPath);
        // assert the job is successful
        assertTrue(job.isSuccessful());
        // (optional) you may read and verify the job output
    }

    /**
     * Local job runner for word count MapReduce workflow.
     * @param conf hadoop configuration
     * @param inputPath input path of local job runner
     * @param outputPath output path of local job runner
     * @return MapReduce job
     * @throws ClassNotFoundException if class-not-found exception occurs
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     */
    public Job runJob(Configuration conf, Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException {
        Job job = Job.getInstance(conf, "tweet");

        /*
        SequenceFile.Reader reader=null;
        try{
        reader=new SequenceFile.Reader(conf, Reader.file(inputPath));
        Text key= new Text();
        BytesWritable value=new BytesWritable();
        while(reader.next(key,value)){
            System.out.println(key);
            byte[] bytes=value.getBytes();
            int size=bytes.length;
            byte[] b=new byte[size];
            InputStream is=new ByteArrayInputStream(bytes);
            is.read(b);
            System.out.println(new String(b));
        }
        }
        finally {
            IOUtils.closeStream(reader);
        }*/

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TweetTableMapper.class);
        job.setReducerClass(TweetTableReducer.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
        return job;
    }
}
