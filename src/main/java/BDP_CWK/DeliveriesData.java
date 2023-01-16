package BDP_CWK;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class DeliveriesData {
    public static class DeliveriesMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        public static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder stb1 = new StringBuilder();
            StringBuilder stb2 = new StringBuilder();
            StringBuilder stb3 = new StringBuilder();
            String headers = "id,inning,over_no,ball,batsman,non_striker,bowler,batsman_runs,extra_runs,total_runs,non_boundary,is_wicket,dismissal_kind,player_dismissed,fielder,extras_type,batting_team,bowling_team";
            if(value.toString().equals(headers) == false){
                String[] stringArr = value.toString().split(",");
                String isWicketDelivery = stringArr[11];
                String extraRunsDel = stringArr[8];
                String totalRunsScored = stringArr[9];
                if(isWicketDelivery.equals("1")){
                    stb1.append("Wicket_Taking_Deliveries");
                }
                if(!extraRunsDel.equals("0")){
                    stb2.append("Extras_Taking_Deliveries");
                }
                if(totalRunsScored.equals("0")){
                    stb3.append("Dot_Balls");
                }
                word.set(stb1.toString());
                context.write(word,one);
                word.set(stb2.toString());
                context.write(word,one);
                word.set(stb3.toString());
                context.write(word,one);
            }
        }

    }
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Deliveries Details <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Deliveries Detail");
        job.setJarByClass(DeliveriesData.class);
        job.setMapperClass(DeliveriesMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
