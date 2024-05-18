import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueRecsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private boolean isFirstLine = true;
    private static final Pattern CSV_PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (isFirstLine) {
            isFirstLine = false;
            return;
        }

        
        String[] values = CSV_PATTERN.split(line);
        String DBN = values[0];
        String schoolName = values[1].replaceAll("\"", "");
       	String year = values[2];

        context.write(new Text("Number of Records: "), new IntWritable(1));
        context.write(new Text(DBN), new IntWritable(1));
        context.write(new Text(schoolName), new IntWritable(1));
        context.write(new Text(year), new IntWritable(1));
    }
}

