import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<Object, Text, Text, Text> {

    private boolean isFirstLine = true;

    private static final Pattern CSV_PATTERN = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(isFirstLine){
            String newHeader = "DBN,Year,TotalEnrollment,GradePK,GradeK,Grade1,Grade2,Grade3,Grade4,Grade5,Grade6,Grade7,Grade8,Grade9,Grade10,Grade11,Grade12,Female,Male,Asian,Black,Hispanic,MultiRacial,NativeAmerican,White,MissingRace,StudentsDisabilities,EnglishLearner,Poverty";
            context.write(new Text(newHeader),new Text());
            isFirstLine = false;
            return;
        }
        String line = value.toString();


        String[] values = CSV_PATTERN.split(line);
        String DBN = values[0];
        String schoolName = values[1].replaceAll("\"", "");
        String year = values[2];
        String total = values [3];
        String gPK = values [5];
        String g0 = values [6];
        String g1 = values [7];
        String g2 = values [8];
        String g3 = values [9];
        String g4 = values [10];
        String g5 = values [11];
        String g6 = values [12];
        String g7 = values [13];
        String g8 = values [14];
        String g9 = values [15];
        String g10 = values [16];
        String g11 = values [17];
        String g12 = values [18];
        String female = values[20];
        String male = values [22];
        String asian = values [24];
        String black = values [26];
        String hispanic = values [28];
        String multiRacial = values [30];
        String white = values [32];
        String missingRace = values [34];
        String disability = values [36];
        String englishLearner = values [38];
        String poverty = values [40].replace("Above","").replace("%","");
        String economicIndex = values [42].replace("Above","").replace("%","");

        if(year.equals(("2021-22"))){
            return;
        }

        String cleanedLine = String.join(",", DBN, year, total, gPK, g0, g1, g2, g3, g4, g5, g6, g7, g8, g9, g10, g11, g12, female, male, asian, black, hispanic, multiRacial,white, missingRace, disability, englishLearner, poverty, economicIndex);
        context.write(new Text(cleanedLine), new Text());
    }
}

