import org.sqlite.SQLiteException;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;

public class ParsingCSV {

    public ParsingCSV(){

    }

    private ArrayList<String> split(String toSplit){

        ArrayList<String> toReturn = new ArrayList<>();

        int i = 0;

        ArrayList<Character> word = new ArrayList<>();

        boolean encounteredQuot = false;

        while(i<toSplit.length()){

            if(toSplit.charAt(i)==','){

                if(encounteredQuot==true){

                    word.add(toSplit.charAt(i));

                }

                else if(encounteredQuot==false){

                    String newWord="";

                    for(char c : word){

                        newWord=newWord+c;

                    }

                    toReturn.add(newWord);

                    word=new ArrayList<>();

                }

            }

            else if(toSplit.charAt(i)=='\"'){

                encounteredQuot=!encounteredQuot;

                word.add(toSplit.charAt(i));

            }

            else {

                word.add(toSplit.charAt(i));

            }

            i=i+1;

            if(i==toSplit.length()){

                String newWord="";

                for(char c : word){

                    newWord=newWord+c;

                }

                toReturn.add(newWord);

                word=new ArrayList<>();

            }

        }

        return toReturn;

    }



    public void CSVparse(String csvFile) throws FileNotFoundException, UnsupportedEncodingException, java.sql.SQLException {

        int recReceived=0;

        int recSuccess=0;

        int recFail=0;

        File badData = new File("bad-data-"+System.currentTimeMillis()+".csv");

        PrintWriter writer = new PrintWriter(badData, "UTF-8");

        File file = new File(csvFile);

        Scanner sc = new Scanner(file).useDelimiter("\n|\r");

        String firstLine = sc.next();

        ArrayList<String> firstSplit = this.split(firstLine);

        int numColumns = firstSplit.size();

        String sqlUrl="jdbc:sqlite::memory:";

        Connection conn = DriverManager.getConnection(sqlUrl);

        if(conn != null){

            DatabaseMetaData meta = conn.getMetaData();

            String sqlString = "CREATE TABLE IF NOT EXISTS parseddata (\n";

            int k = 0;

            for(String col : firstSplit){

                sqlString=sqlString+col+" text NOT NULL";

                if(k==numColumns-1){

                    sqlString=sqlString+");";

                }

                else{

                    sqlString=sqlString+", \n";

                }

                k=k+1;

            }

            Statement stmt = conn.createStatement();

            stmt.execute(sqlString);

        }

        while (sc.hasNext()){

            String line = sc.next();

            if(line.length()>0){

                recReceived=recReceived+1;

                ArrayList<String> splitLine = this.split(line);

                boolean valid=true;

                for(String elem : splitLine){

                    if(elem.equals("")){

                        valid=false;

                    }

                }

                if(valid==true){

                    recSuccess=recSuccess+1;

                    String sqlInsert = "INSERT INTO parseddata VALUES(";

                    int i = 0;

                    while (i < numColumns){

                        boolean containsSingleQuot = false;

                        int singQuotAt = 0;

                        while(singQuotAt < splitLine.get(i).length()){

                            if(splitLine.get(i).charAt(singQuotAt)=='\''){

                                containsSingleQuot=true;

                                break;

                            }

                            singQuotAt=singQuotAt+1;

                        }

                        if(containsSingleQuot==false){

                            sqlInsert=sqlInsert+"'"+splitLine.get(i)+"'";

                        }

                        else {

                            int scc = 0;

                            String escapedSingQuot="";

                            while (scc < splitLine.get(i).length()){

                                if(splitLine.get(i).charAt(scc)!='\''){

                                    escapedSingQuot=escapedSingQuot+splitLine.get(i).charAt(scc);

                                }

                                else {

                                    escapedSingQuot=escapedSingQuot+"'"+splitLine.get(i).charAt(scc);

                                }

                                scc=scc+1;

                            }

                            sqlInsert=sqlInsert+"'"+escapedSingQuot+"'";

                        }

                        if(i<numColumns-1){

                            sqlInsert=sqlInsert+",";

                        }

                        i=i+1;

                    }

                    sqlInsert=sqlInsert+");";


                    Statement stmt = conn.createStatement();

                    try{

                        stmt.execute(sqlInsert);

                    }

                    catch (SQLiteException e){
   

                        System.out.println(sqlInsert);

                        System.out.flush();

                        System.exit(-1);

                    }

                }

                else {

                    recFail=recFail+1;

                    writer.println(line);

                }

            }

        }

        System.out.println("Records received: "+recReceived);

        System.out.println("Records successfully parsed: "+recSuccess);

        System.out.println("Records failed to parse: "+recFail);

        writer.close();

        conn.close();

    }

}
