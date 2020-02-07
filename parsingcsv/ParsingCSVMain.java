import java.io.FileNotFoundException;

import java.io.IOException;

import java.io.UnsupportedEncodingException;







public class ParsingCSVMain {

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException, java.sql.SQLException, IOException {

        if(args.length!=1){

            System.out.println("usage: ParsingCSVMain.java <csv-file>");

            System.exit(0);

        }

        else {

            ParsingCSV parser = new ParsingCSV();

            parser.CSVparse(args[0]);

        }

    }

}
