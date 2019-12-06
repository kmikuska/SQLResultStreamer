package myPackage.sqlresultstreamer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

import com.opencsv.CSVWriter;

/**
 * Streams a ResultSet as CSV.
 */
public class StreamingCsvResultSetExtractor implements ResultSetExtractor<Void> {

  public static enum RecordType 
	  { 
	      ALL,		//Write ALL records to output stream, will not write to output stream special
	      BOTH,		//Write NORMAL records to output stream and SPECIAL records to output stream special
	      SPECIAL,	//Write SPECIAL records to output stream special, will not write NORMAL records
	      NORMAL;	//Write NORMAL records to output stream, will not write SPECIAL records
	  }
  private final RecordType rt;
  private final OutputStream os, os_special;

  /**
   * @param os the OutputStream to stream the CSV to
   */
  public StreamingCsvResultSetExtractor(final OutputStream os, final OutputStream os_special, final RecordType rt) {
    this.os = os;
    this.os_special = os_special;
    this.rt = rt;
  }

  @Override
  public Void extractData(final ResultSet rs) {
    try (OutputStreamWriter osw = new OutputStreamWriter(os, "ISO-8859-1");
    		CSVWriter writer = new CSVWriter(osw);
    		
    		OutputStreamWriter osw_special = new OutputStreamWriter(os_special, "ISO-8859-1");
    		CSVWriter writer_special = new CSVWriter(osw_special);
    		) {
      final ResultSetMetaData rsmd = rs.getMetaData();
      final int columnCount = rsmd.getColumnCount();
      long rowcounter = 0;
      writeHeader(rsmd, columnCount, writer);
      writeHeader(rsmd, columnCount, writer_special);
      while (rs.next()) {
    	String[] row = new String[columnCount];
    	boolean writeRow_special = false, rowHasSpecial = false;
        for (int i = 0; i < columnCount; i++) {
          final Object value = rs.getObject(i+1);
          if(value!=null) {
        	  String strval = value.toString();
        	  row[i] = strval;
        	  boolean hasSpecial = specialChars(strval);
        	  if(hasSpecial) rowHasSpecial = true;
              if((rt == RecordType.BOTH || rt == RecordType.SPECIAL) && hasSpecial) writeRow_special = true;
//              if(rt == RecordType.ALL || ((rt == RecordType.BOTH || rt == RecordType.NORMAL) && !hasSpecial)) rowHasSpecial = true;
          }
        }
        
        rowcounter++;
        
        if(rt == RecordType.ALL || ((rt == RecordType.BOTH || rt == RecordType.NORMAL) && !rowHasSpecial))  {
        	writer.writeNext(row);
        }
//        if(writeRow) {
//        	writer.writeNext(row);
//        }
        if(writeRow_special) {
        	writer_special.writeNext(row);
        }
      }

    System.out.println("Total Rows: " + rowcounter);
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

    return null;
  }

  private static void writeHeader(final ResultSetMetaData rsmd,
      final int columnCount, final CSVWriter writer) throws SQLException {
	  String[] row = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
    	row[i] = rsmd.getColumnName(i+1);
    }
    writer.writeNext(row);
  }
  
  public boolean specialChars(String str) throws IOException {
      for (int i = 0; i < str.length(); i++) {
    	  if (str.charAt(i) >= 128) {
    		  System.out.println("Found Special Character in '" + str + "': " + str.charAt(i));
    		  return true;
    	  }
      }
      return false;
  }
}