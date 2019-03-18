package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;


/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
 public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {

    private FSDataInputStream fsin;
    private ZipInputStream zip;
    private Text currentKey;
    private BytesWritable currentValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem( conf );
        fsin = fs.open( path );
        zip = new ZipInputStream( fsin );

    }

    @Override
    public boolean nextKeyValue() throws IOException, FileNotFoundException,InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        ZipEntry entry = zip.getNextEntry();
        if(entry == null) {
            return false;
        }
        currentKey = new Text( entry.getName() );
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // InputStream stream = zipFile.getInputStream(entry);
        // BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        // StringBuilder currentkeybuilder = new StringBuilder();
        // String line;
        // while ((line = reader.readLine()) != null) {
        //     currentkeybuilder.append(line);
        // } 
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            try
            {
                bytesRead = zip.read( temp, 0, 8192 );
            }
            catch ( EOFException e )
            {
                
                return false;
            }
            if ( bytesRead > 0 )
                bos.write( temp, 0, bytesRead );
            else
                break;
        }       
        // currentValue = currentkeybuilder.toString();  
        // reader.close();
        currentValue = new BytesWritable( bos.toByteArray() );
        zip.closeEntry();
        return true;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // let's ignore this one for now
        return 0;
    }

    @Override
    public void close() throws IOException {
        // your code here
        // the code here depends on what/how you define a split....
        zip.close();
        fsin.close();

    }
}
