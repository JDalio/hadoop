package fileopt;

import com.sun.xml.internal.rngom.util.Uri;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

public class HdfsOpt
{
    public void copy(final String localStc, final String dst) throws Exception
    {
        InputStream in = new BufferedInputStream(new FileInputStream(localStc));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataOutputStream out = fs.create(new Path(dst), new Progressable()
        {
            long times = 0;
            long length = new File(localStc).length();
            public void progress()
            {
                long preout=times * 64 * 1024 * 100/length;
                if(preout<=100)
                {
                    for (int j = 0; j <= String.valueOf(preout).length(); j++)
                    {
                        System.out.print("\b");
                    }
                }
                times++;
                long percentage = 64 * 1024 * 100 * times / length;
                if(percentage<=100)
                {
                    System.out.print(percentage + "%");
                }
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println(" Complete!");
    }

    public static void main(String[] args) throws Exception
    {
        HdfsOpt hdfsOpt=new HdfsOpt();
        hdfsOpt.copy("/mnt/os","hdfs://localhost:9000/input");
//        String uri="hdfs://localhost:9000/test1";
//        Configuration conf=new Configuration();
//        FileSystem fs=FileSystem.get(URI.create(uri),conf);
//        FSDataInputStream in=null;
//        try
//        {
//            in=fs.open(new Path(uri));
//            IOUtils.copyBytes(in,System.out,4096,false);
//            in.seek(0);
//            IOUtils.copyBytes(in,System.out,4096,false);
//        }
//        finally
//        {
//            IOUtils.closeStream(in);
//        }

    }
}
