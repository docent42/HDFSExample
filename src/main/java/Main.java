import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Main
{
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) throws Exception
    {
        // ================= создаем экземпляр для работы с HDFS==================
        FileAccess fileAccess = new FileAccess("hdfs://7939cff8db3e:8020");

        //================= тест листа директорий и поддиректоррий ================
        //fileAccess.list(new Path("/")).forEach(System.out::println);

        // ============== тест чтенния файла ======================================
        //System.out.println(fileAccess.read("test/file1.txt"));

        //================= тест перезапись файла ===================================
        //fileAccess.ifExistDel("test/file.txt");

        // ================= тест запись файла в кластер ===========================
        //OutputStream os = fileAccess.createForWriting("test/file1.txt");
        //BufferedWriter br =
                //new BufferedWriter( new OutputStreamWriter(os, "UTF-8"));

        /*----------------- Генерация данных и запись в файл -------------------------
        for(int i = 0; i < 10_000_000; i++) {
            br.write(getRandomWord() + " ");
        }
        br.flush();
        br.close();*/
       //-----------------------------------------------------------------------------

       // ==== закрываем поток в конце работы ===================
        fileAccess.close();

    }

    private static String getRandomWord()
    {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for(int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
