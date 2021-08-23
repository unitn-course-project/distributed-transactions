package it.unitn.ds1;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class CheckSum {
  public static void main(String[] args) {
    Map<String, Integer> sum = new HashMap<>();
    File file = new File("sum.txt");
    Scanner myReader;
    try {
      myReader = new Scanner(file);
      while (myReader.hasNextLine()) {
        String data = myReader.nextLine();
        System.out.println(data);
        String d[] = data.split(", ");
        if (sum.containsKey(d[0]))
          sum.put(d[0], sum.get(d[0]) + Integer.parseInt(d[1]));
        else

          sum.put(d[0], Integer.parseInt(d[1]));
      }
      myReader.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (String key : sum.keySet())
      System.out.println("Sum check test id " + key + ": value " + sum.get(key));
  }
}
