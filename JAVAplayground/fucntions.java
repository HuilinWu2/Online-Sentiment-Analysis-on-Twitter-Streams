public static String cleanText(String text){
        text = text.replaceAll("[^\\p{ASCII}]","");
        text = text.replaceAll("\\s+"," ");
        text = text.replaceAll("[\\p{Cntrl}]","");
        text = text.replaceAll("[^\\p{Print}]","");
        return text;
    }
}

//===========================================================
public class DistanceCompute {
  
    public double getEuclideanDis(Point p1, Point p2) {
        double distance = 0;
        float[] p1_array = p1.getlocalArray();
        float[] p2_array = p2.getlocalArray();
      
        for (int i = 0; i < p1_array.length; i++) {
            distance += Math.pow(p1_array[i] - p2_array[i], 2);
        }
 
        return Math.sqrt(distance);
    }
}

//==============================================================

static void computeTFIDF(String path, String word) throws Exception {
        File fileDir = new File(path);
        File[] files = fileDir.listFiles();
        Map<String, Integer> containsKeyMap = new HashMap<>();
        Map<String, Integer> totalDocMap = new HashMap<>();
        Map<String, Double> tfMap = new HashMap<>();
        for (File f : files) {
            double termFrequency = 0;
            double totalTerm = 0;
            int containsKeyDoc = 0;
            int totalCount = 0;
            int fileCount = 0;
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            String s = "";
            while ((s = br.readLine()) != null) {
                if (s.equals(word)) {
                    termFrequency++;
                    flag = true;
                }
            }
            totalTerm += totalCount - fileCount;
        }
        //calculate TF*IDF
        for (File f : files) {
            int otherContainsKeyDoc = 0;
            int otherTotalDoc = 0;
            double idf = 0;
            double tfidf = 0;
            for (Map.Entry<String, Integer> entry : containsKeyset) {
                if (!entry.getKey().equals(f.getName())) {
                    otherContainsKeyDoc += entry.getValue();
                }
            }
            for (Map.Entry<String, Integer> entry : totalDocset) {
                if (!entry.getKey().equals(f.getName())) {
                    otherTotalDoc += entry.getValue();
                }
            }
            idf = log((float) otherTotalDoc / (otherContainsKeyDoc + 1), 2);
            for (Map.Entry<String, Double> entry : tfSet) {
                if (entry.getKey().equals(f.getName())) {
                    tfidf = (double) entry.getValue() * idf;
                    System.out.println("tfidf:" + tfidf);
                }
            }
        }
    }
