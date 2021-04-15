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
