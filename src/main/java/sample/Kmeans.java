package sample;

import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.util.function.UnaryOperator;

public class Kmeans
{
	private static int K = 10;
	private static int DIM = 20;
	private static ArrayList<List<Double>> centroid = new ArrayList<List<Double>>();
	private static double THR = 0.01;

	public static void main(String[] args) throws Exception
	{
		JavaSparkContext sc = new JavaSparkContext("local[*]", "programname");
		// Read the points
		JavaRDD<String> data1 = sc.textFile("data.txt");
		JavaRDD<List<Double>> points = data1.map(d -> getPoints(d));

		List<List<Double>> pointsList = points.collect();
		// Read the centroids
		List<String> data2 = sc.textFile("centroid.txt").collect();
		for (int i = 0; i < K; i++)
		{
			centroid.add(getPoints(data2.get(i)));
		}
		ArrayList<List<Double>> updateCentroid = (ArrayList<List<Double>>) centroid.clone();

		do
		{
			// TODO Assign points
			JavaPairRDD<Integer, List<Double>> assignedPoints = points.mapToPair(p -> new Tuple2<>(nearestC(p)._1, nearestC(p)._2));
			JavaPairRDD<Integer, Iterable<List<Double>>> mergedPoints = assignedPoints.groupByKey();
			JavaPairRDD<Integer, List<Double>> updatedCentroids = mergedPoints.mapToPair(p -> update(p));

			updateCentroid.removeAll(updateCentroid);
			updateCentroid.addAll(updatedCentroids.values().collect());
			for (int i = 0; i < updateCentroid.size(); i++)
			{
				if(updateCentroid.get(i).size() < 20){
					for (int j = 0; j < 32 - updateCentroid.get(i).size()  ; j++)
					{
						updateCentroid.get(i).add(0.0);
					}
				}
			}
			centroid = (ArrayList<List<Double>>) updateCentroid.clone();
			// TODO Update centroids
		} while (diff(centroid, updateCentroid) > THR);

		for (int i = 0; i < K; i++)
		{
			System.out.println(centroid.get(i));
		}
	}

	private static double diff(ArrayList<List<Double>> c1, ArrayList<List<Double>> c2)
	{
		// TODO Compute the sum of distances for each pair of centroids, one from c1 and the other from c2
		double sumOfDistances = 0.0;
		for (int i = 0; i < c2.size(); i++)
		{
			sumOfDistances += dist(c1.get(i), c2.get(i));
		}
		return sumOfDistances;
	}

	private static Tuple2<Integer, List<Double>> update(Tuple2<Integer, Iterable<List<Double>>> c)
	{
		Tuple2<Integer, List<Double>> updateCentroid = new Tuple2<>(c._1, new ArrayList<>());

		// c.1 is the ID of the centroid. c.2 is the list of all the points assigned to the centroid.
		// TODO Compute the average of all assigned points to update the centroid.
		c._2.forEach(item ->{
			double sum = 0.0;
			for (int l = 0; l < item.size(); l++)
			{
				sum += item.get(l);
			}
			sum /= ((Collection<?>) c._2).size();
			updateCentroid._2.add(sum);
		});

		return updateCentroid;
	}

	private static Tuple2<Integer, List<Double>> nearestC(List<Double> p)
	{
		// p is one point
			double tempDist;
			double minDistance;
			Tuple2<Integer, List<Double>> nearestCentroid = new Tuple2<>(0, new ArrayList<>());

			minDistance = Double.MAX_VALUE;
			for (int j = 0; j < centroid.size(); j++)
			{
			tempDist = dist(centroid.get(j),p);
			if (tempDist < minDistance)
			{
				minDistance = tempDist;
				nearestCentroid = new Tuple2<>(j, p);
			}
		}
		// TODO Find the nearest centroid to p, and produce the tuple (centroidID, p)
		return nearestCentroid;
	}

	private static double dist(List<Double> p, List<Double> q)
	{
		// Compute the Euclidean distance between two points p and q
		double distance = 0.0;

		for (int i = 0; i < DIM; i++)
		{
			distance += (p.get(i) - q.get(i)) * (p.get(i) - q.get(i));
		}
		return Math.sqrt(distance);
	}

	private static List<Double> getPoints(String d)
	{
		String[] s_point = d.split("\t");
		Double[] d_point = new Double[DIM];
		for (int i = 0; i < DIM; i++)
		{
			d_point[i] = Double.parseDouble(s_point[i]);
		}
		return Arrays.asList(d_point);
	}
}
