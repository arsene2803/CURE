package util;

public class Point {
	private double x;
	private double y;
	private Cluster c;
	
	public Point(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public Point() {
		// TODO Auto-generated constructor stub
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public Cluster getC() {
		return c;
	}

	public void setC(Cluster c) {
		this.c = c;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.x+","+this.y;
	}
	
	
	
	

}
