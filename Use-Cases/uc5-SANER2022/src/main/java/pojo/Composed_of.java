package pojo;

public class Composed_of extends LoggingPojo {

	private Order orderP;	
	private Product orderedProducts;	

	public Composed_of() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Composed_of cloned = (Composed_of) super.clone();
		cloned.setOrderP((Order)cloned.getOrderP().clone());	
		cloned.setOrderedProducts((Product)cloned.getOrderedProducts().clone());	
		return cloned;
    }

	public Order getOrderP() {
		return orderP;
	}	

	public void setOrderP(Order orderP) {
		this.orderP = orderP;
	}
	
	public Product getOrderedProducts() {
		return orderedProducts;
	}	

	public void setOrderedProducts(Product orderedProducts) {
		this.orderedProducts = orderedProducts;
	}
	

}
