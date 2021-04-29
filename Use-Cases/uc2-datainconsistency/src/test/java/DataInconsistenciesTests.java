
import dao.impl.ProductServiceImpl;
import dao.services.ProductService;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import pojo.Product;

public class DataInconsistenciesTests {
	Dataset<Product> productDataset;
	ProductService productService = new ProductServiceImpl();

	@Test
	public void getAllProducts() {
		productDataset = productService.getProductList(null);
		productDataset.show();
	}
}
