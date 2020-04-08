package etl.dto;

/**
 * @author SeawayLee
 * @create 2020-03-30 18:07
 */
public class OrderInfo1 {
    private long orderId;
    private String productName;
    private double price;

    public OrderInfo1() {
    }

    public OrderInfo1(long orderId, String productName, double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    public static OrderInfo1 line2info(String line) {
        String[] fields = line.split(",");
        OrderInfo1 orderInfo1 = new OrderInfo1();
        orderInfo1.setOrderId(Long.parseLong(fields[0]));
        orderInfo1.setProductName(fields[1]);
        orderInfo1.setPrice(Double.parseDouble(fields[2]));
        return orderInfo1;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderInfo1{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }
}
