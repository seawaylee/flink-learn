package etl.dto;

public class OrderInfo2 {
    //订单号
    private Long orderId;
    //下单日期
    private String orderDate;
    //下单的地点
    private String address;

    public static OrderInfo2 line2info(String line) {
        String[] fields = line.split(",");
        OrderInfo2 orderInfo2 = new OrderInfo2();
        orderInfo2.setOrderId(Long.parseLong(fields[0]));
        orderInfo2.setOrderDate(fields[1]);
        orderInfo2.setAddress(fields[2]);
        return orderInfo2;
    }

    public OrderInfo2() {

    }

    @Override
    public String toString() {
        return "OrderInfo2{" +
                "orderId=" + orderId +
                ", orderDate='" + orderDate + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    public OrderInfo2(Long orderId, String orderDate, String address) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.address = address;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}