package com.example.order_service.service;

import com.example.order_service.client.InventoryClient;
import com.example.order_service.dto.OrderRequest;
import com.example.order_service.event.OrderPlaceEvent;
import com.example.order_service.model.Order;
import com.example.order_service.repo.OrderRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepo orderRepo;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<String, OrderPlaceEvent> kafkaTemplate;

    public void placeOrder(OrderRequest orderRequest) {
        var isProductInStock = inventoryClient.isInStock(
                orderRequest.skuCode(),
                orderRequest.quantity());

        if (isProductInStock) {
            Order order = new Order();
            order.setOrderNumber(UUID.randomUUID().toString());
            order.setPrice(orderRequest.price());
            order.setSkuCode(orderRequest.skuCode());
            order.setQuantity(orderRequest.quantity());
            orderRepo.save(order);

            // Send the message to Kafka Topic
            OrderPlaceEvent orderPlaceEvent = new OrderPlaceEvent(order.getOrderNumber() , orderRequest.userDetails().email());
            log.info("Start - Sending OrderPlacedEvent {} to Kafka topic order-placed", orderPlaceEvent);
            kafkaTemplate.send("order-placed", orderPlaceEvent);
            log.info("End - Sending OrderPlacedEvent {} to Kafka topic order-placed", orderPlaceEvent);
        } else {
            throw new RuntimeException("Product with SkuCode" +
                    orderRequest.skuCode() + "is not in stock");
        }

        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());
        order.setPrice(orderRequest.price());
        order.setSkuCode(orderRequest.skuCode());
        order.setQuantity(orderRequest.quantity());
        orderRepo.save(order);
    }
}
