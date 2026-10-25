package io.github.sunil994923.orderhub.api;

import io.github.sunil994923.orderhub.route.OrderRoutes;
import org.apache.camel.EndpointInject;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit6.CamelSpringBootTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@CamelSpringBootTest
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderControllerTest {

    @Autowired
    private MockMvc mvc;

    @EndpointInject("mock:inbound")
    private MockEndpoint inbound;

    @AfterEach
    void reset() {
        inbound.reset();
    }

    @Test
    void acceptsValidOrderAndPublishesIt() throws Exception {
        inbound.expectedMessageCount(1);
        inbound.message(0).header(OrderRoutes.ORDER_ID_HEADER).isNotNull();

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"customerId":"c-1","shippingMethod":"EXPRESS",
                         "items":[{"sku":"SKU-1","quantity":2,"unitPrice":9.99}]}
                        """))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        inbound.assertIsSatisfied();
    }

    @Test
    void rejectsInvalidPayloadWithProblemDetail() throws Exception {
        inbound.expectedMessageCount(0);

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("""
                        {"customerId":"","shippingMethod":"STANDARD",
                         "items":[{"sku":"bad sku","quantity":0,"unitPrice":-1}]}
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_PROBLEM_JSON_VALUE));

        inbound.assertIsSatisfied();
    }
}
