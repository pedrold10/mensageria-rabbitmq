package com.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Consumidor {
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        String NOME_FILA_ORIGINAL = "plica";
        String NOME_FILA_NOVA = "fila_nova";

        canal.queueDeclare(NOME_FILA_ORIGINAL, false, false, false, null);
        canal.queueDeclare(NOME_FILA_NOVA, false, false, false, null); // Declare a nova fila

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody());
            System.out.println("Eu " + consumerTag + " Recebi: " + mensagem);

            String[] parts = mensagem.split("-");
            if (parts.length == 2) {
                int numeroMensagem = Integer.parseInt(parts[0]);


                if (numeroMensagem == 1 || numeroMensagem == 1000000) {

                    canal.basicPublish("", NOME_FILA_NOVA, false, false, null, mensagem.getBytes());
                    System.out.println("Enviada mensagem " + numeroMensagem + " para a nova fila.");
                }
            }


            canal.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };


        canal.basicConsume(NOME_FILA_ORIGINAL, false, callback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA_ORIGINAL);
        });
    }
}
