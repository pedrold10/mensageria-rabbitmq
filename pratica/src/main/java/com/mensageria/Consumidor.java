package com.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumidor {
    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        String NOME_FILA_ORIGINAL = "fila_original";
        String NOME_FILA_NOVA = "fila_nova";

        String NOME_FILA_PLICA = "plica";

        canal.queueDeclare(NOME_FILA_PLICA, true, false, false, null);

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody());
            System.out.println("Eu " + consumerTag + " Recebi: " + mensagem);

            String[] parts = mensagem.split("-");
            if (parts.length == 2) {
                int numeroMensagem = Integer.parseInt(parts[0]);
                long timestampEnvio = Long.parseLong(parts[1]);


                if (numeroMensagem == 1 || numeroMensagem == 1000000) {
                    long timestampRecebimento = System.currentTimeMillis();

                    long diferenca = timestampRecebimento - timestampEnvio;
                    System.out.println("Diferença de tempo (ms) para mensagem " + numeroMensagem + ": " + diferenca);
                }
            }

            canal.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        canal.basicConsume(NOME_FILA_ORIGINAL, true, callback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA_ORIGINAL);
        });

        canal.basicConsume(NOME_FILA_NOVA, false, callback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA_NOVA);
        });
        canal.basicConsume(NOME_FILA_PLICA, false, callback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA_PLICA);
        });
    }
}
