package com.mensageria;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Produtor {

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try (
                Connection connection = connectionFactory.newConnection();
                Channel canal = connection.createChannel();
        ) {
            String NOME_FILA = "plica";

            canal.queueDeclare(NOME_FILA, true, false, false, null);

            int totalMensagens = 1000000;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            for (int i = 1; i <= totalMensagens; i++) {
                long timestamp = System.currentTimeMillis();
                String mensagem = i + "-" + timestamp;

                canal.basicPublish("", NOME_FILA, true, false, null, mensagem.getBytes());

                if (i % 1000 == 0) {
                    System.out.println("Enviadas " + i + " mensagens.");
                }
            }

            System.out.println("Todas as mensagens foram enviadas.");


            String timestamp = sdf.format(new Date());
            String mensagem = "Olá, timestamp: " + timestamp;
            canal.basicPublish("", NOME_FILA, false, false, null, mensagem.getBytes());
        }
    }
}
