#!/usr/bin/env python
import pika


def callback(ch, method, properties, body):
    print(f" [x] Routing key {method.routing_key}")
    print(f" [x] Received {body}")


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.basic_consume(queue="ml-data", on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()
