"""
This program sends a message to a queue on the RabbitMQ server.
Press CTRL+C to peacefully exit the program.

Author: Sowdamini Nandigama
Date: September 29, 2023
"""
import pika
import sys
import webbrowser
import csv
import logging

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")

def send_message(host: str, queue_name: str, message: str):
    try:
        # Constructing the log file by queue name
        queue_name_log_file = f"{queue_name}_producer_file.log"
        # Configure the logging settings
        logging.basicConfig(filename=queue_name_log_file, level=logging.INFO)
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
        logging.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logging.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


if __name__ == "__main__":
    offer_rabbitmq_admin_site()

    # Read messages from a CSV file (assuming one message per line)
    with open("tasks.csv", newline="") as csvfile:
        messages_reader = csv.reader(csvfile)
        try:
            for row in messages_reader:
                message = " ".join(row)  # Combine columns into a single message
                send_message("localhost", "task_queue3", message)
        except KeyboardInterrupt:
            logging.info(f"Error: Reading the data")
