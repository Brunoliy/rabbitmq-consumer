from fastapi import FastAPI
import pika
import json
import time

app = FastAPI()

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f" [x] Received {message}")

def start_consuming():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='my_queue')
            channel.basic_consume(queue='my_queue', on_message_callback=callback, auto_ack=True)
            print(" [*] Waiting for messages. To exit press CTRL+C")
            channel.start_consuming()
        except Exception as e:
            print(f"Error connecting to RabbitMQ: {e}")
            time.sleep(5)  # Espera 5 segundos antes de tentar novamente


@app.on_event("startup")
def startup_event():
    import threading
    thread = threading.Thread(target=start_consuming)
    thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
