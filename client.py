import threading
import json
import time
import uuid
from confluent_kafka import Producer, Consumer, KafkaError

# --- CONFIGURATION ---
KAFKA_BROKER = 'localhost:9092'

# Authentication Config (ACL Simulation placeholder)
# In a real scenario, you add 'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'PLAIN', etc.
kafka_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': f'python-client-{uuid.uuid4()}',
    # 'security.protocol': 'SASL_PLAINTEXT', # Uncomment for Auth
    # 'sasl.mechanism': 'PLAIN',
    # 'sasl.username': 'client_user',
    # 'sasl.password': 'client_secret'
}

consumer_conf = kafka_conf.copy()
consumer_conf.update({
    'group.id': 'android_client_group',
    'auto.offset.reset': 'latest'
})


# --- HELPER FUNCTIONS ---

def delivery_report(err, msg):
    if err is not None:
        print(f'❎Message delivery failed: {err}')
    else:
        print(f'❤❤Message delivered to {msg.topic()}')


def client_listener():
    """Listens for responses from Spark (OTP emails or Validation Success)"""
    c = Consumer(consumer_conf)
    c.subscribe(['client_responses'])

    print("\n[Client Listener] Listening for System messages...")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"\n[NOTIFICATION RECEIVED] >> {json.dumps(data, indent=2)}")

            # Simple logic to automate the test: if we see an OTP, let's verify it
            if 'OTP Sent' in data.get('status', ''):
                print(">>> Auto-triggering OTP Verification step...")
                verify_otp(data['email'], data['otp'])

    except KeyboardInterrupt:
        pass
    finally:
        c.close()


def register_user(email):
    p = Producer(kafka_conf)
    payload = json.dumps({'email': email})
    print(f"\n[Action] Registering user: {email}")
    p.produce('register_topic', payload.encode('utf-8'), callback=delivery_report)
    p.flush()


def verify_otp(email, otp):
    p = Producer(kafka_conf)
    payload = json.dumps({'email': email, 'otp': otp})
    print(f"\n[Action] Verifying OTP for: {email}")
    p.produce('verify_topic', payload.encode('utf-8'), callback=delivery_report)
    p.flush()


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    # 1. Start the listener thread (Simulating the phone waiting for messages)
    t = threading.Thread(target=client_listener)
    t.daemon = True
    t.start()

    print("--- Python 3.13 Client Started ---")
    time.sleep(2)  # Wait for listener to init

    # 2. User initiates Registration
    user_email = "test_user_1@example.com"
    register_user(user_email)

    # Keep main thread alive to let the listener thread work
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting.")