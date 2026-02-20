import sys
import requests
import redis
from confluent_kafka.admin import AdminClient, NewTopic

def check_kafka():
    print("Verifying Kafka conection and topics...")
    
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    topic = "crypto_trades"  
    new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
    
    # try to create the topic we need
    futures = admin_client.create_topics([new_topic])

    for t, f in futures.items():
        try:
            f.result()  
            print(f"Topic '{t}' succesfully created.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic '{t}' already exists.")
            else:
                print(f"Error making Kafka topic: {e}")
                return False

    # check if topic is actually in metadata
    meta = admin_client.list_topics(timeout=10)
    if topic in meta.topics:
        print("Kafka verification pass.")
        return True
    else:
        print(f"Error: Topic '{topic}' not find in cluster metadata.")
        return False

def check_redis():
    print("\nChecking Redis conectivity...")
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        if not r.ping():
            print("Error: Redis ping fail.")
            return False
            
        # test write and read stuff
        key = "infra_test_key"
        val = "infra_test_value"
        r.set(key, val, ex=10) 
        
        read_val = r.get(key)
        if read_val == val:
            print("Redis check is good.")
            return True
        else:
            print(f"Error: Redis data not matching. Expected '{val}', got '{read_val}'.")
            return False
    except Exception as e:
        print(f"Redis connection error: {e}")
        return False

def check_grafana():
    print("\nVerify Grafana status...")
    try:
        response = requests.get("http://localhost:3000/api/health", timeout=5)
        if response.status_code == 200:
            print(f"Grafana verification passed (Status: {response.status_code}).")
            return True
        else:
            print(f"Error: Grafana health check fail (Status: {response.status_code}).")
            return False
    except Exception as e:
        print(f"Grafana connection error: {e}")
        return False

def main():
    print("Starting the infra check script...\n")
    
    kafka_ok = check_kafka()
    redis_ok = check_redis()
    grafana_ok = check_grafana()
    
    print("\n" + "="*40)
    if kafka_ok and redis_ok and grafana_ok:
        print("SUCCESS: All infra checks are passed.")
        sys.exit(0)
    else:
        print("FAILURE: Some infra check failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()