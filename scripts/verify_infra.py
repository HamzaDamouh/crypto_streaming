import sys
import requests
import redis
from confluent_kafka.admin import AdminClient, NewTopic


def check_kafka():
    print("Waking up Kafka")
    
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    
    topic = "crypto_trades"  
    new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
    
    futures = admin_client.create_topics([new_topic])

    for t, f in futures.items():
        try:
            f.result()  
            print(f"Topic '{t}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic '{t}' already exists")
            else:
                print(f"Kafka threw a tantrum: {e}")
                return False

    meta = admin_client.list_topics(timeout=10)
    if topic in meta.topics:
        print(f"Kafka check passed. Data is flowing.")
        return True
    else:
        print(f"Kafka check failed. Where did the topic go?")
        return False

def check_redis():
    print("\nPoking Redis with a stick...")
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        if not r.ping():
            print("Redis is ghosting me")
            return False
            
        key = "student_sleep_hours"
        val = "4"
        r.set(key, val, ex=10) 
        
        read_val = r.get(key)
        if read_val == val:
            print(f"Redis is alive and remembers I only got {val} hours of sleep.")
            return True
        else:
            print(f"Redis forgot my data. Expected {val}, got {read_val}.")
            return False
    except Exception as e:
        print(f"Redis exploded: {e}")
        return False

def check_grafana():
    print("\nChecking Grafana")
    try:
        response = requests.get("http://localhost:3000/api/health", timeout=5)
        if response.status_code == 200:
            print(f"grafana is healthy (Status: {response.status_code})")
            return True
        else:
            print(f"gafana is up but bruv is mad (Status: {response.status_code})")
            return False
    except Exception as e:
        print(f"grafana check failed: {e}\n please work")
        return False

def main():
    print("pre-flight checks\n")
    
    
    kafka_ok = check_kafka()
    redis_ok = check_redis()
    grafana_ok = check_grafana()
    
    print("\n" + "="*40)
    if kafka_ok and redis_ok and grafana_ok:
        print("ALL INFRASTRUCTURE CHECKS PASSED.")
        print(":)")
        sys.exit(0)
    else:
        print("SOMETHING FAILED.")
        print(":(")
        sys.exit(1)

if __name__ == "__main__":
    main()