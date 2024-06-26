very hacky solution to consume kafka-messages based on two given time slots via REST

Test with curl: 
http://localhost:5110/consume?brokers=localhost:9092&topic=my_topic&start_time=2024-06-24T00:00:00&stop_time=2024-06-25T00:00:00"
