from flask import Flask, request, jsonify
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
import json

app = Flask(__name__)


def create_consumer(brokers, topic):
    consumer = KafkaConsumer(
        bootstrap_servers=brokers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer


def consume_messages(consumer, topic, start_time, stop_time):
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        raise Exception(f"No partitions found for topic {topic}")

    start_timestamp = int(datetime.timestamp(start_time) * 1000)
    stop_timestamp = int(datetime.timestamp(stop_time) * 1000)

    messages = []

    for partition in partitions:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        offsets_for_times = consumer.offsets_for_times({tp: start_timestamp})
        if offsets_for_times and offsets_for_times[tp]:
            start_offset = offsets_for_times[tp].offset
            consumer.seek(tp, start_offset)

    for message in consumer:
        message_time = message.timestamp
        if start_timestamp <= message_time <= stop_timestamp:
            messages.append(message.value)
        elif message_time > stop_timestamp:
            break

    return messages


@app.route('/consume', methods=['GET'])
def consume():
    brokers = request.args.get('brokers')
    topic = request.args.get('topic')
    start_time_str = request.args.get('start_time')
    stop_time_str = request.args.get('stop_time')

    if not all([brokers, topic, start_time_str, stop_time_str]):
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S')
        stop_time = datetime.strptime(stop_time_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400

    consumer = create_consumer(brokers.split(','), topic)
    try:
        messages = consume_messages(consumer, topic, start_time, stop_time)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify(messages)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5111)
